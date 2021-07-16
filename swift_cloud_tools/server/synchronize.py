# -*- coding: utf-8 -*-
import requests
import time
import os

from flask import Response
from swift_cloud_tools import create_app
from google.cloud.exceptions import NotFound
from swiftclient import client as swift_client

from swift_cloud_tools.server.utils import Keystone, Swift, Google, Transfer
from swift_cloud_tools.models import TransferProject

BUCKET_LOCATION = 'SOUTHAMERICA-EAST1'
RESERVED_META = [
    'x-delete-at',
    'x-delete-after',
    'x-versions-location',
    'x-history-location',
    'x-undelete-enabled',
    'x-container-sysmeta-undelete-enabled'
]


class SynchronizeProjects():

    def synchronize(self, project_id):
        """Get project in swift."""

        self.project_id = project_id
        self.flush_object = int(os.environ.get("FLUSH_OBJECT", "1000"))

        self.app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
        ctx = self.app.app_context()
        ctx.push()

        self.keystone = Keystone()
        self.conn = self.keystone.get_keystone_connection()
        self.swift = Swift(self.conn, project_id)
        google = Google()
        self.transfer = Transfer()
        resume = False

        status, msg = self.swift.set_account_meta_cloud()

        self.app.logger.info("[SERVICE][TRANSFER] {} SET account_meta_cloud 'AUTH_{}': {}".format(
            status,
            project_id,
            msg
        ))

        if status != 204:
            return Response(msg, mimetype="text/plain", status=status)

        time.sleep(5)

        gcp_client = google.get_gcp_client()
        account = 'auth_{}'.format(project_id)
        try:
            bucket = gcp_client.get_bucket(account)
        except NotFound:
            bucket = gcp_client.create_bucket(
                account,
                location=BUCKET_LOCATION
            )
            bucket.iam_configuration.uniform_bucket_level_access_enabled = False
            bucket.patch()
        except Exception as err:
            self.app.logger.info('[SERVICE][TRANSFER] 500 GET Create bucket: {}'.format(err))
            return Response(err, mimetype="text/plain", status=500)

        try:
            account_stat, containers = self.swift.get_account()
        except Exception as err:
            self.app.logger.info("[SERVICE][TRANSFER] {} GET account 'AUTH_{}': {}".format(
                err.http_status,
                project_id,
                err.msg
            ))
            return Response(err.msg, mimetype="text/plain", status=err.http_status)

        container_count = account_stat.get('x-account-container-count')
        object_count = account_stat.get('x-account-object-count')
        bytes_used = account_stat.get('x-account-bytes-used')

        transfer_object = TransferProject.find_transfer_project(project_id)

        if transfer_object:
            transfer_object.object_count_swift = int(object_count)
            transfer_object.bytes_used_swift = int(bytes_used)
            transfer_object.save()

        self.app.logger.info('[SERVICE][TRANSFER] =========================================')
        self.app.logger.info('[SERVICE][TRANSFER] Account: AUTH_{}'.format(project_id))
        self.app.logger.info('[SERVICE][TRANSFER] container_count: {}'.format(container_count))
        self.app.logger.info('[SERVICE][TRANSFER] object_count: {}'.format(object_count))
        self.app.logger.info('[SERVICE][TRANSFER] bytes_used: {}'.format(bytes_used))

        if transfer_object.last_object and len(containers) > 0:
            self.transfer.last_object = transfer_object.last_object
            self.transfer.get_error = transfer_object.get_error
            self.transfer.object_count_gcp = transfer_object.object_count_gcp
            self.transfer.bytes_used_gcp = transfer_object.bytes_used_gcp
            container = transfer_object.last_object.split('/')[0]
            index = [i for i, x in enumerate(containers) if x.get('name') == container][0]
            containers = containers[index:]
            resume = True

        for container in containers:
            self.app.logger.info('[SERVICE][TRANSFER] ----------')
            self.app.logger.info('[SERVICE][TRANSFER] Container: {}'.format(container.get('name')))

            # if container.get('bytes') > 0:
            if resume:
                prefix = '/'.join(self.transfer.last_object.split('/')[1:-1]) + '/'
                marker = '/'.join(self.transfer.last_object.split('/')[1:])

            try:
                if resume:
                    meta, objects = self.swift.get_container(
                        container.get('name'),
                        prefix=prefix,
                        marker=marker
                    )
                    if len(objects) == 0:
                        break
                else:
                    meta, objects = self.swift.get_container(container.get('name'))
            except requests.exceptions.ConnectionError:
                try:
                    self.conn = self.keystone.get_keystone_connection()
                    self.swift = Swift(self.conn, project_id)
                    if resume:
                        meta, objects = self.swift.get_container(
                            container.get('name'),
                            prefix=prefix,
                            marker=marker
                        )
                        if len(objects) == 0:
                            break
                    else:
                        meta, objects = self.swift.get_container(container.get('name'))
                except Exception as err:
                    if resume:
                        path = '{}/{}'.format(container.get('name'), prefix)
                    else:
                        path = container.get('name')

                    self.app.logger.error("[SERVICE][TRANSFER] {} Get container '{}': {}".format(
                        err.http_status,
                        path,
                        err.http_reason
                    ))
                    continue
            except Exception as err:
                if resume:
                    path = '{}/{}'.format(container.get('name'), prefix)
                else:
                    path = container.get('name')

                self.app.logger.error("[SERVICE][TRANSFER] {} Get container '{}': {}".format(
                    err.http_status,
                    path,
                    err.http_reason
                ))
                continue

            if not resume:
                error = False

                try:
                    status, msg = self.swift.put_container(container.get('name'), meta)
                except requests.exceptions.ConnectionError:
                    try:
                        self.conn = self.keystone.get_keystone_connection()
                        self.swift = Swift(self.conn, project_id)
                        status, msg = self.swift.put_container(container.get('name'), meta)
                    except Exception as err:
                        self.app.logger.error("[SERVICE][TRANSFER] {} PUT container '{}': {}".format(
                            err.http_status,
                            container.get('name'),
                            err.http_reason
                        ))
                        error = True
                except Exception as err:
                    self.app.logger.error("[SERVICE][TRANSFER] {} PUT container '{}': {}".format(
                        err.http_status,
                        container.get('name'),
                        err.http_reason
                    ))
                    error = True

                if error == False:
                    self.app.logger.info("[SERVICE][TRANSFER] {} PUT container '{}': {}".format(
                        status,
                        container.get('name'),
                        msg
                    ))
                error = False
            resume = False

            if len(objects) > 0:
                self._get_container(
                    container.get('name'),
                    bucket,
                    objects
                )

        transfer_object = TransferProject.find_transfer_project(project_id)

        if transfer_object:
            transfer_object.last_object = self.transfer.last_object
            transfer_object.get_error = self.transfer.get_error
            transfer_object.object_count_gcp = self.transfer.object_count_gcp
            transfer_object.bytes_used_gcp = self.transfer.bytes_used_gcp
            transfer_object.save()

    def _get_container(self, container, bucket, objects):
        for obj in objects:
            if obj.get('subdir'):
                prefix = obj.get('subdir')
                error = False

                try:
                    status, msg = self.swift.put_object(
                        container,
                        prefix,
                        None,
                        0,
                        'application/directory',
                        None
                    )
                except requests.exceptions.ConnectionError as err:
                    try:
                        self.conn = self.keystone.get_keystone_connection()
                        self.swift = Swift(self.conn, self.project_id)
                        status, msg = self.swift.put_object(
                            container,
                            prefix,
                            None,
                            0,
                            'application/directory',
                            None
                        )
                    except Exception as err:
                        self.app.logger.error("[SERVICE][TRANSFER] {} PUT folder '{}/{}': {}".format(
                            err.http_status,
                            container,
                            obj.get('subdir'),
                            err.http_reason
                        ))
                        error = True
                except Exception as err:
                    self.app.logger.error("[SERVICE][TRANSFER] {} PUT folder '{}/{}': {}".format(
                        err.http_status,
                        container,
                        obj.get('subdir'),
                        err.http_reason
                    ))
                    error = True

                if error == False:
                    self.app.logger.info("[SERVICE][TRANSFER] {} PUT folder '{}/{}': {}".format(
                        status,
                        container,
                        obj.get('subdir'),
                        msg
                    ))
                error = False

                self.transfer.last_object = '{}/{}'.format(container, prefix)
                self.flush_object -= 1

                if self.flush_object == 0:
                    transfer_object = TransferProject.find_transfer_project(self.project_id)

                    if transfer_object:
                        transfer_object.last_object = self.transfer.last_object
                        transfer_object.get_error = self.transfer.get_error
                        transfer_object.object_count_gcp = self.transfer.object_count_gcp
                        transfer_object.bytes_used_gcp = self.transfer.bytes_used_gcp
                        transfer_object.save()

                    self.flush_object = int(os.environ.get("FLUSH_OBJECT", "1000"))

                try:
                    meta, objects = self.swift.get_container(container, prefix=prefix)
                except requests.exceptions.ConnectionError as err:
                    try:
                        self.conn = self.keystone.get_keystone_connection()
                        self.swift = Swift(self.conn, self.project_id)
                        meta, objects = self.swift.get_container(container, prefix=prefix)
                    except Exception as err:
                        self.app.logger.error("[SERVICE][TRANSFER] {} Get container '{}/{}': {}".format(
                            err.http_status,
                            container,
                            prefix,
                            err.http_reason
                        ))
                        continue
                except Exception as err:
                    self.app.logger.error("[SERVICE][TRANSFER] {} Get container '{}/{}': {}".format(
                        err.http_status,
                        container,
                        prefix,
                        err.http_reason
                    ))
                    continue

                if len(objects) > 0:
                    self._get_container(
                        container,
                        bucket,
                        objects
                    )
            else:
                if obj.get('content_type') != 'application/directory':
                    try:
                        headers, content = self.swift.get_object(container, obj.get('name'))
                    except requests.exceptions.ConnectionError:
                        try:
                            self.conn = self.keystone.get_keystone_connection()
                            self.swift = Swift(self.conn, self.project_id)
                            headers, content = self.swift.get_object(container, obj.get('name'))
                        except Exception as err:
                            self.transfer.get_error += 1
                            self.app.logger.error("[SERVICE][TRANSFER] {} Get object '{}/{}': {}".format(
                                err.http_status,
                                container,
                                obj.get('name'),
                                err.http_reason
                            ))
                            continue
                    except Exception as err:
                        self.transfer.get_error += 1
                        self.app.logger.error("[SERVICE][TRANSFER] {} Get object '{}/{}': {}".format(
                            err.http_status,
                            container,
                            obj.get('name'),
                            err.http_reason
                        ))
                        continue

                    try:
                        status, msg = self.swift.put_object(
                            container,
                            obj.get('name'),
                            content,
                            obj.get('bytes'),
                            obj.get('content_type'),
                            headers
                        )
                    except requests.exceptions.ConnectionError as err:
                        try:
                            self.conn = self.keystone.get_keystone_connection()
                            self.swift = Swift(self.conn, self.project_id)
                            status, msg = self.swift.put_object(
                                container,
                                obj.get('name'),
                                content,
                                obj.get('bytes'),
                                obj.get('content_type'),
                                headers
                            )
                        except Exception as err:
                            self.app.logger.error("[SERVICE][TRANSFER] {} PUT object '{}/{}': {}".format(
                                err.http_status,
                                container,
                                obj.get('name'),
                                err.http_reason
                            ))
                            error = True
                    except Exception as err:
                        self.app.logger.error("[SERVICE][TRANSFER] {} PUT object '{}/{}': {}".format(
                            err.http_status,
                            container,
                            obj.get('name'),
                            err.http_reason
                        ))
                        error = True

                    if error == False:
                        self.app.logger.info("[SERVICE][TRANSFER] {} PUT object '{}/{}': {}".format(
                            status,
                            container,
                            obj.get('name'),
                            msg
                        ))
                        self.transfer.object_count_gcp += 1
                        self.transfer.bytes_used_gcp += obj.get('bytes')
                    error = False

                    self.transfer.last_object = '{}/{}'.format(container, obj.get('name'))
                    self.flush_object -= 1

                    if self.flush_object == 0:
                        transfer_object = TransferProject.find_transfer_project(self.project_id)

                        if transfer_object:
                            transfer_object.last_object = self.transfer.last_object
                            transfer_object.get_error = self.transfer.get_error
                            transfer_object.object_count_gcp = self.transfer.object_count_gcp
                            transfer_object.bytes_used_gcp = self.transfer.bytes_used_gcp
                            transfer_object.save()

                        self.flush_object = int(os.environ.get("FLUSH_OBJECT", "1000"))
