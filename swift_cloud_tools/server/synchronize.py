# -*- coding: utf-8 -*-
import requests
import time
import os

from flask import Response
from swift_cloud_tools import create_app
from google.cloud.exceptions import NotFound
from swiftclient import client as swift_client

from swift_cloud_tools.server.utils import Keystone, Swift, Google
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
        """Get projects in swift."""

        self.project_id = project_id

        self.app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
        ctx = self.app.app_context()
        ctx.push()

        keystone = Keystone()
        self.conn = keystone.get_keystone_connection()
        self.swift = Swift(self.conn, project_id)
        google = Google()

        status, msg = self.swift.set_account_meta_cloud()

        self.app.logger.info("[SERVICE][TRANSFER] {} SET account_meta_cloud 'AUTH_{}': {}".format(
            status,
            project_id,
            msg
        ))

        if status != 204:
            return Response(msg, mimetype="text/plain", status=status)

        time.sleep(5)

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

        self.app.logger.info('[SERVICE][TRANSFER] =========================================')
        self.app.logger.info('[SERVICE][TRANSFER] Account: {}'.format(project_id))
        self.app.logger.info('[SERVICE][TRANSFER] container_count: {}'.format(container_count))
        self.app.logger.info('[SERVICE][TRANSFER] object_count: {}'.format(object_count))
        self.app.logger.info('[SERVICE][TRANSFER] bytes_used: {}'.format(bytes_used))

        transfer_object = TransferProject.find_transfer_project(project_id)

        if transfer_object:
            transfer_object.object_count = int(object_count)
            transfer_object.bytes_used = int(bytes_used)
            transfer_object.save()

        if len(containers) > 0:
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

        for container in containers:
            self.app.logger.info('[SERVICE][TRANSFER] ----------')
            self.app.logger.info('[SERVICE][TRANSFER] Container: {}'.format(container.get('name')))

            # if container.get('bytes') > 0:
            try:
                meta, objects = self.swift.get_container(container.get('name'))
            except requests.exceptions.ConnectionError as err:
                try:
                    # import ipdb;ipdb.set_trace()
                    self.conn = keystone.get_keystone_connection()
                    self.swift = Swift(self.conn, project_id)
                    meta, objects = self.swift.get_container(container.get('name'))
                except Exception as err:
                    self.app.logger.info("[SERVICE][TRANSFER] {} Get container '{}': {}".format(
                        err.http_status,
                        container.get('name'),
                        err.msg
                    ))
                    continue
            except Exception as err:
                self.app.logger.info("[SERVICE][TRANSFER] {} Get container '{}': {}".format(
                    err.http_status,
                    container.get('name'),
                    err.msg
                ))
                continue

            status, msg = self.swift.put_container(container.get('name'), meta)

            self.app.logger.info("[SERVICE][TRANSFER] {} PUT container '{}': {}".format(
                status,
                container.get('name'),
                msg
            ))

            if len(objects) > 0:
                self._get_container(
                    container.get('name'),
                    bucket,
                    objects
                )

    def _get_container(self, container, bucket, objects):
        for obj in objects:
            if obj.get('subdir'):
                prefix = obj.get('subdir')

                try:
                    meta, objects = self.swift.get_container(container, prefix)
                except requests.exceptions.ConnectionError as err:
                    try:
                        # import ipdb;ipdb.set_trace()
                        self.conn = keystone.get_keystone_connection()
                        self.swift = Swift(self.conn, project_id)
                        meta, objects = self.swift.get_container(container.get('name'))
                    except Exception as err:
                        self.app.logger.info("[SERVICE][TRANSFER] {} Get container '{}/{}': {}".format(
                            err.http_status,
                            container,
                            prefix,
                            err.msg
                        ))
                        continue
                except Exception as err:
                    self.app.logger.info("[SERVICE][TRANSFER] {} Get container '{}/{}': {}".format(
                        err.http_status,
                        container,
                        prefix,
                        err.msg
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
                    except requests.exceptions.ConnectionError as err:
                        # err.request.path_url
                        # err.request.url
                        try:
                            # import ipdb;ipdb.set_trace()
                            self.conn = keystone.get_keystone_connection()
                            self.swift = Swift(self.conn, project_id)
                            headers, content = self.swift.get_object(container, obj.get('name'))
                        except Exception as err:
                            self.app.logger.info("[SERVICE][TRANSFER] {} Get object '{}/{}': {}".format(
                                err.http_status,
                                container,
                                obj.get('name'),
                                err.msg
                            ))
                            continue
                    except Exception as err:
                        self.app.logger.info("[SERVICE][TRANSFER] {} Get object '{}/{}': {}".format(
                            err.http_status,
                            container,
                            obj.get('name'),
                            err.msg
                        ))
                        continue

                    status, msg = self.swift.put_object(
                        container,
                        obj.get('name'),
                        content,
                        obj.get('bytes'),
                        obj.get('content_type'),
                        headers
                    )

                    self.app.logger.info("[SERVICE][TRANSFER] {} PUT object '{}/{}': {}".format(
                        status,
                        container,
                        obj.get('name'),
                        msg
                    ))
