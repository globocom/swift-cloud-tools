# -*- coding: utf-8 -*-
import multiprocessing
import itertools
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

    def __init__(self, project_id):
        self.project_id = project_id
        self.project_name = None
        self.bucket = None

        self.transfer_object = TransferProject.find_transfer_project(project_id)

        if self.transfer_object:
            self.project_name = self.transfer_object.project_name

        self.keystone = Keystone()
        self.conn = self.keystone.get_keystone_connection()
        self.swift = Swift(self.conn, project_id)

    def synchronize(self, project_id):
        """Get project in swift."""

        self.project_id = project_id
        self.flush_object = int(os.environ.get("FLUSH_OBJECT", "1000"))

        self.app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
        ctx = self.app.app_context()
        ctx.push()

        google = Google()
        transfer = Transfer()
        resume = False

        transfer_object = TransferProject.find_transfer_project(project_id)

        status, msg = self.swift.set_account_meta_cloud()

        self.app.logger.info('========================================================')
        self.app.logger.info("[{}] {} SET account_meta_cloud 'AUTH_{}': {}".format(
            transfer_object.project_name,
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
            self.bucket = gcp_client.get_bucket(account)
        except NotFound:
            self.bucket = gcp_client.create_bucket(
                account,
                location=BUCKET_LOCATION
            )
            self.bucket.iam_configuration.uniform_bucket_level_access_enabled = False
            self.bucket.patch()
        except Exception as err:
            self.app.logger.info('[{}] 500 GET Create bucket: {}'.format(
                transfer_object.project_name,
                err
            ))
            return Response(err, mimetype="text/plain", status=500)

        try:
            account_stat, containers = self.swift.get_account()
        except requests.exceptions.ConnectionError:
            try:
                self.conn = self.keystone.get_keystone_connection()
                self.swift = Swift(self.conn, project_id)
                account_stat, containers = self.swift.get_account()
            except Exception as err:
                self.app.logger.info("[{}] {} GET account 'AUTH_{}': {}".format(
                    transfer_object.project_name,
                    err.http_status,
                    project_id,
                    err.http_reason
                ))
        except Exception as err:
            self.app.logger.info("[{}] {} GET account 'AUTH_{}': {}".format(
                transfer_object.project_name,
                err.http_status,
                project_id,
                err.http_reason
            ))
            return Response(err.msg, mimetype="text/plain", status=err.http_status)

        container_count = account_stat.get('x-account-container-count')
        object_count = account_stat.get('x-account-object-count')
        bytes_used = account_stat.get('x-account-bytes-used')

        transfer_object.object_count_swift = int(object_count)
        transfer_object.bytes_used_swift = int(bytes_used)
        transfer_object.save()

        self.app.logger.info('========================================================')
        self.app.logger.info('[{}] Account: AUTH_{}'.format(transfer_object.project_name, project_id))
        self.app.logger.info('[{}] Account Name: {}'.format(transfer_object.project_name, transfer_object.project_name))
        self.app.logger.info('[{}] container_count: {}'.format(transfer_object.project_name, container_count))
        self.app.logger.info('[{}] object_count: {}'.format(transfer_object.project_name, object_count))
        self.app.logger.info('[{}] bytes_used: {}'.format(transfer_object.project_name, bytes_used))

        if transfer_object.last_object and len(containers) > 0:
            transfer.last_object = transfer_object.last_object
            transfer.get_error = transfer_object.get_error
            transfer.object_count_gcp = transfer_object.object_count_gcp
            transfer.bytes_used_gcp = transfer_object.bytes_used_gcp
            container = transfer_object.last_object.split('/')[0]
            index = [i for i, x in enumerate(containers) if x.get('name') == container][0]
            containers = containers[index:]
            resume = True

        containers_copy = containers.copy()

        self.app.logger.info('[{}] ---------------------'.format(transfer_object.project_name))
        self.app.logger.info('[{}] Create all containers'.format(transfer_object.project_name))

        while (len(containers_copy) > 0):
            pool = multiprocessing.Pool(processes=6)
            foo = SynchronizeProjects(self.project_id)

            for _ in pool.imap_unordered(
                    foo.send_container, self.iterator_slice(containers_copy, 1)):
                pass

            pool.close()

            try:
                account_stat, containers_copy = self.swift.get_account(marker=containers[-1].get('name'))
            except requests.exceptions.ConnectionError:
                try:
                    self.conn = self.keystone.get_keystone_connection()
                    self.swift = Swift(self.conn, project_id)
                    account_stat, containers_copy = self.swift.get_account(marker=containers[-1].get('name'))
                except Exception as err:
                    self.app.logger.info("[{}] {} GET account 'AUTH_{}': {}".format(
                        transfer_object.project_name,
                        err.http_status,
                        project_id,
                        err.http_reason
                    ))
                    containers_copy = containers = []
            except Exception as err:
                self.app.logger.info("[{}] {} GET account 'AUTH_{}': {}".format(
                    transfer_object.project_name,
                    err.http_status,
                    project_id,
                    err.http_reason
                ))
                containers_copy = containers = []

        for container in containers:
            self.app.logger.info('[{}] ----------'.format(transfer_object.project_name))
            self.app.logger.info('[{}] Container: {}'.format(
                transfer_object.project_name,
                container.get('name')
            ))

            # if container.get('bytes') > 0:
            if resume:
                prefix = '/'.join(transfer.last_object.split('/')[1:-1]) + '/'
                marker = '/'.join(transfer.last_object.split('/')[1:])

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

                    self.app.logger.error("[{}] {} Get container '{}': {}".format(
                        transfer_object.project_name,
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

                self.app.logger.error("[{}] {} Get container '{}': {}".format(
                    transfer_object.project_name,
                    err.http_status,
                    path,
                    err.http_reason
                ))
                continue

            resume = False

            if len(objects) > 0:
                self._get_container(
                    container.get('name'),
                    self.bucket,
                    transfer_object,
                    transfer,
                    objects
                )

        transfer_object.last_object = transfer.last_object
        transfer_object.get_error = transfer.get_error
        transfer_object.object_count_gcp = transfer.object_count_gcp
        transfer_object.bytes_used_gcp = transfer.bytes_used_gcp
        transfer_object.save()

    def _get_container(self, container, bucket, transfer_object, transfer, objects):
        for obj in objects:
            if obj.get('subdir'):
                prefix = obj.get('subdir')

                blob = bucket.blob('{}/{}'.format(container, prefix))
                blob.upload_from_string('',
                    content_type='application/directory',
                    num_retries=3,
                    timeout=120
                )
                self.app.logger.info("[{}] 201 PUT folder '{}/{}': Created".format(
                    transfer_object.project_name,
                    container,
                    prefix
                ))

                transfer.last_object = '{}/{}'.format(container, prefix)
                self.flush_object -= 1

                if self.flush_object == 0:
                    transfer_object.last_object = transfer.last_object
                    transfer_object.get_error = transfer.get_error
                    transfer_object.object_count_gcp = transfer.object_count_gcp
                    transfer_object.bytes_used_gcp = transfer.bytes_used_gcp
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
                        self.app.logger.error("[{}] {} Get container '{}/{}': {}".format(
                            transfer_object.project_name,
                            err.http_status,
                            container,
                            prefix,
                            err.http_reason
                        ))
                        continue
                except Exception as err:
                    self.app.logger.error("[{}] {} Get container '{}/{}': {}".format(
                        transfer_object.project_name,
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
                        transfer_object,
                        transfer,
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
                            transfer.get_error += 1
                            self.app.logger.error("[{}] {} Get object '{}/{}': {}".format(
                                transfer_object.project_name,
                                err.http_status,
                                container,
                                obj.get('name'),
                                err.http_reason
                            ))
                            continue
                    except Exception as err:
                        transfer.get_error += 1
                        self.app.logger.error("[{}] {} Get object '{}/{}': {}".format(
                            transfer_object.project_name,
                            err.http_status,
                            container,
                            obj.get('name'),
                            err.http_reason
                        ))
                        continue

                    obj_path = "{}/{}".format(container, obj.get('name'))
                    blob = bucket.blob(obj_path)

                    if headers.get('cache-control'):
                        blob.cache_control = headers.get('cache-control')

                    if headers.get('content-encoding'):
                        blob.content_encoding = headers.get('content-encoding')

                    if headers.get('content-disposition'):
                        blob.content_disposition = headers.get('content-disposition')

                    metadata = {}

                    meta_keys = list(filter(
                        lambda x: 'x-object-meta' in x.lower(),
                        [*headers.keys()]
                    ))

                    reserved_keys = list(filter(
                        lambda x: x.lower() in RESERVED_META,
                        [*headers.keys()]
                    ))

                    for item in meta_keys:
                        key = item.lower().split('x-object-meta-')[-1]
                        metadata[key] = headers.get(item)

                    for item in reserved_keys:
                        key = item.lower()
                        metadata[key] = headers.get(item)

                    if len(meta_keys) or len(reserved_keys):
                        blob.metadata = metadata

                    blob.upload_from_string(
                        content,
                        content_type=obj.get('content_type'),
                        num_retries=3,
                        timeout=120
                    )
                    self.app.logger.info("[{}] 201 PUT object '{}/{}': Created".format(
                        transfer_object.project_name,
                        container,
                        obj.get('name')
                    ))

                    transfer.object_count_gcp += 1
                    transfer.bytes_used_gcp += obj.get('bytes')
                    transfer.last_object = '{}/{}'.format(container, obj.get('name'))
                    self.flush_object -= 1

                    if self.flush_object == 0:
                        transfer_object.last_object = transfer.last_object
                        transfer_object.get_error = transfer.get_error
                        transfer_object.object_count_gcp = transfer.object_count_gcp
                        transfer_object.bytes_used_gcp = transfer.bytes_used_gcp
                        transfer_object.save()

                        self.flush_object = int(os.environ.get("FLUSH_OBJECT", "1000"))

    def send_container(self, container):
        app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
        ctx = app.app_context()
        ctx.push()

        bucket = None

        if self.bucket:
            bucket = self.bucket
        else:
            google = Google()
            gcp_client = google.get_gcp_client()
            account = 'auth_{}'.format(self.project_id)
            self.bucket = bucket = gcp_client.get_bucket(account)

        try:
            meta, objects = self.swift.get_container(container.get('name'))
        except requests.exceptions.ConnectionError:
            try:
                self.conn = self.keystone.get_keystone_connection()
                self.swift = Swift(self.conn, self.project_id)
                meta, objects = self.swift.get_container(container.get('name'))
            except Exception as err:
                app.logger.error("[{}] {} Get container '{}': {}".format(
                    self.project_name,
                    err.http_status,
                    container.get('name'),
                    err.http_reason
                ))
                return []
        except Exception as err:
            app.logger.error("[{}] {} Get container '{}': {}".format(
                self.project_name,
                err.http_status,
                container.get('name'),
                err.http_reason
            ))
            return []

        blob = bucket.blob(container.get('name') + '/')
        metadata = {}

        for item in meta.items():
            key, value = item
            key = key.lower()
            prefix = key.split('x-container-meta-')

            if len(prefix) > 1:
                meta_key = 'meta-{}'.format(prefix[1].lower())
                metadata[meta_key] = item[1].lower()
                continue

            if key == 'x-container-read':
                metadata["read"] = value
                continue

            if key == 'x-versions-location' or key == 'x-history-location':
                metadata["x-versions-location"] = value
                continue

            if key == 'x-undelete-enabled':
                metadata["x-container-sysmeta-undelete-enabled"] = value
                metadata["x-undelete-enabled"] = value
                continue

        blob.metadata = metadata
        blob.upload_from_string('',
            content_type='application/directory',
            num_retries=3,
            timeout=120
        )
        app.logger.info("[{}] 201 PUT container '{}': Created".format(
            self.project_name,
            container.get('name')
        ))
        return []

    def iterator_slice(self, iterator, length):
        start = 0
        end = length

        while True:
            res = list(itertools.islice(iterator, start, end))
            start += length
            end += length
            if not res:
                break
            yield res[0]
