# -*- coding: utf-8 -*-
import multiprocessing
import itertools
import requests
import time
import os

from flask import Response
from threading import Thread
from swift_cloud_tools import create_app
from google.cloud.exceptions import NotFound
from swiftclient import client as swift_client

from swift_cloud_tools.server.utils import Keystone, Swift, Google, Transfer
from swift_cloud_tools.models import TransferProject, TransferProjectError, db

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

        self.transfer_object = TransferProject.find_transfer_project(project_id)

        if self.transfer_object:
            self.project_name = self.transfer_object.project_name

        self.keystone = Keystone()
        self.conn = self.keystone.get_keystone_connection()
        self.swift = Swift(self.conn, project_id)
        self.FLUSH_OBJECT = int(os.environ.get("FLUSH_OBJECT", "1000"))

    def synchronize(self, project_id):
        """Get project in swift."""

        self.project_id = project_id
        self.flush_object = self.FLUSH_OBJECT

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
            bucket = gcp_client.get_bucket(
                account,
                timeout=30
            )
        except NotFound:
            bucket = gcp_client.create_bucket(
                account,
                location=BUCKET_LOCATION
            )
            bucket.iam_configuration.uniform_bucket_level_access_enabled = False
            bucket.patch()
        except Exception as err:
            self.app.logger.info('[{}] 500 GET Create bucket: {}'.format(
                transfer_object.project_name,
                err
            ))
            return Response(err, mimetype="text/plain", status=500)

        if transfer_object.last_object:
            resume = True

        try:
            if resume:
                container_last = transfer_object.last_object.split('/')[0]
                account_stat, containers = self.swift.get_account(marker=container_last)
                account_stat, containers = self.swift.get_account(end_marker=containers[0].get('name'))
                account_stat, containers = self.swift.get_account(marker=containers[len(containers) - 2].get('name'))
            else:
                account_stat, containers = self.swift.get_account()
        except requests.exceptions.ConnectionError:
            try:
                self.conn = self.keystone.get_keystone_connection()
                self.swift = Swift(self.conn, project_id)
                if resume:
                    container_last = transfer_object.last_object.split('/')[0]
                    account_stat, containers = self.swift.get_account(marker=container_last)
                    account_stat, containers = self.swift.get_account(end_marker=containers[0].get('name'))
                    account_stat, containers = self.swift.get_account(marker=containers[len(containers) - 2].get('name'))
                else:
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

        container_count = account_stat.get('x-account-container-count', 0)
        object_count = account_stat.get('x-account-object-count', 0)
        bytes_used = account_stat.get('x-account-bytes-used', 0)

        bucket.labels = {
            'container-count': container_count,
            'object-count': object_count,
            'bytes-used': bytes_used
        }
        bucket.patch()

        transfer_object.container_count_swift = int(container_count)
        transfer_object.object_count_swift = int(object_count)
        transfer_object.bytes_used_swift = int(bytes_used)
        transfer_object.save()

        self.app.logger.info('========================================================')
        self.app.logger.info('[{}] Account: AUTH_{}'.format(transfer_object.project_name, project_id))
        self.app.logger.info('[{}] Account Name: {}'.format(transfer_object.project_name, transfer_object.project_name))
        self.app.logger.info('[{}] container_count: {}'.format(transfer_object.project_name, container_count))
        self.app.logger.info('[{}] object_count: {}'.format(transfer_object.project_name, object_count))
        self.app.logger.info('[{}] bytes_used: {}'.format(transfer_object.project_name, bytes_used))

        if transfer_object:
            transfer.last_object = transfer_object.last_object
            transfer.count_error = transfer_object.count_error
            transfer.container_count_gcp = transfer_object.container_count_gcp
            transfer.object_count_gcp = transfer_object.object_count_gcp
            transfer.bytes_used_gcp = transfer_object.bytes_used_gcp

        containers_copy = containers.copy()

        self.app.logger.info('[{}] ---------------------'.format(transfer_object.project_name))
        self.app.logger.info('[{}] Create all containers'.format(transfer_object.project_name))

        while (len(containers_copy) > 0):
            pool = multiprocessing.Pool(processes=10)
            foo = SynchronizeProjects(self.project_id)

            for count in pool.imap_unordered(
                    foo.send_container, self.iterator_slice(containers_copy, self.FLUSH_OBJECT)):
                transfer.container_count_gcp += count
                transfer_object.container_count_gcp = transfer.container_count_gcp
                transfer_object.save()

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

        ########################################

        percentage_page = float(os.environ.get("PERCENTAGE_PAGE", "0.1"))
        page_size = int(int(container_count) * percentage_page) or 1
        start = 0
        end = page_size
        parts = []

        while True:
            res = list(itertools.islice(containers, start, end))
            start += page_size
            end += page_size
            if not res:
                break
            parts.append(res)

        threads = [None] * len(parts)
        results = [None] * len(parts)
        for i in range(len(threads)):
            threads[i] = Thread(target=self.mount_export_tree_async, args=(parts[i], bucket, transfer, transfer_object, resume, results, i))
            threads[i].start()
        for i in range(len(threads)):
            threads[i].join()
        for result in results:
            self.app.logger.info("[{}] Finished page container': {}".format(
                transfer_object.project_name,
                result
            ))

        ########################################

        # for container in containers:
        #     self.app.logger.info('[{}] ----------'.format(transfer_object.project_name))
        #     self.app.logger.info('[{}] Container: {}'.format(
        #         transfer_object.project_name,
        #         container.get('name')
        #     ))

        #     # if container.get('bytes') > 0:
        #     if resume:
        #         prefix = '/'.join(transfer.last_object.split('/')[1:-1]) + '/'
        #         marker = '/'.join(transfer.last_object.split('/')[1:])

        #     try:
        #         if resume:
        #             meta, objects = self.swift.get_container(
        #                 container.get('name'),
        #                 prefix=prefix,
        #                 marker=marker
        #             )
        #             if len(objects) == 0:
        #                 break
        #         else:
        #             meta, objects = self.swift.get_container(container.get('name'))
        #     except requests.exceptions.ConnectionError:
        #         try:
        #             self.conn = self.keystone.get_keystone_connection()
        #             self.swift = Swift(self.conn, project_id)
        #             if resume:
        #                 meta, objects = self.swift.get_container(
        #                     container.get('name'),
        #                     prefix=prefix,
        #                     marker=marker
        #                 )
        #                 if len(objects) == 0:
        #                     break
        #             else:
        #                 meta, objects = self.swift.get_container(container.get('name'))
        #         except Exception as err:
        #             if resume:
        #                 path = '{}/{}'.format(container.get('name'), prefix)
        #             else:
        #                 path = container.get('name')

        #             self.app.logger.error("[{}] {} Get container '{}': {}".format(
        #                 transfer_object.project_name,
        #                 err.http_status,
        #                 path,
        #                 err.http_reason
        #             ))
        #             continue
        #     except Exception as err:
        #         if resume:
        #             path = '{}/{}'.format(container.get('name'), prefix)
        #         else:
        #             path = container.get('name')

        #         self.app.logger.error("[{}] {} Get container '{}': {}".format(
        #             transfer_object.project_name,
        #             err.http_status,
        #             path,
        #             err.http_reason
        #         ))
        #         continue

        #     resume = False

        #     if len(objects) > 0:
        #         self._get_container(
        #             container.get('name'),
        #             bucket,
        #             transfer_object,
        #             transfer,
        #             objects
        #         )

        # transfer_object.last_object = transfer.last_object
        # transfer_object.count_error = transfer.count_error
        # transfer_object.container_count_gcp = transfer.container_count_gcp
        # transfer_object.object_count_gcp = transfer.object_count_gcp
        # transfer_object.bytes_used_gcp = transfer.bytes_used_gcp
        # transfer_object.save()

        ########################################

    def mount_export_tree_async(self, containers, bucket, transfer, transfer_object, resume, result, index):
        app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
        ctx = app.app_context()
        ctx.push()

        for container in containers:
            app.logger.info('[{}] ----------'.format(transfer_object.project_name))
            app.logger.info('[{}] Container: {}'.format(
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

                    app.logger.error("[{}] {} Get container '{}': {}".format(
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

                app.logger.error("[{}] {} Get container '{}': {}".format(
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
                    bucket,
                    transfer_object,
                    transfer,
                    objects
                )

        app.logger.error("[{}] 201 Finished container '{}'".format(
            transfer_object.project_name,
            container.get('name')
        ))

        db.session.begin()
        transfer_project = TransferProject.query.filter_by(project_id=transfer_object.project_id).first()
        transfer_project.last_object = transfer.last_object
        transfer_project.count_error = transfer.count_error
        transfer_project.object_count_gcp = transfer.object_count_gcp
        transfer_project.bytes_used_gcp = transfer.bytes_used_gcp
        db.session.commit()

        result[index] = 'ok'


    def _get_container(self, container, bucket, transfer_object, transfer, objects):
        for obj in objects:
            if obj.get('subdir'):
                prefix = obj.get('subdir')

                if not prefix:
                    self.app.logger.info("[{}] 500 PUT folder '{}/None': Prefix None".format(
                        transfer_object.project_name,
                        container
                    ))
                    continue

                blob = bucket.blob('{}/{}'.format(container, prefix))
                blob.upload_from_string('',
                    content_type='application/directory',
                    num_retries=3,
                    timeout=30
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
                    transfer_object.save()

                    self.flush_object = self.FLUSH_OBJECT

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
                            transfer.count_error += 1
                            obj_path = "{}/{}".format(container, obj.get('name'))

                            transfer_error = TransferProjectError(
                                object_error=obj_path,
                                transfer_project_id=transfer_object.id
                            )
                            transfer_error.save()

                            self.app.logger.error("[{}] {} Get object '{}/{}': {}".format(
                                transfer_object.project_name,
                                err.http_status,
                                container,
                                obj.get('name'),
                                err.http_reason
                            ))
                            continue
                    except Exception as err:
                        transfer.count_error += 1
                        obj_path = "{}/{}".format(container, obj.get('name'))

                        transfer_error = TransferProjectError(
                            object_error=obj_path,
                            transfer_project_id=transfer_object.id
                        )
                        transfer_error.save()

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
                        timeout=900
                    )
                    self.app.logger.info("[{}] 201 PUT object '{}' {} {}: Created".format(
                        transfer_object.project_name,
                        obj_path,
                        obj.get('content_type'),
                        len(content)
                    ))

                    content = None

                    transfer.object_count_gcp += 1
                    transfer.bytes_used_gcp += obj.get('bytes')
                    transfer.last_object = '{}/{}'.format(container, obj.get('name'))
                    self.flush_object -= 1

                    if self.flush_object == 0:
                        transfer_object.last_object = transfer.last_object
                        transfer_object.count_error = transfer.count_error
                        transfer_object.object_count_gcp = transfer.object_count_gcp
                        transfer_object.bytes_used_gcp = transfer.bytes_used_gcp
                        transfer_object.save()

                        self.flush_object = self.FLUSH_OBJECT

    def send_container(self, containers):
        app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
        ctx = app.app_context()
        ctx.push()

        error = False
        count = 0
        container_name = None

        google = Google()
        gcp_client = google.get_gcp_client()
        account = 'auth_{}'.format(self.project_id)

        try:
            bucket = gcp_client.get_bucket(
                account,
                timeout=30
            )
        except requests.exceptions.ReadTimeout as err:
            try:
                bucket = gcp_client.get_bucket(
                    account,
                    timeout=30
                )
            except Exception as err:
                app.logger.error("[{}] {} Get bucket '{}': {}".format(
                    self.project_name,
                    err.http_status,
                    account,
                    err.http_reason
                ))

        for container in containers:
            try:
                container_name = container.get('name')
                meta, objects = self.swift.get_container(container_name)
            except requests.exceptions.ConnectionError:
                try:
                    self.conn = self.keystone.get_keystone_connection()
                    self.swift = Swift(self.conn, self.project_id)
                    meta, objects = self.swift.get_container(container_name)
                except Exception as err:
                    app.logger.error("[{}] {} Get container '{}': {}".format(
                        self.project_name,
                        err.http_status,
                        container_name,
                        err.http_reason
                    ))
                    error = True
            except Exception as err:
                app.logger.error("[{}] {} Get container '{}': {}".format(
                    self.project_name,
                    err.http_status,
                    container_name,
                    err.http_reason
                ))
                error = True

            if not error:
                blob = bucket.blob(container_name + '/')
                metadata = {}

                metadata['object-count'] = meta.get('x-container-object-count', 0)
                metadata['bytes-used'] = meta.get('x-container-bytes-used', 0)

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
                    timeout=30
                )
                app.logger.info("[{}] 201 PUT container '{}': Created".format(
                    self.project_name,
                    container_name
                ))
                count +=1
        return count

    def iterator_slice(self, iterator, length):
        start = 0
        end = length

        while True:
            res = list(itertools.islice(iterator, start, end))
            start += length
            end += length
            if not res:
                break
            yield res
