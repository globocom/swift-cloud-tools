# -*- coding: utf-8 -*-
import itertools
import requests
import time
import json
import os
import gc

from datetime import datetime
from flask import Response
from threading import Thread
from swift_cloud_tools import create_app
from google.auth.exceptions import TransportError
from google.api_core.exceptions import BadRequest, NotFound
from swiftclient import client as swift_client
from keystoneauth1.exceptions.auth import AuthorizationFailure
from http.client import IncompleteRead

from swift_cloud_tools.server.utils import Keystone, Swift, Google, Transfer
from swift_cloud_tools.models import TransferProject, TransferProjectError, db

BUCKET_LOCATION = os.environ.get('BUCKET_LOCATION', 'US-EAST1')
RESERVED_META = [
    'x-delete-at',
    'x-delete-after',
    'x-versions-location',
    'x-history-location',
    'x-undelete-enabled',
    'x-container-sysmeta-undelete-enabled'
]


class SynchronizeProjects():

    def __init__(self, project_id, hostname):
        self.project_id = project_id
        self.project_name = None
        self.hostname = hostname

        self.transfer_object = TransferProject.find_transfer_project(project_id)

        if self.transfer_object:
            self.project_name = self.transfer_object.project_name

        self.keystone = Keystone()
        self.conn = self.keystone.get_keystone_connection()
        self.swift = Swift(self.conn, project_id)
        self.FLUSH_OBJECT = int(os.environ.get("FLUSH_OBJECT", "1000"))

    def synchronize(self, project_id, hostname):
        """Get project in swift."""

        self.project_id = project_id
        self.flush_object = self.FLUSH_OBJECT
        self.hostname = hostname

        self.app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
        ctx = self.app.app_context()
        ctx.push()

        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        session.mount('https://', adapter)
        session.mount('http://', adapter)

        google = Google()
        transfer = Transfer()
        resume = False

        transfer_object = TransferProject.find_transfer_project(project_id)

        storage_client = google.get_storage_client()
        account = 'auth_{}'.format(project_id)

        try:
            bucket = storage_client.get_bucket(
                account,
                timeout=30
            )
        except NotFound:
            bucket = storage_client.create_bucket(
                account,
                location=BUCKET_LOCATION
            )

            labels = bucket.labels
            labels['account-meta-cloud'] = 'gcp'
            labels['container-count'] = 0
            labels['object-count'] = 0
            labels['bytes-used'] = 0
            bucket.labels = labels

            # bucket.iam_configuration.uniform_bucket_level_access_enabled = False
            bucket.patch()
        except Exception as err:
            self.app.logger.error('[{}] 500 GET Create bucket: {}'.format(
                transfer_object.project_name,
                err
            ))
            return Response(err, mimetype="text/plain", status=500)

        self.app.logger.info('========================================================')
        self.app.logger.info("[{}] SET account_meta_cloud 'AUTH_{}'".format(
            transfer_object.project_name,
            project_id
        ))

        if transfer_object.last_object:
            resume = True

        self.app.logger.info('[{}] Resume: {}'.format(
            transfer_object.project_name,
            resume
        ))

        try:
            if resume:
                container_last = transfer_object.last_object.split('/')[0]
                account_stat, containers = self.swift.get_account(marker=container_last)

                if len(containers) > 0:
                    account_stat, containers = self.swift.get_account(end_marker=containers[0].get('name'))

                    if len(containers) > 1:
                        account_stat, containers = self.swift.get_account(marker=containers[len(containers) - 2].get('name'))
                else:
                    account_stat, containers = self.swift.get_account()
            else:
                account_stat, containers = self.swift.get_account()
        except requests.exceptions.ConnectionError:
            try:
                self.conn = self.keystone.get_keystone_connection()
                self.swift = Swift(self.conn, project_id)

                if resume:
                    container_last = transfer_object.last_object.split('/')[0]
                    account_stat, containers = self.swift.get_account(marker=container_last)

                    if len(containers) > 0:
                        account_stat, containers = self.swift.get_account(end_marker=containers[0].get('name'))

                        if len(containers) > 1:
                            account_stat, containers = self.swift.get_account(marker=containers[len(containers) - 2].get('name'))
                    else:
                        account_stat, containers = self.swift.get_account()
                else:
                    account_stat, containers = self.swift.get_account()
            except AuthorizationFailure:
                app.logger.error("[{}] 500 GET account 'AUTH_{}': Keystone authorization failure".format(
                    transfer_object.project_name,
                    project_id
                ))
            except Exception as err:
                self.app.logger.error("[{}] 500 GET account 'AUTH_{}': {}".format(
                    transfer_object.project_name,
                    project_id,
                    err
                ))
        except Exception as err:
            self.app.logger.error("[{}] 500 GET account 'AUTH_{}': {}".format(
                transfer_object.project_name,
                project_id,
                err
            ))
            return Response(err.msg, mimetype="text/plain", status=err.http_status)

        container_count = int(account_stat.get('x-account-container-count', 0))
        object_count = int(account_stat.get('x-account-object-count', 0))
        bytes_used = int(account_stat.get('x-account-bytes-used', 0))

        if not resume:
            transfer_object.container_count_swift = container_count
            transfer_object.object_count_swift = object_count
            transfer_object.bytes_used_swift = bytes_used
            transfer_object.save()

        self.app.logger.info('========================================================')
        self.app.logger.info('[{}] Account: AUTH_{}'.format(transfer_object.project_name, project_id))
        self.app.logger.info('[{}] Account Name: {}'.format(transfer_object.project_name, transfer_object.project_name))
        self.app.logger.info('[{}] container_count: {}'.format(transfer_object.project_name, container_count))
        self.app.logger.info('[{}] object_count: {}'.format(transfer_object.project_name, object_count))
        self.app.logger.info('[{}] bytes_used: {}'.format(transfer_object.project_name, bytes_used))
        self.app.logger.info('[{}] host_name: {}'.format(transfer_object.project_name, self.hostname))

        if resume:
            self.app.logger.info('========================================================')
            self.app.logger.info('[{}] Resumed containers: count {}'.format(
                transfer_object.project_name,
                len(containers)
            ))
            self.app.logger.info('[{}] Resumed containers: {}'.format(
                transfer_object.project_name,
                containers
            ))

        if transfer_object:
            transfer.last_object = transfer_object.last_object
            transfer.count_error = transfer_object.count_error
            transfer.container_count_gcp = transfer_object.container_count_gcp
            transfer.object_count_gcp = transfer_object.object_count_gcp
            transfer.bytes_used_gcp = transfer_object.bytes_used_gcp

        ########################################
        #              Containers              #
        ########################################

        if transfer_object.container_count_gcp < container_count:
            containers_copy = containers.copy()

            self.app.logger.info('[{}] ---------------------'.format(transfer_object.project_name))
            self.app.logger.info('[{}] Create all containers'.format(transfer_object.project_name))

            while (len(containers_copy) > 0):
                page_size = 1
                parts = []

                if len(containers_copy) > 35:
                    percentage_page = float(os.environ.get("PERCENTAGE_PAGE", "0.1"))
                    page_size = int(len(containers_copy) * percentage_page) or 1

                start = 0
                end = page_size

                while True:
                    res = list(itertools.islice(containers_copy, start, end))
                    start += page_size
                    end += page_size
                    if not res:
                        break
                    parts.append(res)

                threads = [None] * len(parts)
                results = [None] * len(parts)
                counts = [0] * len(parts)

                for i in range(len(threads)):
                    time.sleep(0.5)
                    threads[i] = Thread(target=self.send_container, args=(self.app, storage_client, parts[i], transfer, transfer_object, results, counts, i))
                    threads[i].start()
                for i in range(len(threads)):
                    threads[i].join()
                for i, result in enumerate(results):
                    time.sleep(0.1)
                    transfer.container_count_gcp += counts[i]

                    db.session.begin()
                    transfer_project = db.session.query(TransferProject).filter_by(project_id=transfer_object.project_id).first()
                    transfer_project.container_count_gcp = transfer.container_count_gcp
                    time.sleep(0.1)
                    db.session.commit()

                    labels = bucket.labels
                    labels['container-count'] = transfer.container_count_gcp
                    bucket.labels = labels
                    time.sleep(2)
                    bucket.patch()

                    del transfer_project
                    gc.collect()
                    self.app.logger.info("[{}] Finished page container': {} - {}".format(
                        transfer_object.project_name, i,
                        counts[i]
                    ))

                try:
                    account_stat, containers_copy = self.swift.get_account(marker=containers_copy[-1].get('name'))
                except requests.exceptions.ConnectionError:
                    try:
                        self.conn = self.keystone.get_keystone_connection()
                        self.swift = Swift(self.conn, project_id)
                        account_stat, containers_copy = self.swift.get_account(marker=containers_copy[-1].get('name'))
                    except AuthorizationFailure:
                        app.logger.error("[{}] 500 Get account 'AUTH_{}': Keystone authorization failure".format(
                            transfer_object.project_name,
                            project_id
                        ))
                    except Exception as err:
                        self.app.logger.error("[{}] 500 GET account 'AUTH_{}': {}".format(
                            transfer_object.project_name,
                            project_id,
                            err
                        ))
                        containers_copy = containers = []
                except Exception as err:
                    self.app.logger.error("[{}] 500 GET account 'AUTH_{}': {}".format(
                        transfer_object.project_name,
                        project_id,
                        err
                    ))
                    containers_copy = containers = []

        ########################################
        #               Objects                #
        ########################################

        page_size = 1
        parts = []

        self.app.logger.info('========================================================')
        self.app.logger.info('[{}] Containers: count {}'.format(
            transfer_object.project_name,
            len(containers)
        ))
        self.app.logger.info('[{}] Containers: {}'.format(
            transfer_object.project_name,
            containers
        ))

        if len(containers) > 35:
            percentage_page = float(os.environ.get("PERCENTAGE_PAGE", "0.1"))
            page_size = int(container_count * percentage_page) or 1

        start = 0
        end = page_size

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
            time.sleep(0.5)
            threads[i] = Thread(target=self.send_object, args=(self.app, parts[i], bucket, transfer, transfer_object, results, i))
            threads[i].start()
        for i in range(len(threads)):
            threads[i].join()
        for i, result in enumerate(results):
            time.sleep(0.1)
            self.app.logger.info("[{}] Finished page container': {} - {}".format(
                transfer_object.project_name, i,
                result
            ))

        status, msg = self.swift.set_account_meta_cloud_migration()

        self.app.logger.info('========================================================')
        self.app.logger.info("[{}] {} SET account_meta_cloud_migration 'AUTH_{}': {}".format(
            transfer_object.project_name,
            status,
            project_id,
            msg
        ))

        if status != 204:
            return Response(msg, mimetype="text/plain", status=status)


    def send_container(self, app, storage_client, containers, transfer, transfer_object, result, counts, index):
        error = False
        container_name = None
        account = 'auth_{}'.format(self.project_id)

        try:
            bucket = storage_client.get_bucket(
                account,
                timeout=30
            )
        except TransportError:
            try:
                bucket = storage_client.get_bucket(
                    account,
                    timeout=30
                )
            except Exception as err:
                app.logger.error("[{}] 500 Get bucket '{}': {}".format(
                    self.project_name,
                    account,
                    err
                ))
        except requests.exceptions.ReadTimeout as err:
            try:
                bucket = storage_client.get_bucket(
                    account,
                    timeout=30
                )
            except Exception as err:
                app.logger.error("[{}] 504 Get bucket '{}': {}".format(
                    self.project_name,
                    account,
                    err
                ))
        except Exception as err:
            app.logger.error('[{}] 500 GET bucket: {}'.format(
                self.project_name,
                err
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
                except AuthorizationFailure:
                    app.logger.error("[{}] 500 Get container '{}': Keystone authorization failure".format(
                        self.project_name,
                        container_name
                    ))
                    error = True
                except Exception as err:
                    app.logger.error("[{}] 500 Get container '{}': {}".format(
                        self.project_name,
                        container_name,
                        err
                    ))
                    error = True
            except json.decoder.JSONDecodeError:
                try:
                    meta, objects = self.swift.get_container(container_name)
                except Exception as e:
                    app.logger.error("[{}] 500 Get container '{}': {}".format(
                        self.project_name,
                        container_name,
                        err
                    ))
                    error = True
            except Exception as err:
                app.logger.error("[{}] 500 Get container '{}': {}".format(
                    self.project_name,
                    container_name,
                    err
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
                try:
                    blob.upload_from_string('',
                        content_type='application/directory',
                        num_retries=3,
                        timeout=30
                    )
                    # app.logger.info("[{}] 201 PUT container '{}': Created".format(
                    #     self.project_name,
                    #     container_name
                    # ))
                    counts[index] += 1
                except BadRequest:
                    transfer.count_error += 1
                    app.logger.error("[{}] 400 PUT container '{}': BadRequest".format(
                        self.project_name,
                        container_name
                    ))
                    transfer_error = TransferProjectError(
                        object_error=container_name,
                        transfer_project_id=transfer_object.id,
                        created=datetime.now()
                    )
                    transfer_error.save()
                except requests.exceptions.ReadTimeout:
                    transfer.count_error += 1
                    app.logger.error("[{}] 504 PUT container '{}': ReadTimeout".format(
                        self.project_name,
                        container_name
                    ))
                    transfer_error = TransferProjectError(
                        object_error=container_name,
                        transfer_project_id=transfer_object.id,
                        created=datetime.now()
                    )
                    transfer_error.save()
                except Exception as err:
                    transfer.count_error += 1
                    app.logger.error("[{}] 500 PUT container '{}': {}".format(
                        self.project_name,
                        container_name,
                        err
                    ))
                    transfer_error = TransferProjectError(
                        object_error=container_name,
                        transfer_project_id=transfer_object.id,
                        created=datetime.now()
                    )
                    transfer_error.save()

        app.logger.info('send_container.....')
        time.sleep(0.1)
        result[index] = 'ok'


    def send_object(self, app, containers, bucket, transfer, transfer_object, result, index):
        ctx = app.app_context()
        ctx.push()

        prefix = None
        marker = None

        for container in containers:
            app.logger.info('[{}] ----------'.format(transfer_object.project_name))
            app.logger.info('[{}] Container: {}'.format(
                transfer_object.project_name,
                container.get('name')
            ))

            try:
                meta, objects = self.swift.get_container(container.get('name'))
            except requests.exceptions.ConnectionError:
                try:
                    self.conn = self.keystone.get_keystone_connection()
                    self.swift = Swift(self.conn, project_id)
                    meta, objects = self.swift.get_container(container.get('name'))
                except AuthorizationFailure:
                    app.logger.error("[{}] 500 Get container '{}': Keystone authorization failure".format(
                        transfer_object.project_name,
                        container.get('name')
                    ))
                    continue
                except Exception as err:
                    app.logger.error("[{}] 500 Get container '{}': {}".format(
                        transfer_object.project_name,
                        container.get('name'),
                        err
                    ))
                    continue
            except Exception as err:
                app.logger.error("[{}] 500 Get container '{}': {}".format(
                    transfer_object.project_name,
                    container.get('name'),
                    err
                ))
                continue

            if len(objects) > 0:
                self._get_container(
                    container.get('name'),
                    bucket,
                    transfer,
                    transfer_object,
                    objects
                )

        app.logger.info("[{}] Finished send_object '{}'".format(
            transfer_object.project_name,
            index
        ))

        db.session.begin()
        transfer_project = db.session.query(TransferProject).filter_by(project_id=transfer_object.project_id).first()
        transfer_project.last_object = transfer.last_object
        transfer_project.count_error = transfer.count_error
        transfer_project.object_count_gcp = transfer.object_count_gcp
        transfer_project.bytes_used_gcp = transfer.bytes_used_gcp
        time.sleep(0.1)
        db.session.commit()

        labels = bucket.labels
        labels['object-count'] = transfer.object_count_gcp
        labels['bytes-used'] = transfer.bytes_used_gcp
        bucket.labels = labels
        time.sleep(0.1)
        bucket.patch()

        del transfer_project
        gc.collect()
        time.sleep(0.5)
        result[index] = 'ok'


    def _get_container(self, container, bucket, transfer, transfer_object, objects):
        ctx = self.app.app_context()
        ctx.push()

        for obj in objects:
            if obj.get('subdir'):
                prefix = obj.get('subdir')

                if not prefix:
                    self.app.logger.error("[{}] 500 PUT folder '{}/None': Prefix None".format(
                        transfer_object.project_name,
                        container
                    ))
                    continue

                blob = bucket.blob('{}/{}'.format(container, prefix))

                try:
                    blob.upload_from_string('',
                        content_type='application/directory',
                        num_retries=3,
                        timeout=30
                    )
                    # self.app.logger.info("[{}] 201 PUT folder '{}/{}': Created".format(
                    #     transfer_object.project_name,
                    #     container,
                    #     prefix
                    # ))
                except BadRequest:
                    transfer.count_error += 1
                    self.app.logger.error("[{}] 400 PUT folder '{}/{}': BadRequest".format(
                        transfer_object.project_name,
                        container,
                        prefix
                    ))
                    transfer_error = TransferProjectError(
                        object_error="{}/{}".format(container, prefix),
                        transfer_project_id=transfer_object.id,
                        created=datetime.now()
                    )
                    transfer_error.save()
                    continue
                except requests.exceptions.ReadTimeout:
                    transfer.count_error += 1
                    self.app.logger.error("[{}] 504 PUT folder '{}/{}': ReadTimeout".format(
                        transfer_object.project_name,
                        container,
                        prefix
                    ))
                    transfer_error = TransferProjectError(
                        object_error="{}/{}".format(container, prefix),
                        transfer_project_id=transfer_object.id,
                        created=datetime.now()
                    )
                    transfer_error.save()
                    continue
                except Exception as err:
                    transfer.count_error += 1
                    self.app.logger.error("[{}] 500 PUT folder '{}/{}': {}".format(
                        transfer_object.project_name,
                        container,
                        prefix,
                        err
                    ))
                    transfer_error = TransferProjectError(
                        object_error="{}/{}".format(container, prefix),
                        transfer_project_id=transfer_object.id,
                        created=datetime.now()
                    )
                    transfer_error.save()
                    continue

                transfer.last_object = '{}/{}'.format(container, prefix)
                self.flush_object -= 1
                del blob
                gc.collect()

                if self.flush_object <= 0:
                    self.app.logger.info('flush_object.......')
                    db.session.begin()
                    transfer_project = db.session.query(TransferProject).filter_by(project_id=transfer_object.project_id).first()
                    transfer_project.last_object = transfer.last_object
                    transfer_project.count_error = transfer.count_error
                    transfer_project.object_count_gcp = transfer.object_count_gcp
                    transfer_project.bytes_used_gcp = transfer.bytes_used_gcp
                    time.sleep(0.1)
                    db.session.commit()

                    labels = bucket.labels
                    labels['object-count'] = transfer.object_count_gcp
                    labels['bytes-used'] = transfer.bytes_used_gcp
                    bucket.labels = labels
                    bucket.patch()

                    del transfer_project
                    gc.collect()
                    self.flush_object = self.FLUSH_OBJECT

                try:
                    meta, objects = self.swift.get_container(container, prefix=prefix)
                except requests.exceptions.ConnectionError as err:
                    try:
                        self.conn = self.keystone.get_keystone_connection()
                        self.swift = Swift(self.conn, self.project_id)
                        meta, objects = self.swift.get_container(container, prefix=prefix)
                    except AuthorizationFailure:
                        self.app.logger.error("[{}] 500 Get container '{}': Keystone authorization failure".format(
                            transfer_object.project_name,
                            container
                        ))
                    except Exception as err:
                        self.app.logger.error("[{}] 500 Get container '{}/{}': {}".format(
                            transfer_object.project_name,
                            container,
                            prefix,
                            err
                        ))
                        continue
                except Exception as err:
                    self.app.logger.error("[{}] 500 Get container '{}/{}': {}".format(
                        transfer_object.project_name,
                        container,
                        prefix,
                        err
                    ))
                    continue

                if len(objects) > 0:
                    self._get_container(
                        container,
                        bucket,
                        transfer,
                        transfer_object,
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
                        except IncompleteRead:
                            transfer.count_error += 1
                            self.app.logger.error("[{}] 500 Get object '{}/{}': Keystone authorization failure".format(
                                transfer_object.project_name,
                                container,
                                obj.get('name')
                            ))
                            transfer_error = TransferProjectError(
                                object_error="{}/{}".format(container, obj.get('name')),
                                transfer_project_id=transfer_object.id,
                                created=datetime.now()
                            )
                            transfer_error.save()
                            continue
                        except AuthorizationFailure:
                            transfer.count_error += 1
                            self.app.logger.error("[{}] 500 Get object '{}/{}': Keystone authorization failure".format(
                                transfer_object.project_name,
                                container,
                                obj.get('name')
                            ))
                            transfer_error = TransferProjectError(
                                object_error="{}/{}".format(container, obj.get('name')),
                                transfer_project_id=transfer_object.id,
                                created=datetime.now()
                            )
                            transfer_error.save()
                            continue
                        except Exception as err:
                            transfer.count_error += 1
                            self.app.logger.error("[{}] 500 Get object '{}/{}': {}".format(
                                transfer_object.project_name,
                                container,
                                obj.get('name'),
                                err
                            ))
                            transfer_error = TransferProjectError(
                                object_error="{}/{}".format(container, obj.get('name')),
                                transfer_project_id=transfer_object.id,
                                created=datetime.now()
                            )
                            transfer_error.save()
                            continue
                    except IncompleteRead:
                        transfer.count_error += 1
                        self.app.logger.error("[{}] 500 Get object '{}/{}': {}".format(
                            transfer_object.project_name,
                            container,
                            obj.get('name'),
                            err
                        ))
                        transfer_error = TransferProjectError(
                            object_error="{}/{}".format(container, obj.get('name')),
                            transfer_project_id=transfer_object.id,
                            created=datetime.now()
                        )
                        transfer_error.save()
                        continue
                    except Exception as err:
                        transfer.count_error += 1
                        self.app.logger.error("[{}] 500 Get object '{}/{}': {}".format(
                            transfer_object.project_name,
                            container,
                            obj.get('name'),
                            err
                        ))
                        transfer_error = TransferProjectError(
                            object_error="{}/{}".format(container, obj.get('name')),
                            transfer_project_id=transfer_object.id,
                            created=datetime.now()
                        )
                        transfer_error.save()
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

                    try:
                        blob.upload_from_string(
                            content,
                            content_type=obj.get('content_type'),
                            num_retries=3,
                            timeout=900
                        )
                        # self.app.logger.info("[{}] 201 PUT object '{}' {} {}: Created".format(
                        #     transfer_object.project_name,
                        #     obj_path,
                        #     obj.get('content_type'),
                        #     len(content)
                        # ))
                    except BadRequest:
                        transfer.count_error += 1
                        self.app.logger.error("[{}] 400 PUT object '{}' {}: BadRequest".format(
                            transfer_object.project_name,
                            obj_path
                        ))
                        transfer_error = TransferProjectError(
                            object_error=obj_path,
                            transfer_project_id=transfer_object.id,
                            created=datetime.now()
                        )
                        transfer_error.save()
                    except requests.exceptions.ReadTimeout:
                        transfer.count_error += 1
                        self.app.logger.error("[{}] 504 PUT object '{}' {}: ReadTimeout".format(
                            transfer_object.project_name,
                            obj_path
                        ))
                        transfer_error = TransferProjectError(
                            object_error=obj_path,
                            transfer_project_id=transfer_object.id,
                            created=datetime.now()
                        )
                        transfer_error.save()
                    except Exception as err:
                        transfer.count_error += 1
                        self.app.logger.error("[{}] 500 PUT object '{}': {}".format(
                            transfer_object.project_name,
                            obj_path,
                            err
                        ))
                        transfer_error = TransferProjectError(
                            object_error=obj_path,
                            transfer_project_id=transfer_object.id,
                            created=datetime.now()
                        )
                        transfer_error.save()

                    del content
                    del blob
                    gc.collect()

                    transfer.object_count_gcp += 1
                    transfer.bytes_used_gcp += obj.get('bytes')
                    transfer.last_object = '{}/{}'.format(container, obj.get('name'))
                    self.flush_object -= 1

                    if self.flush_object <= 0:
                        self.app.logger.info('flush_object.......')
                        db.session.begin()
                        transfer_project = db.session.query(TransferProject).filter_by(project_id=transfer_object.project_id).first()
                        transfer_project.last_object = transfer.last_object
                        transfer_project.count_error = transfer.count_error
                        transfer_project.object_count_gcp = transfer.object_count_gcp
                        transfer_project.bytes_used_gcp = transfer.bytes_used_gcp
                        time.sleep(0.1)
                        db.session.commit()

                        labels = bucket.labels
                        labels['object-count'] = transfer.object_count_gcp
                        labels['bytes-used'] = transfer.bytes_used_gcp
                        bucket.labels = labels
                        time.sleep(2)
                        bucket.patch()

                        del transfer_project
                        gc.collect()
                        self.flush_object = self.FLUSH_OBJECT
