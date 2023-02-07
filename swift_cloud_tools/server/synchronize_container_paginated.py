# -*- coding: utf-8 -*-
import itertools
import requests
import time
import json
import os

from datetime import datetime
from random import uniform
from flask import Response
from threading import Thread
from swift_cloud_tools import create_app
from google.auth.exceptions import TransportError
from google.api_core.exceptions import BadRequest, NotFound, Conflict
from google.api_core.retry import Retry
from swiftclient import client as swift_client
from keystoneauth1.exceptions.auth import AuthorizationFailure
from http.client import IncompleteRead

from swift_cloud_tools.server.utils import Keystone, Swift, Google, Transfer
from swift_cloud_tools.models import TransferProject, TransferContainerPaginated, TransferContainerPaginatedError, db

BUCKET_LOCATION = os.environ.get('BUCKET_LOCATION', 'US-EAST1')
RESERVED_META = [
    'x-delete-at',
    'x-delete-after',
    'x-versions-location',
    'x-history-location',
    'x-undelete-enabled',
    'x-container-sysmeta-undelete-enabled',
    'content-encoding'
]


class SynchronizeContainersPaginated():

    def __init__(self, project_id, container_name, marker, hostname):
        self.project_id = project_id
        self.project_name = None
        self.container_name = container_name
        self.marker = marker
        self.hostname = hostname

        while True:
            try:
                self.transfer_object = TransferProject.find_transfer_project(project_id)
                break
            except Exception as err:
                time.sleep(5)

        if self.transfer_object:
            self.project_name = self.transfer_object.project_name

        self.keystone = Keystone()
        self.conn = self.keystone.get_keystone_connection()
        self.swift = Swift(self.conn, project_id)

    def synchronize(self, project_id, container_name, hostname, marker):
        """Get project in swift."""

        self.project_id = project_id
        self.container_name = container_name
        self.hostname = hostname
        self.marker = marker

        self.app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
        ctx = self.app.app_context()
        ctx.push()

        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        session.mount('https://', adapter)
        session.mount('http://', adapter)

        google = Google()
        transfer = Transfer()

        while True:
            try:
                transfer_object = TransferProject.find_transfer_project(project_id)
                break
            except Exception as err:
                self.app.logger.error("[synchronize] 500 Query 'mysql': {}".format(err))
                time.sleep(5)

        storage_client = google.get_storage_client()
        account = 'auth_{}'.format(project_id)
        time.sleep(int(uniform(5, 10)))

        try:
            bucket = storage_client.get_bucket(
                account,
                timeout=30
            )
        except Exception as err:
            self.app.logger.error('[{}] 500 GET Create bucket: {}'.format(
                transfer_object.project_name,
                err
            ))
            return Response(err, mimetype="text/plain", status=500)

        self.app.logger.info('========================================================')
        self.app.logger.info("[{}] SET account_meta_cloud 'AUTH_{}': {}".format(
            transfer_object.project_name,
            project_id,
            container_name
        ))

        ########################################
        #               Objects                #
        ########################################

        self.app.logger.info('[{}] ----------'.format(transfer_object.project_name))
        self.app.logger.info('[{}] Container: {}, Marker: {}'.format(
            transfer_object.project_name,
            container_name,
            marker
        ))

        try:
            meta, objects = self.swift.get_container(container_name, marker=marker, full_listing=False, delimiter=None)
        except requests.exceptions.ConnectionError:
            try:
                self.conn = self.keystone.get_keystone_connection()
                self.swift = Swift(self.conn, project_id)
                meta, objects = self.swift.get_container(container_name, marker=marker, full_listing=False, delimiter=None)
            except AuthorizationFailure:
                self.app.logger.error("[{}] 500 Get container '{}', Marker '{}': Keystone authorization failure".format(
                    transfer_object.project_name,
                    container_name,
                    marker
                ))
            except Exception as err:
                self.app.logger.error("[{}] 500 Get container '{}', Marker '{}': {}".format(
                    transfer_object.project_name,
                    container_name,
                    marker,
                    err
                ))
        except Exception as err:
            self.app.logger.error("[{}] 500 Get container '{}', Marker '{}': {}".format(
                transfer_object.project_name,
                container_name,
                marker,
                err
            ))

        if len(objects) > 0:
            page_size = 1000
            parts = []

            start = 0
            end = page_size

            while True:
                res = list(itertools.islice(objects, start, end))
                start += page_size
                end += page_size
                if not res:
                    break
                parts.append(res)

            threads = [None] * len(parts)

            for i in range(len(threads)):
                time.sleep(0.5)
                threads[i] = Thread(target=self._get_container, args=(
                    self.app,
                    storage_client,
                    account,
                    container_name,
                    transfer,
                    transfer_object,
                    parts[i]
                ))
                threads[i].start()
            for i in range(len(threads)):
                threads[i].join()

        self.app.logger.info("[{}] Finished send_object '{}', Marker '{}'".format(
            transfer_object.project_name,
            container_name,
            marker
        ))

        time.sleep(int(uniform(1, 20)) + int(uniform(5, 20)) + int(uniform(10, 20)))
        container_count_gcp = 0
        object_count_gcp = 0
        bytes_used_gcp = 0

        count = 0
        while True:
            try:
                if count == 0:
                    db.session.begin()
                transfer_project = db.session.query(TransferProject).filter_by(project_id=transfer_object.project_id).first()
                transfer_project.last_object = transfer.last_object
                transfer_project.count_error = TransferProject.count_error + transfer.count_error
                transfer_project.container_count_gcp = TransferProject.container_count_gcp + 1
                transfer_project.object_count_gcp = TransferProject.object_count_gcp + transfer.object_count_gcp
                transfer_project.bytes_used_gcp = TransferProject.bytes_used_gcp + transfer.bytes_used_gcp
                time.sleep(0.1)
                db.session.commit()
                container_count_gcp = transfer_project.container_count_gcp
                object_count_gcp = transfer_project.object_count_gcp
                bytes_used_gcp = transfer_project.bytes_used_gcp
                break
            except Exception as err:
                self.app.logger.error("[synchronize] 500 Save 'mysql' transfer_project: {}".format(
                    err
                ))
                time.sleep(5)
                count += 1

        count = 0
        while True:
            try:
                if count == 0:
                    db.session.begin()
                transfer_container_paginated = db.session.query(TransferContainerPaginated).filter_by(
                    project_id=transfer_object.project_id,
                    container_name=container_name
                ).first()
                transfer_container_paginated.count_error = TransferContainerPaginated.count_error + transfer.count_error
                transfer_container_paginated.container_count_gcp = 1
                transfer_container_paginated.object_count_gcp = TransferContainerPaginated.object_count_gcp + transfer.object_count_gcp
                transfer_container_paginated.bytes_used_gcp = TransferContainerPaginated.bytes_used_gcp + transfer.bytes_used_gcp
                time.sleep(0.1)
                db.session.commit()
                break
            except Exception as err:
                self.app.logger.error("[synchronize] 500 Save 'mysql' transfer_container_paginated: {}".format(
                    err
                ))
                time.sleep(5)
                count += 1

        while True:
            try:
                bucket_flush = storage_client.get_bucket(
                    account,
                    timeout=30
                )
                labels_flush = bucket_flush.labels
                labels_flush['container-count'] = container_count_gcp
                labels_flush['object-count'] = object_count_gcp
                labels_flush['bytes-used'] = bytes_used_gcp
                bucket_flush.labels = labels_flush
                time.sleep(0.1)
                deadline = Retry(deadline=60)
                bucket_flush.patch(timeout=10, retry=deadline)
                break
            except Exception as err:
                self.app.logger.error("[synchronize] 500 Save 'bucket': {}".format(
                    err
                ))
                time.sleep(5)


    def _get_container(self, app, storage_client, account, container, transfer, transfer_object, objects):
        ctx = app.app_context()
        ctx.push()

        bucket = storage_client.get_bucket(
            account,
            timeout=30
        )

        for obj in objects:
            if obj.get('content_type') == "application/directory" and len(obj.get('name', '')) > 0
                        and obj.get('name','')[-1] == "/":
                prefix = obj.get('name')

                blob = bucket.blob('{}/{}'.format(container, prefix))

                try:
                    blob.upload_from_string('',
                        content_type='application/directory',
                        num_retries=3,
                        timeout=30
                    )
                    # app.logger.info("[{}] 201 PUT folder '{}/{}': Created".format(
                    #     transfer_object.project_name,
                    #     container,
                    #     prefix
                    # ))
                except BadRequest:
                    transfer.count_error += 1
                    app.logger.error("[{}] 400 PUT folder '{}/{}': BadRequest".format(
                        transfer_object.project_name,
                        container,
                        prefix
                    ))
                    while True:
                        try:
                            transfer_error = TransferContainerPaginatedError(
                                object_error="{}/{}".format(container, prefix),
                                transfer_container_paginated_id=transfer_container_paginated.id,
                                created=datetime.now()
                            )
                            transfer_error.save()
                            break
                        except Exception as err:
                            app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                err
                            ))
                            time.sleep(5)
                    continue
                except requests.exceptions.ReadTimeout:
                    transfer.count_error += 1
                    app.logger.error("[{}] 504 PUT folder '{}/{}': ReadTimeout".format(
                        transfer_object.project_name,
                        container,
                        prefix
                    ))
                    while True:
                        try:
                            transfer_error = TransferContainerPaginatedError(
                                object_error="{}/{}".format(container, prefix),
                                transfer_container_paginated_id=transfer_container_paginated.id,
                                created=datetime.now()
                            )
                            transfer_error.save()
                            break
                        except Exception as err:
                            app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                err
                            ))
                            time.sleep(5)
                    continue
                except Exception as err:
                    transfer.count_error += 1
                    app.logger.error("[{}] 500 PUT folder '{}/{}': {}".format(
                        transfer_object.project_name,
                        container,
                        prefix,
                        err
                    ))
                    while True:
                        try:
                            transfer_error = TransferContainerPaginatedError(
                                object_error="{}/{}".format(container, prefix),
                                transfer_container_paginated_id=transfer_container_paginated.id,
                                created=datetime.now()
                            )
                            transfer_error.save()
                            break
                        except Exception as err:
                            app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                err
                            ))
                            time.sleep(5)
                    continue

                del blob
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
                            app.logger.error("[{}] 500 Get object '{}/{}': Keystone authorization failure".format(
                                transfer_object.project_name,
                                container,
                                obj.get('name')
                            ))
                            while True:
                                try:
                                    transfer_error = TransferContainerPaginatedError(
                                        object_error="{}/{}".format(container, obj.get('name')),
                                        transfer_container_paginated_id=transfer_container_paginated.id,
                                        created=datetime.now()
                                    )
                                    transfer_error.save()
                                    break
                                except Exception as err:
                                    app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                        err
                                    ))
                                    time.sleep(5)
                            continue
                        except AuthorizationFailure:
                            transfer.count_error += 1
                            app.logger.error("[{}] 500 Get object '{}/{}': Keystone authorization failure".format(
                                transfer_object.project_name,
                                container,
                                obj.get('name')
                            ))
                            while True:
                                try:
                                    transfer_error = TransferContainerPaginatedError(
                                        object_error="{}/{}".format(container, obj.get('name')),
                                        transfer_container_paginated_id=transfer_container_paginated.id,
                                        created=datetime.now()
                                    )
                                    transfer_error.save()
                                    break
                                except Exception as err:
                                    app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                        err
                                    ))
                                    time.sleep(5)
                            continue
                        except Exception as err:
                            transfer.count_error += 1
                            app.logger.error("[{}] 500 Get object '{}/{}': {}".format(
                                transfer_object.project_name,
                                container,
                                obj.get('name'),
                                err
                            ))
                            while True:
                                try:
                                    transfer_error = TransferContainerPaginatedError(
                                        object_error="{}/{}".format(container, obj.get('name')),
                                        transfer_container_paginated_id=transfer_container_paginated.id,
                                        created=datetime.now()
                                    )
                                    transfer_error.save()
                                    break
                                except Exception as err:
                                    app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                        err
                                    ))
                                    time.sleep(5)
                            continue
                    except IncompleteRead:
                        transfer.count_error += 1
                        app.logger.error("[{}] 500 Get object '{}/{}': {}".format(
                            transfer_object.project_name,
                            container,
                            obj.get('name'),
                            err
                        ))
                        while True:
                            try:
                                transfer_error = TransferContainerPaginatedError(
                                    object_error="{}/{}".format(container, obj.get('name')),
                                    transfer_container_paginated_id=transfer_container_paginated.id,
                                    created=datetime.now()
                                )
                                transfer_error.save()
                                break
                            except Exception as err:
                                app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                    err
                                ))
                                time.sleep(5)
                        continue
                    except Exception as err:
                        transfer.count_error += 1
                        app.logger.error("[{}] 500 Get object '{}/{}': {}".format(
                            transfer_object.project_name,
                            container,
                            obj.get('name'),
                            err
                        ))
                        while True:
                            try:
                                transfer_error = TransferContainerPaginatedError(
                                    object_error="{}/{}".format(container, obj.get('name')),
                                    transfer_container_paginated_id=transfer_container_paginated.id,
                                    created=datetime.now()
                                )
                                transfer_error.save()
                                break
                            except Exception as err:
                                app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                    err
                                ))
                                time.sleep(5)
                        continue

                    try:
                        obj_path = "{}/{}".format(container, obj.get('name'))
                        blob = bucket.blob(obj_path)
                        metadata = {}

                        if headers.get('cache-control'):
                            blob.cache_control = headers.get('cache-control')

                        if headers.get('content-encoding'):
                            metadata['content-encoding'] = headers.get('content-encoding')

                        if headers.get('content-disposition'):
                            blob.content_disposition = headers.get('content-disposition')

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

                        if len(metadata):
                            blob.metadata = metadata

                        blob.upload_from_string(
                            content,
                            content_type=obj.get('content_type'),
                            num_retries=3,
                            timeout=900
                        )
                        # app.logger.info("[{}] 201 PUT object '{}' {} {}: Created".format(
                        #     transfer_object.project_name,
                        #     obj_path,
                        #     obj.get('content_type'),
                        #     len(content)
                        # ))
                    except BadRequest:
                        transfer.count_error += 1
                        app.logger.error("[{}] 400 PUT object '{}' {}: BadRequest".format(
                            transfer_object.project_name,
                            obj_path
                        ))
                        while True:
                            try:
                                transfer_error = TransferContainerPaginatedError(
                                    object_error=obj_path,
                                    transfer_container_paginated_id=transfer_container_paginated.id,
                                    created=datetime.now()
                                )
                                transfer_error.save()
                                break
                            except Exception as err:
                                app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                    err
                                ))
                                time.sleep(5)
                        continue
                    except requests.exceptions.ReadTimeout:
                        transfer.count_error += 1
                        app.logger.error("[{}] 504 PUT object '{}' {}: ReadTimeout".format(
                            transfer_object.project_name,
                            obj_path
                        ))
                        while True:
                            try:
                                transfer_error = TransferContainerPaginatedError(
                                    object_error=obj_path,
                                    transfer_container_paginated_id=transfer_container_paginated.id,
                                    created=datetime.now()
                                )
                                transfer_error.save()
                                break
                            except Exception as err:
                                app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                    err
                                ))
                                time.sleep(5)
                        continue
                    except Exception as err:
                        transfer.count_error += 1
                        app.logger.error("[{}] 500 PUT object '{}': {}".format(
                            transfer_object.project_name,
                            obj_path,
                            err
                        ))
                        while True:
                            try:
                                transfer_error = TransferContainerPaginatedError(
                                    object_error=obj_path,
                                    transfer_container_paginated_id=transfer_container_paginated.id,
                                    created=datetime.now()
                                )
                                transfer_error.save()
                                break
                            except Exception as err:
                                app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                    err
                                ))
                                time.sleep(5)
                        continue

                    del content
                    del blob

                    transfer.object_count_gcp += 1
                    transfer.bytes_used_gcp += obj.get('bytes')
