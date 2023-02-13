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
from swift_cloud_tools.models import TransferProject, TransferContainer, TransferContainerError, db

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


class SynchronizeContainers():

    def __init__(self, project_id, container_name, hostname):
        self.project_id = project_id
        self.project_name = None
        self.container_name = container_name
        self.hostname = hostname

        while True:
            try:
                self.transfer_object = TransferProject.find_transfer_project(self.project_id)
                break
            except Exception as err:
                time.sleep(5)

        if self.transfer_object:
            self.project_name = self.transfer_object.project_name

        self.keystone = Keystone()
        self.conn = self.keystone.get_keystone_connection()
        self.swift = Swift(self.conn, self.project_id)

    def synchronize(self, project_id, container_name, hostname):
        """Get project in swift."""

        self.project_id = project_id
        self.container_name = container_name
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

        while True:
            try:
                transfer_object = TransferProject.find_transfer_project(self.project_id)
                break
            except Exception as err:
                self.app.logger.error("[synchronize] 500 Query 'mysql': {}".format(err))
                time.sleep(5)

        error = False

        try:
            account_stat, containers = self.swift.get_account()
        except requests.exceptions.ConnectionError:
            try:
                self.conn = self.keystone.get_keystone_connection()
                self.swift = Swift(self.conn, self.project_id)
                account_stat, containers = self.swift.get_account()
            except AuthorizationFailure:
                self.app.logger.error("[{}] 500 Get account '{}': Keystone authorization failure".format(
                    transfer_object.project_name,
                    self.project_id
                ))
                error = True
            except Exception as err:
                self.app.logger.error("[{}] 500 Get account '{}': {}".format(
                    transfer_object.project_name,
                    self.project_id,
                    err
                ))
                error = True
        except json.decoder.JSONDecodeError:
            try:
                account_stat, containers = self.swift.get_account()
            except Exception as e:
                self.app.logger.error("[{}] 500 Get account '{}': {}".format(
                    transfer_object.project_name,
                    self.project_id,
                    err
                ))
                error = True
        except Exception as err:
            self.app.logger.error("[{}] 500 GET account 'AUTH_{}': {}".format(
                transfer_object.project_name,
                self.project_id,
                err
            ))
            error = True

        if not error:
            count = 0
            while True:
                try:
                    if count == 0:
                        db.session.begin()
                    transfer_project = db.session.query(TransferProject).filter_by(project_id=transfer_object.project_id).first()
                    transfer_project.container_count_swift = int(account_stat.get('x-account-container-count', 0))
                    transfer_project.object_count_swift = int(account_stat.get('x-account-object-count', 0))
                    transfer_project.bytes_used_swift = int(account_stat.get('x-account-bytes-used', 0))
                    time.sleep(0.1)
                    db.session.commit()
                    break
                except Exception as err:
                    self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                        err
                    ))
                    time.sleep(5)
                    count += 1

        storage_client = google.get_storage_client()
        account = 'auth_{}'.format(self.project_id)
        time.sleep(int(uniform(5, 10)))

        try:
            bucket = storage_client.get_bucket(
                account,
                timeout=30
            )
        except NotFound:
            try:
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
                deadline = Retry(deadline=60)
                bucket.patch(timeout=10, retry=deadline)
            except Conflict:
                pass
        except Exception as err:
            self.app.logger.error('[{}] 500 GET Create bucket: {}'.format(
                transfer_object.project_name,
                err
            ))
            return Response(err, mimetype="text/plain", status=500)

        self.app.logger.info('========================================================')
        self.app.logger.info("[{}] SET account_meta_cloud 'AUTH_{}': {}".format(
            transfer_object.project_name,
            self.project_id,
            self.container_name
        ))

        ########################################
        #              Container               #
        ########################################

        self.app.logger.info('[{}] ---------------------'.format(transfer_object.project_name))
        self.app.logger.info('[{}] Create container: {}'.format(transfer_object.project_name, self.container_name))

        error = False
        account = 'auth_{}'.format(self.project_id)

        try:
            meta, objects = self.swift.get_container(self.container_name)
        except requests.exceptions.ConnectionError:
            try:
                self.conn = self.keystone.get_keystone_connection()
                self.swift = Swift(self.conn, self.project_id)
                meta, objects = self.swift.get_container(self.container_name)
            except AuthorizationFailure:
                self.app.logger.error("[{}] 500 Get container '{}': Keystone authorization failure".format(
                    self.project_name,
                    self.container_name
                ))
                error = True
            except Exception as err:
                self.app.logger.error("[{}] 500 Get container '{}': {}".format(
                    self.project_name,
                    self.container_name,
                    err
                ))
                error = True
        except json.decoder.JSONDecodeError:
            try:
                meta, objects = self.swift.get_container(self.container_name)
            except Exception as e:
                self.app.logger.error("[{}] 500 Get container '{}': {}".format(
                    self.project_name,
                    self.container_name,
                    err
                ))
                error = True
        except Exception as err:
            self.app.logger.error("[{}] 500 Get container '{}': {}".format(
                self.project_name,
                self.container_name,
                err
            ))
            error = True

        if not error:
            count = 0
            while True:
                try:
                    if count == 0:
                        db.session.begin()
                    transfer_container = db.session.query(TransferContainer).filter_by(
                        project_id=transfer_object.project_id,
                        container_name=self.container_name
                    ).first()
                    transfer_container.container_count_swift = 1
                    transfer_container.object_count_swift = meta.get('x-container-object-count', 0)
                    transfer_container.bytes_used_swift = meta.get('x-container-bytes-used', 0)
                    time.sleep(0.1)
                    db.session.commit()
                    break
                except Exception as err:
                    self.app.logger.error("[synchronize] 500 Save 'mysql' transfer_container: {}".format(
                        err
                    ))
                    time.sleep(5)
                    count += 1

        if not error:
            blob = bucket.blob(self.container_name + '/')
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
                self.app.logger.info("[{}] 201 PUT container '{}': Created".format(
                    self.project_name,
                    self.container_name
                ))
            except BadRequest:
                transfer.count_error += 1
                self.app.logger.error("[{}] 400 PUT container '{}': BadRequest".format(
                    self.project_name,
                    self.container_name
                ))
                while True:
                    try:
                        transfer_error = TransferContainerError(
                            object_error=self.container_name,
                            transfer_container_id=transfer_container.id,
                            created=datetime.now()
                        )
                        transfer_error.save()
                        break
                    except Exception as err:
                        self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                            err
                        ))
                        time.sleep(5)
            except requests.exceptions.ReadTimeout:
                transfer.count_error += 1
                self.app.logger.error("[{}] 504 PUT container '{}': ReadTimeout".format(
                    self.project_name,
                    self.container_name
                ))
                while True:
                    try:
                        transfer_error = TransferContainerError(
                            object_error=self.container_name,
                            transfer_container_id=transfer_container.id,
                            created=datetime.now()
                        )
                        transfer_error.save()
                        break
                    except Exception as err:
                        self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                            err
                        ))
                        time.sleep(5)
            except Exception as err:
                transfer.count_error += 1
                self.app.logger.error("[{}] 500 PUT container '{}': {}".format(
                    self.project_name,
                    self.container_name,
                    err
                ))
                while True:
                    try:
                        transfer_error = TransferContainerError(
                            object_error=self.container_name,
                            transfer_container_id=transfer_container.id,
                            created=datetime.now()
                        )
                        transfer_error.save()
                        break
                    except Exception as err:
                        self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                            err
                        ))
                        time.sleep(5)

        self.app.logger.info('send_container.....')

        ########################################
        #               Objects                #
        ########################################

        self.app.logger.info('[{}] ----------'.format(transfer_object.project_name))
        self.app.logger.info('[{}] Container: {}'.format(
            transfer_object.project_name,
            self.container_name
        ))

        try:
            meta, objects = self.swift.get_container(self.container_name)
        except requests.exceptions.ConnectionError:
            try:
                self.conn = self.keystone.get_keystone_connection()
                self.swift = Swift(self.conn, self.project_id)
                meta, objects = self.swift.get_container(self.container_name)
            except AuthorizationFailure:
                self.app.logger.error("[{}] 500 Get container '{}': Keystone authorization failure".format(
                    transfer_object.project_name,
                    self.container_name
                ))
            except Exception as err:
                self.app.logger.error("[{}] 500 Get container '{}': {}".format(
                    transfer_object.project_name,
                    self.container_name,
                    err
                ))
        except Exception as err:
            self.app.logger.error("[{}] 500 Get container '{}': {}".format(
                transfer_object.project_name,
                self.container_name,
                err
            ))

        if len(objects) > 0:
            self._get_container(
                storage_client,
                account,
                transfer,
                transfer_object,
                transfer_container,
                objects
            )

        self.app.logger.info("[{}] Finished send_object '{}'".format(
            transfer_object.project_name,
            self.container_name
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
                transfer_container = db.session.query(TransferContainer).filter_by(
                    project_id=transfer_object.project_id,
                    container_name=self.container_name
                ).first()
                transfer_container.last_object = transfer.last_object
                transfer_container.count_error = TransferContainer.count_error + transfer.count_error
                transfer_container.container_count_gcp = 1
                transfer_container.object_count_gcp = TransferContainer.object_count_gcp + transfer.object_count_gcp
                transfer_container.bytes_used_gcp = TransferContainer.bytes_used_gcp + transfer.bytes_used_gcp
                time.sleep(0.1)
                db.session.commit()
                break
            except Exception as err:
                self.app.logger.error("[synchronize] 500 Save 'mysql' transfer_container: {}".format(
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

        # status, msg = self.swift.set_account_meta_cloud_migration()

        # self.app.logger.info('========================================================')
        # self.app.logger.info("[{}] {} SET account_meta_cloud_migration 'AUTH_{}': {}".format(
        #     transfer_object.project_name,
        #     status,
        #     self.project_id,
        #     msg
        # ))

        # if status != 204:
        #     return Response(msg, mimetype="text/plain", status=status)


    def _get_container(self, storage_client, account, transfer, transfer_object, transfer_container, objects):
        ctx = self.app.app_context()
        ctx.push()

        bucket = storage_client.get_bucket(
            account,
            timeout=30
        )

        for obj in objects:
            if obj.get('subdir'):
                prefix = obj.get('subdir')

                if not prefix:
                    self.app.logger.error("[{}] 500 PUT folder '{}/None': Prefix None".format(
                        transfer_object.project_name,
                        self.container_name
                    ))
                    continue

                blob = bucket.blob('{}/{}'.format(self.container_name, prefix))

                try:
                    blob.upload_from_string('',
                        content_type='application/directory',
                        num_retries=3,
                        timeout=30
                    )
                    self.app.logger.info("[{}] 201 PUT folder '{}/{}': Created".format(
                        transfer_object.project_name,
                        self.container_name,
                        prefix
                    ))
                except BadRequest:
                    transfer.count_error += 1
                    self.app.logger.error("[{}] 400 PUT folder '{}/{}': BadRequest".format(
                        transfer_object.project_name,
                        self.container_name,
                        prefix
                    ))
                    while True:
                        try:
                            transfer_error = TransferContainerError(
                                object_error="{}/{}".format(self.container_name, prefix),
                                transfer_container_id=transfer_container.id,
                                created=datetime.now()
                            )
                            transfer_error.save()
                            break
                        except Exception as err:
                            self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                err
                            ))
                            time.sleep(5)
                    continue
                except requests.exceptions.ReadTimeout:
                    transfer.count_error += 1
                    self.app.logger.error("[{}] 504 PUT folder '{}/{}': ReadTimeout".format(
                        transfer_object.project_name,
                        self.container_name,
                        prefix
                    ))
                    while True:
                        try:
                            transfer_error = TransferContainerError(
                                object_error="{}/{}".format(self.container_name, prefix),
                                transfer_container_id=transfer_container.id,
                                created=datetime.now()
                            )
                            transfer_error.save()
                            break
                        except Exception as err:
                            self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                err
                            ))
                            time.sleep(5)
                    continue
                except Exception as err:
                    transfer.count_error += 1
                    self.app.logger.error("[{}] 500 PUT folder '{}/{}': {}".format(
                        transfer_object.project_name,
                        self.container_name,
                        prefix,
                        err
                    ))
                    while True:
                        try:
                            transfer_error = TransferContainerError(
                                object_error="{}/{}".format(self.container_name, prefix),
                                transfer_container_id=transfer_container.id,
                                created=datetime.now()
                            )
                            transfer_error.save()
                            break
                        except Exception as err:
                            self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                err
                            ))
                            time.sleep(5)
                    continue

                transfer.last_object = '{}/{}'.format(self.container_name, prefix)
                del blob

                try:
                    meta, objects = self.swift.get_container(self.container_name, prefix=prefix)
                except requests.exceptions.ConnectionError as err:
                    try:
                        self.conn = self.keystone.get_keystone_connection()
                        self.swift = Swift(self.conn, self.project_id)
                        meta, objects = self.swift.get_container(self.container_name, prefix=prefix)
                    except AuthorizationFailure:
                        self.app.logger.error("[{}] 500 Get container '{}': Keystone authorization failure".format(
                            transfer_object.project_name,
                            self.container_name
                        ))
                    except Exception as err:
                        self.app.logger.error("[{}] 500 Get container '{}/{}': {}".format(
                            transfer_object.project_name,
                            self.container_name,
                            prefix,
                            err
                        ))
                        continue
                except Exception as err:
                    self.app.logger.error("[{}] 500 Get container '{}/{}': {}".format(
                        transfer_object.project_name,
                        self.container_name,
                        prefix,
                        err
                    ))
                    continue

                if len(objects) > 0:
                    self._get_container(
                        storage_client,
                        account,
                        transfer,
                        transfer_object,
                        transfer_container,
                        objects
                    )
            else:
                if obj.get('content_type') != 'application/directory':
                    try:
                        headers, content = self.swift.get_object(self.container_name, obj.get('name'))
                    except requests.exceptions.ConnectionError:
                        try:
                            self.conn = self.keystone.get_keystone_connection()
                            self.swift = Swift(self.conn, self.project_id)
                            headers, content = self.swift.get_object(self.container_name, obj.get('name'))
                        except IncompleteRead:
                            transfer.count_error += 1
                            self.app.logger.error("[{}] 500 Get object '{}/{}': Keystone authorization failure".format(
                                transfer_object.project_name,
                                self.container_name,
                                obj.get('name')
                            ))
                            while True:
                                try:
                                    transfer_error = TransferContainerError(
                                        object_error="{}/{}".format(self.container_name, obj.get('name')),
                                        transfer_container_id=transfer_container.id,
                                        created=datetime.now()
                                    )
                                    transfer_error.save()
                                    break
                                except Exception as err:
                                    self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                        err
                                    ))
                                    time.sleep(5)
                            continue
                        except AuthorizationFailure:
                            transfer.count_error += 1
                            self.app.logger.error("[{}] 500 Get object '{}/{}': Keystone authorization failure".format(
                                transfer_object.project_name,
                                self.container_name,
                                obj.get('name')
                            ))
                            while True:
                                try:
                                    transfer_error = TransferContainerError(
                                        object_error="{}/{}".format(self.container_name, obj.get('name')),
                                        transfer_container_id=transfer_container.id,
                                        created=datetime.now()
                                    )
                                    transfer_error.save()
                                    break
                                except Exception as err:
                                    self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                        err
                                    ))
                                    time.sleep(5)
                            continue
                        except Exception as err:
                            transfer.count_error += 1
                            self.app.logger.error("[{}] 500 Get object '{}/{}': {}".format(
                                transfer_object.project_name,
                                self.container_name,
                                obj.get('name'),
                                err
                            ))
                            while True:
                                try:
                                    transfer_error = TransferContainerError(
                                        object_error="{}/{}".format(self.container_name, obj.get('name')),
                                        transfer_container_id=transfer_container.id,
                                        created=datetime.now()
                                    )
                                    transfer_error.save()
                                    break
                                except Exception as err:
                                    self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                        err
                                    ))
                                    time.sleep(5)
                            continue
                    except IncompleteRead:
                        transfer.count_error += 1
                        self.app.logger.error("[{}] 500 Get object '{}/{}': {}".format(
                            transfer_object.project_name,
                            self.container_name,
                            obj.get('name'),
                            err
                        ))
                        while True:
                            try:
                                transfer_error = TransferContainerError(
                                    object_error="{}/{}".format(self.container_name, obj.get('name')),
                                    transfer_container_id=transfer_container.id,
                                    created=datetime.now()
                                )
                                transfer_error.save()
                                break
                            except Exception as err:
                                self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                    err
                                ))
                                time.sleep(5)
                        continue
                    except Exception as err:
                        transfer.count_error += 1
                        self.app.logger.error("[{}] 500 Get object '{}/{}': {}".format(
                            transfer_object.project_name,
                            self.container_name,
                            obj.get('name'),
                            err
                        ))
                        while True:
                            try:
                                transfer_error = TransferContainerError(
                                    object_error="{}/{}".format(self.container_name, obj.get('name')),
                                    transfer_container_id=transfer_container.id,
                                    created=datetime.now()
                                )
                                transfer_error.save()
                                break
                            except Exception as err:
                                self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                    err
                                ))
                                time.sleep(5)
                        continue

                    try:
                        obj_path = "{}/{}".format(self.container_name, obj.get('name'))
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
                        self.app.logger.info("[{}] 201 PUT object '{}' {} {}: Created".format(
                            transfer_object.project_name,
                            obj_path,
                            obj.get('content_type'),
                            len(content)
                        ))
                    except BadRequest:
                        transfer.count_error += 1
                        self.app.logger.error("[{}] 400 PUT object '{}' {}: BadRequest".format(
                            transfer_object.project_name,
                            obj_path
                        ))
                        while True:
                            try:
                                transfer_error = TransferContainerError(
                                    object_error=obj_path,
                                    transfer_container_id=transfer_container.id,
                                    created=datetime.now()
                                )
                                transfer_error.save()
                                break
                            except Exception as err:
                                self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                    err
                                ))
                                time.sleep(5)
                        continue
                    except requests.exceptions.ReadTimeout:
                        transfer.count_error += 1
                        self.app.logger.error("[{}] 504 PUT object '{}' {}: ReadTimeout".format(
                            transfer_object.project_name,
                            obj_path
                        ))
                        while True:
                            try:
                                transfer_error = TransferContainerError(
                                    object_error=obj_path,
                                    transfer_container_id=transfer_container.id,
                                    created=datetime.now()
                                )
                                transfer_error.save()
                                break
                            except Exception as err:
                                self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                    err
                                ))
                                time.sleep(5)
                        continue
                    except Exception as err:
                        transfer.count_error += 1
                        self.app.logger.error("[{}] 500 PUT object '{}': {}".format(
                            transfer_object.project_name,
                            obj_path,
                            err
                        ))
                        while True:
                            try:
                                transfer_error = TransferContainerError(
                                    object_error=obj_path,
                                    transfer_container_id=transfer_container.id,
                                    created=datetime.now()
                                )
                                transfer_error.save()
                                break
                            except Exception as err:
                                self.app.logger.error("[synchronize] 500 Save 'mysql': {}".format(
                                    err
                                ))
                                time.sleep(5)
                        continue

                    del content
                    del blob

                    transfer.object_count_gcp += 1
                    transfer.bytes_used_gcp += obj.get('bytes')
                    transfer.last_object = '{}/{}'.format(self.container_name, obj.get('name'))
