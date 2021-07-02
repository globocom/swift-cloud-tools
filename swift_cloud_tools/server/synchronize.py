# -*- coding: utf-8 -*-
import time
import os

from flask import Response
from swift_cloud_tools import create_app
from google.cloud.exceptions import NotFound
from swiftclient import client as swift_client

from swift_cloud_tools.server.utils import Keystone, Swift, Google

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

        self.app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
        ctx = self.app.app_context()
        ctx.push()

        keystone = Keystone()
        self.conn = keystone.get_keystone_connection()
        self.swift = Swift(self.conn, project_id)
        google = Google()

        self.swift.set_account_meta_cloud()
        time.sleep(5)
        account_stat, containers = self.swift.get_account()

        container_count = account_stat.get('x-account-container-count')
        object_count = account_stat.get('x-account-object-count')
        bytes_used = account_stat.get('x-account-bytes-used')

        self.app.logger.info('[API] =========================================')
        self.app.logger.info('[API] Account: {}'.format(project_id))
        self.app.logger.info('[API] container_count: {}'.format(container_count))
        self.app.logger.info('[API] object_count: {}'.format(object_count))
        self.app.logger.info('[API] bytes_used: {}'.format(bytes_used))

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
                self.app.logger.info('[API] 500 GET Create bucket: {}'.format(err))
                return Response(err, mimetype="text/plain", status=500)

        for container in containers:
            self.app.logger.info('[API] ----------')
            self.app.logger.info('[API] Container: {}'.format(container.get('name')))
            if container.get('bytes') > 0:
                meta, objects = self.swift.get_container(container.get('name'))

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
                    content_type='application/directory;charset=UTF-8'
                )

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
                meta, objects = self.swift.get_container(container, prefix)

                if len(objects) > 0:
                    self._get_container(
                        container,
                        bucket,
                        objects
                    )
            else:
                if obj.get('content_type') != 'application/directory':
                    headers, content = swift_client.get_object(
                        self.swift.storage_url,
                        self.conn.auth_token,
                        container,
                        obj.get('name'),
                        http_conn=self.swift.http_conn
                    )

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

                    blob.upload_from_string(content, content_type=obj.get('content_type'))
                    self.app.logger.info('[API] Object: {}'.format(obj.get('name')))
