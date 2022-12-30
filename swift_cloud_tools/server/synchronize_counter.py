#!/usr/bin/python3
import os
import json

from google.api_core.exceptions import NotFound
from google.api_core.retry import Retry

from swift_cloud_tools.server.utils import Google
from swift_cloud_tools import create_app


class SynchronizeCounters():

    def synchronize(self, message):
        """Get project in swift."""

        data = message.data.decode("utf-8")
        params = json.loads(message.attributes.get('params'))
        google = Google()
        storage_client = google.get_storage_client()

        app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
        ctx = app.app_context()
        ctx.push()

        account = 'auth_{}'.format(params.get('account'))

        try:
            bucket = storage_client.get_bucket(
                account,
                timeout=30
            )
        except NotFound:
            app.logger.error('[COUNTER] GET bucket: bucket not found')
        except Exception as err:
            app.logger.error('[COUNTER] GET bucket: {}'.format(err))

        labels = bucket.labels
        container_count = int(labels.get('container-count', 0))

        if params.get('kind') == 'account':
            if data == 'CREATE':
                labels['container-count'] = container_count + 1
            elif data == 'DELETE':
                labels['container-count'] = container_count - 1
        elif params.get('kind') == 'container':
            container = '{}/'.format(params.get('container'))
            blob = bucket.get_blob(container)
            metadata = blob.metadata
            object_count = int(metadata.get('object-count', 0))
            bytes_used = int(metadata.get('bytes-used', 0))

            if data == 'CREATE':
                labels['object-count'] = object_count + 1
                labels['bytes-used'] = bytes_used + params.get('bytes-used')

                metadata['object-count'] = object_count + 1
                metadata['bytes-used'] = bytes_used + params.get('bytes-used')

                blob.metadata = metadata
                deadline = Retry(deadline=60)
                blob.patch(timeout=10, retry=deadline)
            elif data == 'DELETE':
                labels['object-count'] = object_count - 1
                labels['bytes-used'] = bytes_used - params.get('bytes-used')

                metadata['object-count'] = object_count - 1
                metadata['bytes-used'] = bytes_used - params.get('bytes-used')

                blob.metadata = metadata
                deadline = Retry(deadline=60)
                blob.patch(timeout=10, retry=deadline)

        bucket.labels = labels
        deadline = Retry(deadline=60)
        bucket.patch(timeout=10, retry=deadline)

        message.ack()
