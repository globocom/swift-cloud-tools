#!/usr/bin/python3
import json
import os

from google.api_core.exceptions import NotFound, RetryError
from google.api_core.retry import Retry

from swift_cloud_tools.server.utils import Google
from swift_cloud_tools import create_app


class SynchronizeCounters():

    def synchronize(self, message):
        """Get project in swift."""

        app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
        ctx = app.app_context()
        ctx.push()

        data = message.data.decode("utf-8")
        params = json.loads(message.attributes.get('params'))
        google = Google()
        storage_client = google.get_storage_client()

        account = 'auth_{}'.format(params.get('account'))
        counter = params.get('counter', 1)

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
                labels['container-count'] = container_count + counter
            elif data == 'DELETE':
                labels['container-count'] = container_count - counter
            elif data == 'RESET':
                labels['container-count'] = 0
                labels['object-count'] = 0
                labels['bytes-used'] = 0
        elif params.get('kind') == 'container':
            container = '{}/'.format(params.get('container'))
            blob = bucket.get_blob(container)
            app.logger.info('[COUNTER] container: {}'.format(container))

            if blob and blob.exists():
                metadata = blob.metadata
                object_count = int(metadata.get('object-count', 0))
                bytes_used = int(metadata.get('bytes-used', 0))

                if data == 'CREATE':
                    labels['object-count'] = object_count + counter
                    labels['bytes-used'] = bytes_used + params.get('bytes-used')

                    metadata['object-count'] = object_count + counter
                    metadata['bytes-used'] = bytes_used + params.get('bytes-used')

                    blob.metadata = metadata
                    deadline = Retry(deadline=60)
                    blob.patch(timeout=10, retry=deadline)
                    app.logger.info('[COUNTER] metadata: {}'.format(metadata))
                elif data == 'DELETE':
                    labels['object-count'] = object_count - counter
                    labels['bytes-used'] = bytes_used - params.get('bytes-used')

                    metadata['object-count'] = object_count - counter
                    metadata['bytes-used'] = bytes_used - params.get('bytes-used')

                    blob.metadata = metadata
                    deadline = Retry(deadline=60)
                    blob.patch(timeout=10, retry=deadline)
                    app.logger.info('[COUNTER] metadata: {}'.format(metadata))

        bucket.labels = labels

        try:
            # ack
            deadline = Retry(deadline=60)
            bucket.patch(timeout=10, retry=deadline)
            app.logger.info('[COUNTER] labels: {}'.format(labels))
        except RetryError:
            # nack
            return False

        return True
