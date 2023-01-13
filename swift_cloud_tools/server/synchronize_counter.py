#!/usr/bin/python3
import json
import os

from google.api_core.exceptions import NotFound, RetryError, Conflict
from google.api_core.retry import Retry

from swift_cloud_tools.server.utils import Google
from swift_cloud_tools import create_app


class SynchronizeCounters():

    def __init__(self):
        self.app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
        ctx = self.app.app_context()
        ctx.push()

    def synchronize(self, message):
        """Get project in swift."""

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
            self.app.logger.error('[COUNTER] GET bucket: bucket not found')
        except Exception as err:
            self.app.logger.error('[COUNTER] GET bucket: {}'.format(err))

        labels = bucket.labels
        container_count = int(labels.get('container-count', 0))
        container = '{}/'.format(params.get('container'))

        self.app.logger.info('[COUNTER] container: {}'.format(container))

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
            blob = bucket.get_blob(container)
            self.app.logger.info('[COUNTER] labels before: {}'.format(labels))

            if blob and blob.exists():
                metadata = blob.metadata
                object_count_meta = int(metadata.get('object-count', 0))
                bytes_used_meta = int(metadata.get('bytes-used', 0))
                object_count_label = int(labels.get('object-count', 0))
                bytes_used_label = int(labels.get('bytes-used', 0))

                self.app.logger.info('[COUNTER] metadata before: {}'.format(metadata))
                self.app.logger.info('[COUNTER] counter: {}'.format(counter))

                if data == 'CREATE':
                    labels['object-count'] = object_count_label + counter
                    labels['bytes-used'] = bytes_used_label + params.get('bytes-used')

                    metadata['object-count'] = object_count_meta + counter
                    metadata['bytes-used'] = bytes_used_meta + params.get('bytes-used')

                    blob.metadata = metadata
                    try:
                        deadline = Retry(deadline=60)
                        blob.patch(timeout=60, retry=deadline)
                    except Conflict:
                        pass
                    self.app.logger.info('[COUNTER] metadata after: {}'.format(metadata))
                elif data == 'DELETE':
                    labels['object-count'] = object_count_label - counter
                    labels['bytes-used'] = bytes_used_label - params.get('bytes-used')

                    metadata['object-count'] = object_count_meta - counter
                    metadata['bytes-used'] = bytes_used_meta - params.get('bytes-used')

                    blob.metadata = metadata
                    try:
                        deadline = Retry(deadline=60)
                        blob.patch(timeout=60, retry=deadline)
                    except Conflict:
                        pass
                    self.app.logger.info('[COUNTER] metadata after: {}'.format(metadata))

            del blob

        bucket.labels = labels

        try:
            # ack
            try:
                deadline = Retry(deadline=60)
                bucket.patch(timeout=60, retry=deadline)
            except Conflict:
                pass
            self.app.logger.info('[COUNTER] labels after: {}'.format(labels))
        except RetryError:
            # nack
            del bucket
            return False

        del bucket
        return True
