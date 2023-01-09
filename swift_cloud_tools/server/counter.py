#!/usr/bin/python3
import asyncio
import time
import os

from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound, InvalidArgument

from swift_cloud_tools.server.synchronize_counter import SynchronizeCounters
from swift_cloud_tools.server.utils import Google
from swift_cloud_tools import create_app

SUBSCRIPTION = 'updates'
MAX_MESSAGES = 100

app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
ctx = app.app_context()
ctx.push()

google = Google()
credentials = google.get_client()


async def work():
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")

    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    subscription_path = subscriber.subscription_path(project_id, SUBSCRIPTION)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        sync = SynchronizeCounters()
        synchronize = sync.synchronize(message)
        synchronize = None

    while True:
        app.logger.info('[SERVICE][COUNTER] Counter task started')

        try:
            response = subscriber.pull(
                request={
                    "subscription": subscription_path,
                    "max_messages": MAX_MESSAGES,
                }
            )
        except NotFound:
            time.sleep(1)
            app.logger.error('[SERVICE][COUNTER] Subscription not exists')
            time.sleep(4)
            app.logger.info('[SERVICE][COUNTER] Counter task completed')
            continue

        # Pulling a Subscription Synchronously
        for msg in response.received_messages:
            sync = SynchronizeCounters()
            synchronize = sync.synchronize(msg.message)

            try:
                if synchronize:
                    subscriber.acknowledge(
                        request={
                            "subscription": subscription_path,
                            "ack_ids": [msg.ack_id],
                        }
                    )
                    app.logger.info('[SERVICE][COUNTER] ack')
                else:
                    ack_deadline_seconds = 0
                    subscriber.modify_ack_deadline(
                        request={
                            "subscription": subscription_path,
                            "ack_ids": [msg.ack_id],
                            "ack_deadline_seconds": ack_deadline_seconds,
                        }
                    )
                    app.logger.info('[SERVICE][COUNTER] nack')

                synchronize = None
            except InvalidArgument:
                synchronize = None
                continue

        app.logger.info('[SERVICE][COUNTER] Counter task completed')
        # app.logger.info('[SERVICE][COUNTER] Sending passive monitoring to zabbix')
        # zabbix.send()

        await asyncio.sleep(int(os.environ.get("COUNTER_TIME", '300')))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    try:
        asyncio.ensure_future(work())
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        app.logger.info('[SERVICE][COUNTER] Closing loop')
        loop.close()
