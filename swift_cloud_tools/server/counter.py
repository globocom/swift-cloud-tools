#!/usr/bin/python3
import asyncio
import os

from google.cloud import pubsub_v1

from swift_cloud_tools import create_app

SUBSCRIPTION = 'updates'


async def work():
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")

    app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
    ctx = app.app_context()
    ctx.push()

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, SUBSCRIPTION)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(message.data)
        print(message.attributes.get('params'))
        message.ack()

    while True:
        app.logger.info('[SERVICE][COUNTER] Counter task started')

        future = subscriber.subscribe(subscription_path, callback)

        try:
            future.result()
        except Exception as err:
            app.logger.info("[SERVICE][COUNTER] future.result(): {}".format(err))
            # Close the subscriber if not using a context manager.
            # subscriber.close()

        app.logger.info('[SERVICE][COUNTER] Counter task completed')
        app.logger.info('[SERVICE][COUNTER] Sending passive monitoring to zabbix')
        zabbix.send()

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
