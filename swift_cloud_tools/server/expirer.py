#!/usr/bin/python3
import asyncio
import os

from datetime import datetime

from swift_cloud_tools.server.utils import Google
from swift_cloud_tools.models import ExpiredObject
from swift_cloud_tools.server import zbx_passive
from swift_cloud_tools import create_app


async def work():
    google = Google()
    gcp_client = google.get_gcp_client()
    app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
    ctx = app.app_context()
    ctx.push()

    while True:
        app.logger.info('[SERVICE] Expire task started')

        if not gcp_client:
            gcp_client = google.get_gcp_client()

        current_time = datetime.now()
        raws = ExpiredObject.query.filter(ExpiredObject.date <= current_time).all()

        for raw in raws:
            bucket = gcp_client.get_bucket(raw.account)
            obj_path = "{}/{}".format(raw.container, raw.obj)

            if not bucket or not bucket.exists():
                app.logger.info('[SERVICE] Bucket not exists: {}'.format(raw.account))
                app.logger.info('[SERVICE] Object removed from database only: {}/{}'.format(raw.account, obj_path))
                raw.delete()
                continue

            blob = bucket.get_blob(obj_path)

            if not blob or not blob.exists():
                app.logger.info('[SERVICE] Blob not exists: {}'.format(obj_path))
                app.logger.info('[SERVICE] Object removed from database only: {}/{}'.format(raw.account, obj_path))
                raw.delete()
                continue

            res = raw.delete()

            if res[1] == 200:
                app.logger.info('[SERVICE] Object removed: {}/{}'.format(raw.account, obj_path))
                blob.delete()

        app.logger.info('[SERVICE] Expire task completed')
        app.logger.info('[SERVICE] Sending passive monitoring to zabbix')
        zbx_passive.send()

        await asyncio.sleep(int(os.environ.get("EXPIRY_TIME", '3600')))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    try:
        asyncio.ensure_future(work())
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        app.logger.info('[SERVICE] Closing loop')
        loop.close()
