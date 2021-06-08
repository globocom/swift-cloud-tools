#!/usr/bin/python3
import asyncio
import json
import os

from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime

from swift_cloud_tools.models import ExpiredObject
from swift_cloud_tools.server import zbx_passive
from swift_cloud_tools import create_app


def _get_credentials():
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(os.environ.get("GCP_CREDENTIALS"))
    )

    return credentials.with_scopes(
        ['https://www.googleapis.com/auth/cloud-platform']
    )

def _get_client():
    credentials = _get_credentials()
    return storage.Client(credentials=credentials)

async def work():
    client = _get_client()
    app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
    ctx = app.app_context()
    ctx.push()

    while True:
        app.logger.info('[SERVICE] Expire task started')

        if not client:
            client = _get_client()

        current_time = datetime.now()
        raws = ExpiredObject.query.filter(ExpiredObject.date <= current_time).all()

        for raw in raws:
            bucket = client.get_bucket(raw.account)
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

        zbx_passive.send()
        app.logger.info('[SERVICE] Sending passive monitoring to zabbix')

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
