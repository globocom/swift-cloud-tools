#!/usr/bin/python3
import asyncio
import os

# from swift_cloud_tools.server.zbx_passive import Zabbix
from swift_cloud_tools import create_app
from swift_cloud_tools.server.utils import Health


async def work():
    app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
    ctx = app.app_context()
    ctx.push()

    while True:
        app.logger.info('[SERVICE][ACLUPDATE] Acl update task started')

        health = Health()
        health.acl_update()

        app.logger.info('[SERVICE][ACLUPDATE] Acl update task completed')
        # app.logger.info('[SERVICE][ACLUPDATE] Sending passive monitoring to zabbix')
        # zabbix.send()

        await asyncio.sleep(int(os.environ.get("ACL_UPDATE_TIME", '600')))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    try:
        asyncio.ensure_future(work())
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        app.logger.info('[SERVICE][ACLUPDATE] Closing loop')
        loop.close()
