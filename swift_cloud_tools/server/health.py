#!/usr/bin/python3
import asyncio
import os
import random

# from swift_cloud_tools.server.zbx_passive import Zabbix
from swift_cloud_tools import create_app
from swift_cloud_tools.server.utils import Health


async def work():
    app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
    ctx = app.app_context()
    ctx.push()

    while True:
        app.logger.info('[SERVICE][HEALTH] Health task started')

        health = Health()
        averages = health.get_load_averages()
        app.logger.info(averages)

        weightDccm = random.randint(1,10)
        weightGcp = random.randint(1,10)

        dns_weight = health.set_dns_weight(weightDccm, weightGcp)
        app.logger.info(dns_weight)

        app.logger.info('[SERVICE][HEALTH] Health task completed')
        # app.logger.info('[SERVICE][HEALTH] Sending passive monitoring to zabbix')
        # zabbix.send()

        await asyncio.sleep(int(os.environ.get("HEALTH_TIME", '600')))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    try:
        asyncio.ensure_future(work())
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        app.logger.info('[SERVICE][HEALTH] Closing loop')
        loop.close()
