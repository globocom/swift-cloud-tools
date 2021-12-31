#!/usr/bin/python3

import asyncio
import os
import random

# from swift_cloud_tools.server.zbx_passive import Zabbix
from swift_cloud_tools import create_app
from swift_cloud_tools.server.utils import Health

INITIAL_DCCM_WEIGHT = 254
INITIAL_GCP_WEIGHT = 1

CRITICAL_DCCM_WEIGHT = 200
CRITICAL_GCP_WEIGHT = 55


def is_load_high(averages, critical):
    for avg in averages:
        load = avg.get('load')
        if float(load[0]) >= float(critical):
            return True

    return False


async def work():
    app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
    ctx = app.app_context()
    ctx.push()

    while True:
        app.logger.info('[SERVICE][HEALTH] Health task started')

        health = Health()
        averages = health.get_load_averages()
        app.logger.info(averages)

        critical = app.config.get('HEALTH_CRITICAL_LOAD')
        high_load = is_load_high(averages, critical)

        if high_load:
            dns_weight = health.set_dns_weight(CRITICAL_DCCM_WEIGHT,
                                               CRITICAL_GCP_WEIGHT)
            app.logger.info(dns_weight)
        else:
            current_dccm_weight, current_gcp_weight = health.get_dns_weight()
            if current_dccm_weight != INITIAL_DCCM_WEIGHT:
                dns_weight = health.set_dns_weight(INITIAL_DCCM_WEIGHT,
                                                   INITIAL_GCP_WEIGHT)
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
