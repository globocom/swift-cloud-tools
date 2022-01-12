#!/usr/bin/python3

import asyncio
import os
import logging
import json

# from swift_cloud_tools.server.zbx_passive import Zabbix
from swift_cloud_tools import create_app
from swift_cloud_tools.server.utils import Health

logger = logging.getLogger(__name__)


class WeightHandler:
    values = {
        "low": {
            "load_min": 0.00,
            "load_max": 1.00,
            "conn_min": 0,
            "conn_max": 5000,
            "dccm_weight": 255,
            "gcp_weight": 1
        },
        "medium": {
            "load_min": 1.01,
            "load_max": 2.00,
            "conn_min": 5001,
            "conn_max": 15000,
            "dccm_weight": 235,
            "gcp_weight": 20
        },
        "high": {
            "load_min": 2.01,
            "load_max": 9.99,
            "conn_min": 15001,
            "conn_max": 99999,
            "dccm_weight": 200,
            "gcp_weight": 55
        }
    }

    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.health = Health()
        dc_w, gcp_w = self.health.get_dns_weight()
        self.current_dccm_weight = dc_w
        self.current_gcp_weight = gcp_w
        self._log(f'Current DNS Weight: Globo {dc_w}, GCP {gcp_w}')

    def _log(self, msg, level="info"):
        if self.dry_run:
            msg = f'[SERVICE][HEALTH (DRY RUN)] {msg}'
        else:
            msg = f'[SERVICE][HEALTH] {msg}'

        if level == "warning":
            logger.warning(msg)

        if level == "error":
            logger.error(msg)

        logger.info(msg)

    def _update_weight_values(self):
        try:
            self.values = json.loads(os.environ.get("HEALTH_VALUES"))
        except json.JSONDecodeError:
            self._log("Invalid JSON in HEALTH_VALUES", "warning")
        except Exception as err:
            self._log(err, "error")

    @property
    def current_level(self):
        current = "low"

        for level in self.values.keys():
            items = self.values[level]

            if self.current_gcp_weight == items.get("gcp_weight"):
                current = level

        return current

    def _stat_level(self, stat):
        load = float(stat.get("load"))
        connections = int(stat.get("connections"))
        result = {"load": "low", "connections": "low"}

        for level in self.values.keys():
            items = self.values[level]

            if load >= items.get("load_min") and load <= items.get("load_max"):
                result["load"] = level

            if connections >= items.get("conn_min") and load <= items.get("conn_max"):
                result["connections"] = level

        return result

    def verify_stats(self):
        stats = self.health.stats()
        current = self.current_level

        for stat in stats:
            self._log(f'{stat}')
            level = self._stat_level(stat)
            conn_level = level.get("connections")
            load_level = level.get("load")

            if conn_level != current:
                self.update_weight(conn_level)
                return None

            if load_level != current:
                self.update_weight(load_level)
                return None

    def update_weight(self, level):
        update_level = level if self.current_level == "medium" else "medium"
        item = self.values.get(update_level)
        dccm_weight = item.get("dccm_weight")
        gcp_weight = item.get("gcp_weight")
        self._log(f'Update DNS Weight: Globo {dccm_weight}, GCP {gcp_weight}')

        if not self.dry_run:
            self.health.set_dns_weight(dccm_weight, gcp_weight)


async def work():
    app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
    ctx = app.app_context()
    ctx.push()

    while True:
        dry_run = app.config.get("HEALTH_DRY_RUN") == "True"
        if dry_run:
            start_msg = "[SERVICE][HEALTH (DRY RUN)]"
        else:
            start_msg = "[SERVICE][HEALTH]"

        app.logger.info(f'{start_msg} Health task started')

        weight_handler = WeightHandler(dry_run)
        weight_handler.verify_stats()

        app.logger.info(f'{start_msg} Health task completed\n')
        # app.logger.info('[SERVICE][HEALTH] Sending passive monitoring to zabbix')
        # zabbix.send()

        await asyncio.sleep(int(app.config.get("HEALTH_INTERVAL")))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    try:
        asyncio.ensure_future(work())
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info('[SERVICE][HEALTH] Closing loop')
        loop.close()
