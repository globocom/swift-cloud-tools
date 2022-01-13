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
            "cpu_min": 0,
            "cpu_max": 20,
            "conn_min": 0,
            "conn_max": 90000,
            "dccm_weight": 255,
            "gcp_weight": 1
        },
        "medium": {
            "cpu_min": 21,
            "cpu_max": 40,
            "conn_min": 90001,
            "conn_max": 180000,
            "dccm_weight": 235,
            "gcp_weight": 20
        },
        "high": {
            "cpu_min": 41,
            "cpu_max": 100,
            "conn_min": 180001,
            "conn_max": 999999,
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

        current = self.current_level
        self._log(f'Current DNS Weight -> Globo: {dc_w}, GCP: {gcp_w} ({current.upper()})')

    def _log(self, msg, level="info"):
        dry_run_msg = " (DRY RUN)" if self.dry_run else ""
        msg = f'[SERVICE][HEALTH{dry_run_msg}] {msg}'

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
        cpu = float(stat.get("cpu"))
        connections = int(stat.get("connections"))
        result = {
            "host": stat.get("host"),
            "cpu": "low",
            "connections": "low"
        }

        for level in self.values.keys():
            items = self.values[level]

            if cpu >= items.get("cpu_min") and cpu <= items.get("cpu_max"):
                result["cpu"] = level

            if connections >= items.get("conn_min") and connections <= items.get("conn_max"):
                result["connections"] = level

        return result

    def verify_stats(self):
        stats = self.health.stats()
        current = self.current_level

        for stat in stats:
            stat_level = self._stat_level(stat)

            cpu_level = stat_level.get("cpu")
            if cpu_level == "error":
                cpu_level = current

            conn_level = stat_level.get("connections")
            if conn_level == "error":
                conn_level = current

            host = stat_level.get("host")
            self._log(f'Host stats: {stat}')

            if cpu_level != current:
                self._log(f'Host {host} with CPU level {cpu_level.upper()}, current is {current.upper()}.')
                self.update_weight(cpu_level)
                return None

            if conn_level != current:
                self._log(f'Host {host} with connections level {conn_level.upper()}, current is {current.upper()}.')
                self.update_weight(conn_level)
                return None

    def update_weight(self, level):
        update_level = level if self.current_level == "medium" else "medium"
        item = self.values.get(update_level)
        dccm_weight = item.get("dccm_weight")
        gcp_weight = item.get("gcp_weight")
        self._log(f'Updating DNS Weight to {update_level.upper()} -> Globo: {dccm_weight}, GCP: {gcp_weight}')

        if not self.dry_run:
            self.health.set_dns_weight(dccm_weight, gcp_weight)


async def work():
    app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
    ctx = app.app_context()
    ctx.push()

    while True:
        dry_run = app.config.get("HEALTH_DRY_RUN") == "True"
        dry_run_msg = " (DRY RUN)" if dry_run else ""
        start_msg = f'[SERVICE][HEALTH{dry_run_msg}]'

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
