#!/usr/bin/python3
import os

from pyzabbix import ZabbixMetric, ZabbixSender
from datetime import datetime

from flask import current_app as app


class Zabbix():

    def __init__(self, passive_monitor):
        self.passive_monitor = passive_monitor

    def send(self):
        zabbix_server = ZabbixSender(
            zabbix_server=os.environ.get("ZBX_PASSIVE_SERVER"),
            zabbix_port=int(os.environ.get("ZBX_PASSIVE_PORT")),
            use_config=None,
            chunk_size=2
        )

        time_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Create the message
        passive_message_status = ZabbixMetric(
            self.passive_monitor,
            'passive_message',
            'Working - {0}'.format(time_now)
        )

        # Create status (0 - OK; 1 - Warning; 2 - Critical)
        passive_monitor_status = ZabbixMetric(
            self.passive_monitor,
            'passive_check', 0
        )

        metrics = [passive_message_status, passive_monitor_status]
        result = zabbix_server.send(metrics)

        try:
            if result.failed == 0:
                app.logger.info('[SERVICE][ZABBIX] Passive monitoring sent successfully')
            else:
                app.logger.error('[SERVICE][ZABBIX] Failed to send passive monitoring')
        except AttributeError:
            app.logger.error('[SERVICE][ZABBIX] Failure to verify passive monitoring return')
