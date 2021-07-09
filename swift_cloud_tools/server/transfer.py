#!/usr/bin/python3
import asyncio
import threading
import logging
import copy
import os
import time

from datetime import datetime

from swift_cloud_tools.models import TransferProject
from swift_cloud_tools.server.synchronize import SynchronizeProjects
from swift_cloud_tools.server.zbx_passive import Zabbix
from swift_cloud_tools import create_app

log = logging.getLogger(__name__)


async def work():
    zabbix = Zabbix(os.environ.get("ZBX_PASSIVE_MONITOR_TRANSFER"))
    app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
    ctx = app.app_context()
    ctx.push()
    threads = {}

    while True:
        app.logger.info('[SERVICE][TRANSFER] Transfer task started')
        env = os.environ.get("ENVIRONMENT")
        workers = int(os.environ.get("WORKERS"))

        raws = TransferProject.query.filter(
            TransferProject.environment == env,
            TransferProject.initial_date != None,
            TransferProject.final_date == None
        ).all()

        running = len(raws)
        available = workers - running

        raws = TransferProject.query.filter(
            TransferProject.environment == env,
            TransferProject.initial_date == None,
            TransferProject.final_date == None
        ).all()

        app.logger.info('[SERVICE][TRANSFER] running: {}'.format(running))
        app.logger.info('[SERVICE][TRANSFER] available: {}'.format(available))
        app.logger.info('[SERVICE][TRANSFER] queue: {}'.format(len(raws)))
        app.logger.info('[SERVICE][TRANSFER] threads: {}'.format(threads))

        threads_copy = copy.copy(threads)

        for key in threads_copy.keys():
            thread = threads_copy[key]
            app.logger.info('[SERVICE][TRANSFER] name: {}, isAlive: {}'.format(thread.name, thread.isAlive()))
            if not thread.isAlive():
                del threads[key]
                transfer_object = TransferProject.find_transfer_project(key)

                if transfer_object:
                    transfer_object.final_date = datetime.now()
                    transfer_object.save()

        for raw in raws[:available]:
            sync = SynchronizeProjects()
            x = threading.Thread(target=sync.synchronize, args=(raw.project_id,), name=raw.project_id)
            x.start()
            threads[raw.project_id] = x
            raw.initial_date = datetime.now()
            msg, status = raw.save()

        app.logger.info('[SERVICE][TRANSFER] Transfer task completed')
        app.logger.info('[SERVICE][TRANSFER] Sending passive monitoring to zabbix')
        zabbix.send()

        await asyncio.sleep(int(os.environ.get("TRANSFER_TIME", '600')))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    try:
        asyncio.ensure_future(work())
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        log.info('[SERVICE][TRANSFER] Closing loop')
        loop.close()
