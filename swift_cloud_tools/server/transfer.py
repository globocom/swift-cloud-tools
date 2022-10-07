#!/usr/bin/python3
import asyncio
import threading
import logging
import socket
import copy
import os
import time
import gc

from datetime import datetime
from random import uniform
from sqlalchemy import func

from swift_cloud_tools.models import TransferProject, ProjectHostname, db
from swift_cloud_tools.server.synchronize import SynchronizeProjects
from swift_cloud_tools.server.zbx_passive import Zabbix
from swift_cloud_tools import create_app

log = logging.getLogger(__name__)


async def work():
    zabbix = Zabbix(os.environ.get("ZBX_PASSIVE_MONITOR_TRANSFER"))
    number_of_units = int(os.environ.get("NUMBER_OF_UNITS"))
    transfer_time = int(os.environ.get("TRANSFER_TIME", '600'))
    app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
    ctx = app.app_context()
    ctx.push()
    threads = {}
    hostnames = {}

    while True:
        app.logger.info('[SERVICE][TRANSFER] Transfer task started')
        env = os.environ.get("ENVIRONMENT")
        workers = int(os.environ.get("WORKERS", 10))
        hostname = socket.gethostname()

        raws = TransferProject.query.filter(
            TransferProject.environment == env,
            TransferProject.initial_date != None,
            TransferProject.final_date == None
        ).all()

        running = len(raws)
        available = workers - running

        time.sleep(int(uniform(10, 20)))

        raws = TransferProject.query.filter(
            TransferProject.environment == env,
            TransferProject.initial_date == None,
            TransferProject.final_date == None
        ).all()

        projects_hostnames = db.session.query(
            ProjectHostname.hostname,
            func.count(ProjectHostname.hostname)
        ).group_by(ProjectHostname.hostname).all()

        for project_hostname in projects_hostnames:
            hostnames[project_hostname[0]] = project_hostname[1]

        diff = number_of_units - len(hostnames)

        if hostname in hostnames:
            if diff == 0:
                count = hostnames[hostname]
                del hostnames[hostname]
                has_small = False
                for item in hostnames:
                    if hostnames[item] < count:
                        has_small = True
                        break
                if has_small:
                    raws = []
            elif hostname in hostnames:
                raws = []

        app.logger.info('[SERVICE][TRANSFER] running: {}'.format(running))
        app.logger.info('[SERVICE][TRANSFER] available: {}'.format(available))
        app.logger.info('[SERVICE][TRANSFER] queue: {}'.format(len(raws)))
        app.logger.info('[SERVICE][TRANSFER] threads: {}'.format(threads))
        app.logger.info('[SERVICE][TRANSFER] hostname: {}'.format(hostname))

        threads_copy = copy.copy(threads)

        for key in threads_copy.keys():
            thread = threads_copy[key]
            app.logger.info('[SERVICE][TRANSFER] name: {}, isAlive: {}'.format(thread.name, thread.isAlive()))
            if not thread.isAlive():
                del threads[key]
                gc.collect()
                transfer_object = TransferProject.find_transfer_project(key)

                if transfer_object:
                    transfer_object.final_date = datetime.now()
                    transfer_object.save()

                project_hostname = ProjectHostname.find_project_hostname(key, hostname)

                if project_hostname:
                    msg, status = project_hostname.delete()

        for raw in raws[:1]:
            sync = SynchronizeProjects(raw.project_id, hostname)
            x = threading.Thread(target=sync.synchronize, args=(raw.project_id, hostname,), name=raw.project_id)
            x.start()
            threads[raw.project_id] = x
            raw.initial_date = datetime.now()
            msg, status = raw.save()

            project_hostname = ProjectHostname(
                project_id=raw.project_id,
                hostname=hostname,
                updated=datetime.now()
            )
            msg, status = project_hostname.save()

        app.logger.info('[SERVICE][TRANSFER] Transfer task completed')
        app.logger.info('[SERVICE][TRANSFER] Sending passive monitoring to zabbix')
        zabbix.send()

        await asyncio.sleep(transfer_time)


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
