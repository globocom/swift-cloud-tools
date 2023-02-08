#!/usr/bin/python3
import asyncio
import threading
import logging
import socket
import copy
import os
import time

from datetime import datetime
from random import uniform
from sqlalchemy import func

from swift_cloud_tools.models import TransferContainer, ProjectContainerHostname, db
from swift_cloud_tools.server.synchronize_container import SynchronizeContainers
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
        app.logger.info('[SERVICE][TRANSFER_CONTAINER] Transfer container task started')
        env = os.environ.get("ENVIRONMENT")
        workers = int(os.environ.get("WORKERS", 10))
        hostname = socket.gethostname()

        try:
            raws = TransferContainer.query.filter(
                TransferContainer.environment == env,
                TransferContainer.initial_date != None,
                TransferContainer.final_date == None
            ).all()
        except Exception as err:
            app.logger.info("[SERVICE][TRANSFER_CONTAINER] 500 Query 'mysql': {}".format(err))
            continue

        running = len(raws)
        available = workers - running

        time.sleep(int(uniform(5, 15)))

        try:
            raws = TransferContainer.query.filter(
                TransferContainer.environment == env,
                TransferContainer.initial_date == None,
                TransferContainer.final_date == None
            ).all()
        except Exception as err:
            app.logger.info("[SERVICE][TRANSFER_CONTAINER] 500 Query 'mysql': {}".format(err))
            continue

        try:
            project_container_hostnames = db.session.query(
                ProjectContainerHostname.hostname,
                func.count(ProjectContainerHostname.hostname)
            ).group_by(ProjectContainerHostname.hostname).all()
        except Exception as err:
            app.logger.info("[SERVICE][TRANSFER] 500 Query 'mysql': {}".format(err))
            continue

        for project_container_hostname in project_container_hostnames:
            hostnames[project_container_hostname[0]] = project_container_hostname[1]

        diff = number_of_units - len(hostnames)

        if (diff == 0) or (hostname in hostnames):
            raws = []

        app.logger.info('[SERVICE][TRANSFER_CONTAINER] running: {}'.format(running))
        app.logger.info('[SERVICE][TRANSFER_CONTAINER] available: {}'.format(available))
        app.logger.info('[SERVICE][TRANSFER_CONTAINER] queue: {}'.format(len(raws)))
        app.logger.info('[SERVICE][TRANSFER_CONTAINER] threads: {}'.format(threads))
        app.logger.info('[SERVICE][TRANSFER_CONTAINER] hostname: {}'.format(hostname))

        threads_copy = copy.copy(threads)

        for key in threads_copy.keys():
            thread = threads_copy[key]
            app.logger.info('[SERVICE][TRANSFER_CONTAINER] name: {}, isAlive: {}'.format(thread.name, thread.isAlive()))
            if not thread.isAlive():
                del threads[key]
                key = key.split('_', 1)
                project_id = key[0]
                container_name = key[1]
                del hostnames[hostname]
                try:
                    transfer_container = TransferContainer.find_transfer_container(project_id, container_name)

                    if transfer_container:
                        transfer_container.final_date = datetime.now()
                        transfer_container.save()
                except Exception as err:
                    app.logger.info("[SERVICE][TRANSFER_CONTAINER] 500 Save 'mysql': {}".format(err))
                    continue

                try:
                    project_container_hostname = ProjectContainerHostname.find_project_container_hostname(project_id, container_name, hostname)

                    if project_container_hostname:
                        msg, status = project_container_hostname.delete()
                except Exception as err:
                    app.logger.info("[SERVICE][TRANSFER_CONTAINER] 500 Delete 'mysql': {}".format(err))
                    continue

        for raw in raws[:1]:
            try:
                project_container_hostname = ProjectContainerHostname(
                    project_id=raw.project_id,
                    container_name=raw.container_name,
                    hostname=hostname,
                    updated=datetime.now()
                )
                msg, status = project_container_hostname.save()

                try:
                    raw.initial_date = datetime.now()
                    msg, status = raw.save()
                except Exception as err:
                    app.logger.info("[SERVICE][TRANSFER_CONTAINER] 500 Save 'mysql': {}".format(err))
                    continue

                sync = SynchronizeContainers(raw.project_id, raw.container_name, hostname)
                thread_name = '{}_{}'.format(raw.project_id, raw.container_name)
                x = threading.Thread(target=sync.synchronize, args=(raw.project_id, raw.container_name, hostname,), name=thread_name)
                x.start()
                threads[thread_name] = x
            except Exception as err:
                if '1062' in str(err):
                    # "Duplicate entry", 409
                    continue
                else:
                    app.logger.info("[SERVICE][TRANSFER_CONTAINER] 500 Save 'mysql': {}".format(err))
                    continue

        app.logger.info('[SERVICE][TRANSFER_CONTAINER] Transfer container task completed')
        app.logger.info('[SERVICE][TRANSFER_CONTAINER] Sending passive monitoring to zabbix')
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
        log.info('[SERVICE][TRANSFER_CONTAINER] Closing loop')
        loop.close()
