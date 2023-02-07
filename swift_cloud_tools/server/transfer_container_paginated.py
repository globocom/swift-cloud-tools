#!/usr/bin/python3
import asyncio
import threading
import logging
import socket
import copy
import os
import re
import time

from datetime import datetime
from random import uniform
from sqlalchemy import func

from swift_cloud_tools.models import TransferContainerPaginated, db
from swift_cloud_tools.server.synchronize_container_paginated import SynchronizeContainersPaginated
from swift_cloud_tools.server.zbx_passive import Zabbix
from swift_cloud_tools import create_app

log = logging.getLogger(__name__)


async def work():
    zabbix = Zabbix(os.environ.get("ZBX_PASSIVE_MONITOR_TRANSFER"))
    number_of_units = int(os.environ.get("NUMBER_OF_UNITS", 10))
    transfer_time = int(os.environ.get("TRANSFER_TIME", '600'))
    app = create_app('config/{}_config.py'.format(os.environ.get("FLASK_CONFIG")))
    ctx = app.app_context()
    ctx.push()
    threads = {}
    hostnames = {}

    while True:
        app.logger.info('[SERVICE][TRANSFER_CONTAINER_PAGINATED] Transfer container task started')
        env = os.environ.get("ENVIRONMENT")
        hostname = socket.gethostname()

        try:
            raws = TransferContainerPaginated.query.filter(
                TransferContainerPaginated.environment == env,
                TransferContainerPaginated.initial_date != None,
                TransferContainerPaginated.final_date == None
            ).all()
        except Exception as err:
            app.logger.info("[SERVICE][TRANSFER_CONTAINER_PAGINATED] 500 Query 'mysql': {}".format(err))
            continue

        running = len(raws)
        available = number_of_units - running

        time.sleep(int(uniform(5, 15)))

        try:
            raws = TransferContainerPaginated.query.filter(
                TransferContainerPaginated.environment == env,
                TransferContainerPaginated.initial_date == None,
                TransferContainerPaginated.final_date == None
            ).all()
        except Exception as err:
            app.logger.info("[SERVICE][TRANSFER_CONTAINER_PAGINATED] 500 Query 'mysql': {}".format(err))
            continue

        try:
            container_page_hostnames = db.session.query(
                TransferContainerPaginated.hostname,
                func.count(TransferContainerPaginated.hostname)
            ).filter(TransferContainerPaginated.final_date=None)
            .filter(TransferContainerPaginated.hostname!=None)
            .group_by(TransferContainerPaginated.hostname).all()
        except Exception as err:
            app.logger.info("[SERVICE][TRANSFER] 500 Query 'mysql': {}".format(err))
            continue

        for container_page_hostname in container_page_hostnames:
            hostnames[container_page_hostname[0]] = container_page_hostname[1]

        diff = number_of_units - len(hostnames)

        if (diff == 0) or (hostname in hostnames):
            raws = []

        app.logger.info('[SERVICE][TRANSFER_CONTAINER_PAGINATED] running: {}'.format(running))
        app.logger.info('[SERVICE][TRANSFER_CONTAINER_PAGINATED] available: {}'.format(available))
        app.logger.info('[SERVICE][TRANSFER_CONTAINER_PAGINATED] queue: {}'.format(len(raws)))
        app.logger.info('[SERVICE][TRANSFER_CONTAINER_PAGINATED] threads: {}'.format(threads))
        app.logger.info('[SERVICE][TRANSFER_CONTAINER_PAGINATED] hostname: {}'.format(hostname))

        threads_copy = copy.copy(threads)

        for key in threads_copy.keys():
            thread = threads_copy[key]
            app.logger.info('[SERVICE][TRANSFER_CONTAINER_PAGINATED] name: {}, isAlive: {}'.format(thread.name, thread.isAlive()))
            if not thread.isAlive():
                del threads[key]
                key = re.search("project_id=(.*);container_name=(.*);marker=(.*)", key)
                project_id = key.group(0)
                container_name = key.group(1)
                marker = key.group(2)
                del hostnames[hostname]
                try:
                    transfer_container = TransferContainerPaginated.find_transfer_container(project_id, container_name, marker)

                    if transfer_container:
                        transfer_container.final_date = datetime.now()
                        transfer_container.save()
                except Exception as err:
                    app.logger.info("[SERVICE][TRANSFER_CONTAINER_PAGINATED] 500 Save 'mysql': {}".format(err))
                    continue

        for raw in raws[:1]:
            try:
                try:
                    raw.initial_date = datetime.now()
                    raw.hostname = hostname
                    msg, status = raw.save()
                except Exception as err:
                    app.logger.info("[SERVICE][TRANSFER_CONTAINER_PAGINATED] 500 Save 'mysql': {}".format(err))
                    continue

                sync = SynchronizeContainersPaginated(raw.project_id, raw.container_name, raw.marker, hostname)
                thread_name = 'project_id={};container_name={};marker={}'.format(raw.project_id, raw.container_name, raw.marker)
                x = threading.Thread(target=sync.synchronize, args=(raw.project_id, raw.container_name, raw.marker, hostname,), name=thread_name)
                x.start()
                threads[thread_name] = x
            except Exception as err:
                app.logger.info("[SERVICE][TRANSFER_CONTAINER_PAGINATED] 500 Save 'mysql': {}".format(err))
                continue

        app.logger.info('[SERVICE][TRANSFER_CONTAINER_PAGINATED] Transfer container task completed')
        app.logger.info('[SERVICE][TRANSFER_CONTAINER_PAGINATED] Sending passive monitoring to zabbix')
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
        log.info('[SERVICE][TRANSFER_CONTAINER_PAGINATED] Closing loop')
        loop.close()
