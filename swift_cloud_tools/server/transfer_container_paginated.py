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

from swift_cloud_tools.models import TransferContainerPaginated, db
from swift_cloud_tools.server.synchronize_container_paginated import SynchronizeContainersPaginated
from swift_cloud_tools.server.zbx_passive import Zabbix
from swift_cloud_tools import create_app

log = logging.getLogger(__name__)


async def work():
    zabbix = Zabbix(os.environ.get("ZBX_PASSIVE_MONITOR_TRANSFER"))
    number_of_units = int(os.environ.get("NUMBER_OF_UNITS", 10))
    transfer_time = int(os.environ.get("TRANSFER_TIME", '600'))

    app = create_app(f'config/{os.environ.get("FLASK_CONFIG")}_config.py')
    ctx = app.app_context()
    ctx.push()

    threads = {}
    hostnames = {}

    while True:
        app.logger.info('[SERVICE][TRANSFER_CONTAINER] Transfer container task started')
        env = os.environ.get("ENVIRONMENT")
        hostname = socket.gethostname()

        try:
            raws = TransferContainerPaginated.query.filter(
                TransferContainerPaginated.hostname != None,
                TransferContainerPaginated.environment == env,
                TransferContainerPaginated.initial_date != None,
                TransferContainerPaginated.final_date == None
            ).all()
        except Exception as err:
            app.logger.info(f"[SERVICE][TRANSFER_CONTAINER] 500 Query 'mysql': {err}")
            continue

        running = len(raws)
        available = number_of_units - running

        time.sleep(int(uniform(5, 15)))

        try:
            raws = TransferContainerPaginated.query.filter(
                TransferContainerPaginated.hostname == None,
                TransferContainerPaginated.environment == env,
                TransferContainerPaginated.initial_date == None,
                TransferContainerPaginated.final_date == None
            ).all()
        except Exception as err:
            app.logger.info(f"[SERVICE][TRANSFER_CONTAINER] 500 Query 'mysql': {err}")
            continue

        try:
            container_page_hostnames = db.session.query(
                TransferContainerPaginated.hostname
            ).filter(
                TransferContainerPaginated.final_date == None
            ).filter(
                TransferContainerPaginated.hostname != None
            ).all()

            hostnames = [x.hostname for x in container_page_hostnames]
        except Exception as err:
            app.logger.info(f"[SERVICE][TRANSFER] 500 Query 'mysql': {err}")
            continue

        diff = number_of_units - len(hostnames)

        app.logger.info('######################################')
        app.logger.info(f'[SERVICE][TRANSFER_CONTAINER] running: {running}')
        app.logger.info(f'[SERVICE][TRANSFER_CONTAINER] available: {available}')
        app.logger.info(f'[SERVICE][TRANSFER_CONTAINER] queue: {len(raws)}')
        app.logger.info(f'[SERVICE][TRANSFER_CONTAINER] hostname: {hostname}')
        app.logger.info(f'[SERVICE][TRANSFER_CONTAINER] threads: {threads}')

        try:
            container_page_hostnames = db.session.query(
                TransferContainerPaginated.hostname
            ).filter(
                TransferContainerPaginated.hostname == hostname
            ).all()

            container_page_hostnames = [x.hostname for x in container_page_hostnames]
        except Exception as err:
            app.logger.info(f"[SERVICE][TRANSFER] 500 Query 'mysql': {err}")
            continue

        if (diff == 0) or (len(container_page_hostnames) == 1):
            raws = []

        threads_copy = copy.copy(threads)

        for key in threads_copy.keys():
            thread = threads_copy[key]
            app.logger.info(f'[SERVICE][TRANSFER_CONTAINER] name: {thread.name}, isAlive: {thread.isAlive()}')
            if not thread.isAlive():
                key_re = re.search("project_id=(.*);container_name=(.*);marker=(.*)", key)
                project_id = key_re.group(1)
                container_name = key_re.group(2)
                marker = key_re.group(3)
                marker = None if marker == 'None' else marker
                del threads[key]
                app.logger.info(f'[SERVICE][TRANSFER_CONTAINER] del threads: {project_id};{container_name};{marker}')
                app.logger.info(f'[SERVICE][TRANSFER_CONTAINER] --> threads: {threads}')

                try:
                    db.session.begin()
                    transfer_container = db.session.query(TransferContainerPaginated).filter_by(
                        project_id=project_id,
                        container_name=container_name,
                        marker=marker
                    ).first()
                    transfer_container.hostname = None
                    transfer_container.final_date = datetime.now()
                    time.sleep(0.1)
                    db.session.commit()
                    app.logger.info(f'[SERVICE][TRANSFER_CONTAINER] --> commit id: {transfer_container.id}')
                except Exception as err:
                    app.logger.info(f"[SERVICE][TRANSFER_CONTAINER] 500 Save 'mysql' stop: {err}")
                    continue

        for raw in raws[:1]:
            try:
                try:
                    raw.initial_date = datetime.now()
                    raw.hostname = hostname
                    msg, status = raw.save()
                except Exception as err:
                    app.logger.info(f"[SERVICE][TRANSFER_CONTAINER] 500 Save 'mysql' start: {err}")
                    continue

                sync = SynchronizeContainersPaginated(raw.project_id, raw.container_name, raw.marker, hostname)
                thread_name = f'project_id={raw.project_id};container_name={raw.container_name};marker={raw.marker}'
                x = threading.Thread(target=sync.synchronize, args=(raw.project_id, raw.container_name, raw.marker, hostname,), name=thread_name)
                x.start()
                app.logger.info(f'[SERVICE][TRANSFER_CONTAINER] start threads: {thread_name}')
                threads[thread_name] = x

            except Exception as err:
                app.logger.info(f"[SERVICE][TRANSFER_CONTAINER] 500 Save 'mysql' start: {err}")
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
