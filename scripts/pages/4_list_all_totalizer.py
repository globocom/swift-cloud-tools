# EXAMPLE
# python scripts/pages/4_list_all_totalizer.py False production

import sys

from swift_cloud_tools.models import db
from swift_cloud_tools.server.utils import Keystone
from swift_cloud_tools import create_app

from swiftclient import client as swift_client


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

params = sys.argv[1:]
applying = eval(params[0])
environment = params[1]

app = create_app(f"config/{environment}_config.py")
ctx = app.app_context()
ctx.push()

keystone = Keystone()
conn = keystone.get_keystone_connection()

sql = "SELECT project_id, project_name FROM `transfer_project`;"
projects = db.session.execute(sql)

for project in projects:
    print(f"\n{bcolors.OKCYAN}PROJETO - {bcolors.ENDC}{bcolors.OKGREEN}{project.project_name}{bcolors.ENDC}")
    print(f"{bcolors.OKCYAN}==========================================={bcolors.ENDC}")

    url = f"https://api.s3.globoi.com/v1/AUTH_{project.project_id}"
    headers = {'X-Cloud-Bypass': '136f8e168edb41afbbad3da60d048c64'}
    account = f"auth_{project.project_id}"
    marker = None

    http_conn = swift_client.http_connection(url, insecure=False, timeout=3600)

    account_stat, containers = swift_client.get_account(
        url,
        conn.auth_token,
        marker=None,
        end_marker=None,
        full_listing=True,
        http_conn=http_conn,
        headers=headers
    )

    container_count_dccm = len(containers)
    container_count_gcp = 0

    print(f"{bcolors.OKBLUE}################ Totalização de containers do projeto '{project.project_name}' ################{bcolors.ENDC}")

    for container in containers:
        sql = f"SELECT EXISTS(SELECT * FROM `transfer_container_paginated` WHERE project_id = '{project.project_id}') as exist;"

        result = db.session.execute(sql)
        row = dict(result.next())
        exist = row.get('exist') == True

        if not exist:
            print(f"{bcolors.FAIL}O projeto '{project.project_id}' não está cadastrado na base{bcolors.ENDC}")
            break

        container_name = container.get('name')

        if not container_name:
            print(f"{bcolors.WARNING}O nome do container '{container_name}' está incorreto{bcolors.ENDC}")
            continue

        sql = f"SELECT EXISTS(SELECT * FROM `transfer_container_paginated` WHERE project_id = '{project.project_id}' AND container_name = '{container_name}') as exist;"

        result = db.session.execute(sql)
        row = dict(result.next())
        exist = row.get('exist') == True

        if not exist:
            print(f"{bcolors.HEADER}'{container_name}'{bcolors.ENDC} - {bcolors.FAIL}{bcolors.BOLD}nok{bcolors.BOLD}{bcolors.ENDC} - {bcolors.OKCYAN}não cadastrado{bcolors.ENDC}")
            continue

        sql = f"SELECT COUNT(*) as finished FROM `transfer_container_paginated` WHERE project_id = '{project.project_id}' AND container_name = '{container_name}' AND (initial_date IS NULL OR final_date IS NULL);"

        result = db.session.execute(sql)
        row = dict(result.next())
        finished = True if row.get('finished') == 0 else False

        if applying:
            if finished:
                print(f"{bcolors.OKGREEN}'{container_name}'{bcolors.ENDC} - {bcolors.OKGREEN}{bcolors.BOLD}ok{bcolors.BOLD}{bcolors.ENDC} - {bcolors.OKCYAN}finalizado{bcolors.ENDC}")
                container_count_gcp += 1
            else:
                print(f"{bcolors.WARNING}'{container_name}'{bcolors.ENDC} - {bcolors.WARNING}{bcolors.BOLD}nok{bcolors.BOLD}{bcolors.ENDC} - {bcolors.OKCYAN}em migração{bcolors.ENDC}")

    print(f"\n{bcolors.WARNING}Finalizados {container_count_gcp} de {container_count_dccm}{bcolors.ENDC}")

print(f"\n{bcolors.OKGREEN}ok...{bcolors.ENDC}")
