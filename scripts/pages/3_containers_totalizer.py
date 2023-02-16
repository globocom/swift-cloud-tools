# EXAMPLE
# python scripts/pages/3_containers_totalizer.py 643f797035bf416ba8001e95947622c0 show_failover False production

import time
import sys

from swift_cloud_tools.models import TransferProject, db
from swift_cloud_tools.server.utils import Keystone, Google
from swift_cloud_tools import create_app

from swiftclient import client as swift_client
from google.api_core.exceptions import NotFound, Conflict
from google.api_core.retry import Retry


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
project_id = params[0]
project_name = params[1]
applying = eval(params[2])
environment = params[3]

app = create_app(f"config/{environment}_config.py")
ctx = app.app_context()
ctx.push()

google = Google()
keystone = Keystone()
conn = keystone.get_keystone_connection()
storage_client = google.get_storage_client()

url = f"https://api.s3.globoi.com/v1/AUTH_{project_id}"
headers = {'X-Cloud-Bypass': '136f8e168edb41afbbad3da60d048c64'}
account = f"auth_{project_id}"
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

try:
    bucket = storage_client.get_bucket(
        account,
        timeout=30
    )
except NotFound:
    print(f"{bcolors.FAIL}Bucket não encontrado - '{account}'{bcolors.ENDC}")
    sys.exit()

print(f"{bcolors.OKBLUE}################ Totalização de containers do projeto '{project_name}' ################{bcolors.ENDC}")

for container in containers:
    sql = f"SELECT EXISTS(SELECT * FROM `transfer_container_paginated` WHERE project_id = '{project_id}') as exist;"

    result = db.session.execute(sql)
    row = dict(result.next())
    exist = row.get('exist') == True

    if not exist:
        print(f"{bcolors.FAIL}O projeto '{project_id}' não está cadastrado na base{bcolors.ENDC}")
        break

    container_name = container.get('name')

    if not container_name:
        print(f"{bcolors.WARNING}O nome do container '{container_name}' está incorreto{bcolors.ENDC}")
        continue

    sql = f"SELECT EXISTS(SELECT * FROM `transfer_container_paginated` WHERE project_id = '{project_id}' AND container_name = '{container_name}') as exist;"

    result = db.session.execute(sql)
    row = dict(result.next())
    exist = row.get('exist') == True

    if not exist:
        print(f"{bcolors.HEADER}'{container_name}'{bcolors.ENDC} - {bcolors.FAIL}{bcolors.BOLD}nok{bcolors.BOLD}{bcolors.ENDC} - {bcolors.OKCYAN}não cadastrado{bcolors.ENDC}")
        continue

    sql = f"SELECT COUNT(*) as finished FROM `transfer_container_paginated` WHERE project_id = '{project_id}' AND container_name = '{container_name}' AND (initial_date IS NULL OR final_date IS NULL);"

    result = db.session.execute(sql)
    row = dict(result.next())
    finished = True if row.get('finished') == 0 else False

    if finished:
        if applying:
            count = 0
            while True:
                try:
                    if count == 0:
                        db.session.begin()
                    transfer_project = db.session.query(TransferProject).filter_by(project_id=project_id).first()
                    transfer_project.container_count_gcp = TransferProject.container_count_gcp + 1
                    time.sleep(0.1)
                    db.session.commit()
                    break
                except Exception as e:
                    print(f"{bcolors.FAIL}{bcolors.BOLD}Problemas ao salvar os dados no mysql{bcolors.BOLD}{bcolors.ENDC}")
                    time.sleep(5)
                    count += 1

            while True:
                try:
                    labels = bucket.labels
                    container_count = int(labels.get('container-count', 0))
                    labels['container-count'] = container_count + 1
                    bucket.labels = labels
                    time.sleep(0.1)
                    deadline = Retry(deadline=60)
                    bucket.patch(timeout=10, retry=deadline)
                    break
                except Conflict:
                    print(f"{bcolors.FAIL}{bcolors.BOLD}Problemas ao salvar os dados no bucket{bcolors.BOLD}{bcolors.ENDC}")
                    print(f"{bcolors.WARNING}{bcolors.BOLD}Nova tentativa em 5 segundos...{bcolors.BOLD}{bcolors.ENDC}")
                    time.sleep(5)
                except Exception as e:
                    print(f"{bcolors.FAIL}{bcolors.BOLD}Problemas ao salvar os dados no bucket{bcolors.BOLD}{bcolors.ENDC}: {e}")
                    print(f"{bcolors.WARNING}{bcolors.BOLD}Nova tentativa em 5 segundos...{bcolors.BOLD}{bcolors.ENDC}")
                    time.sleep(5)

        print(f"{bcolors.OKGREEN}'{container_name}'{bcolors.ENDC} - {bcolors.OKGREEN}{bcolors.BOLD}ok{bcolors.BOLD}{bcolors.ENDC} - {bcolors.OKCYAN}finalizado{bcolors.ENDC}")
    else:
        print(f"{bcolors.WARNING}'{container_name}'{bcolors.ENDC} - {bcolors.WARNING}{bcolors.BOLD}nok{bcolors.BOLD}{bcolors.ENDC} - {bcolors.OKCYAN}em migração{bcolors.ENDC}")

print(f"{bcolors.OKGREEN}ok...{bcolors.ENDC}")
