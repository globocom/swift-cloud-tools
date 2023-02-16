# EXAMPLE
# python scripts/pages/2_create_marker_db.py 643f797035bf416ba8001e95947622c0 show_failover production

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
project_id = params[0]
project_name = params[1]
environment = params[2]

app = create_app(f"config/{environment}_config.py")
ctx = app.app_context()
ctx.push()

keystone = Keystone()
conn = keystone.get_keystone_connection()

url = f"https://api.s3.globoi.com/v1/AUTH_{project_id}"
headers = {'X-Cloud-Bypass': '136f8e168edb41afbbad3da60d048c64'}
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

for container in containers:
    container_name = container.get('name')
    marker = None

    if not container_name:
        continue

    sql = f"INSERT INTO `transfer_container_paginated` (`project_id`, `project_name`, `container_name`, `marker`, `hostname`, `environment`, `object_count_swift`, `bytes_used_swift`, `count_error`, `object_count_gcp`, `bytes_used_gcp`, `initial_date`, `final_date`) VALUES ('{project_id}', '{project_name}', '{container_name}', NULL, NULL, 'pages2', 0, 0, 0, 0, 0, NULL, NULL);"

    print(f"{bcolors.OKGREEN}'{project_name}' - '{container_name}'{bcolors.ENDC} - {bcolors.OKCYAN}''{bcolors.ENDC}")
    query = db.session.execute(sql)

    while True:
        meta, objects = swift_client.get_container(
            url, 
            conn.auth_token, 
            container_name, 
            delimiter=None,
            prefix=None,
            marker=marker, 
            full_listing=False,
            http_conn=http_conn, 
            headers=headers, 
            limit=10000
        )

        if (len(objects) > 0):
            marker = objects[-1].get('name')
            sql = f"INSERT INTO `transfer_container_paginated` (`project_id`, `project_name`, `container_name`, `marker`, `hostname`, `environment`, `object_count_swift`, `bytes_used_swift`, `count_error`, `object_count_gcp`, `bytes_used_gcp`, `initial_date`, `final_date`) VALUES ('{project_id}', '{project_name}', '{container_name}', '{marker}', NULL, 'pages2', 0, 0, 0, 0, 0, NULL, NULL);"

            print(f"{bcolors.OKGREEN}'{project_name}' - '{container_name}'{bcolors.ENDC} - {bcolors.OKCYAN}'{marker}'{bcolors.ENDC}")
            query = db.session.execute(sql)
        else:
            break

print(f"{bcolors.OKGREEN}ok...{bcolors.ENDC}")
