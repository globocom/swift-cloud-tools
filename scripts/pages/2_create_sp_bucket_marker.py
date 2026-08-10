# EXAMPLE
# python scripts/pages/2_create_sp_bucket_marker.py fee525a415c44147896903fab66d6855 False development

import sys
import os
import mysql.connector

# from swift_cloud_tools.models import db
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

LIMIT = 10000

params = sys.argv[1:]
legacy_swift_id = params[0]
applying = eval(params[1])
environment = params[2]

app = create_app(f"config/{environment}_config.py")
ctx = app.app_context()
ctx.push()

keystone = Keystone()
conn = keystone.get_keystone_connection()
keystone_admin_url = os.environ.get("KEYSTONE_ADMIN_URL")

cnx_transfer = mysql.connector.connect(
    user=os.environ.get("MYSQL_TRANSFER_USER"),
    password=os.environ.get("MYSQL_TRANSFER_PASSWORD"),
    host=os.environ.get("MYSQL_TRANSFER_HOST"),
    database=os.environ.get("MYSQL_TRANSFER_NAME")
)
cursor_transfer = cnx_transfer.cursor(buffered=True)

url = f"{keystone_admin_url}/v1/AUTH_{legacy_swift_id}"
# headers = {'X-Cloud-Bypass': '136f8e168edb41afbbad3da60d048c64'}
marker = None
count = 0

http_conn = swift_client.http_connection(url, insecure=False, timeout=3600)

account_stat, containers = swift_client.get_account(
    url,
    conn.auth_token,
    marker=None,
    end_marker=None,
    full_listing=True,
    http_conn=http_conn
    # headers=headers
)

container_count_dccm = int(account_stat.get('x-account-container-count'))
object_count_dccm = int(account_stat.get('x-account-object-count'))
bytes_used_dccm = int(account_stat.get('x-account-bytes-used'))

sql = "select project_name " \
      "from transfer_project " \
      "where project_id = '%s';" % legacy_swift_id

query = (sql)
cursor_transfer.execute(query)

result = cursor_transfer.fetchone()

if not result:
    print(f"\n{bcolors.WARNING}O projeto {bcolors.ENDC}{bcolors.OKGREEN}'{legacy_swift_id}'{bcolors.ENDC}{bcolors.WARNING} não está cadastrado na tabela {bcolors.ENDC}{bcolors.OKGREEN}`transfer_project`{bcolors.ENDC}")
    sys.exit()

project_name = result[0]

print(f"\n{bcolors.OKCYAN}PROJETO - {bcolors.ENDC}{bcolors.OKGREEN}{project_name}{bcolors.ENDC}")
print(f"{bcolors.OKCYAN}==========================================={bcolors.ENDC}")

for container in containers:
    container_name = container.get('name')
    marker = None

    if not container_name:
        continue

    if '.trash-' in container_name:
        continue

    if '_version_' in container_name:
        continue

    # sql = f"INSERT INTO `transfer_container_paginated` (`project_id`, `project_name`, `container_name`, `marker`, `hostname`, `environment`, `object_count_swift`, `bytes_used_swift`, `count_error`, `object_count_gcp`, `bytes_used_gcp`, `initial_date`, `final_date`) VALUES ('{legacy_swift_id}', '{project_name}', '{container_name}', NULL, NULL, 'pages', 0, 0, 0, 0, 0, NULL, NULL);"
    sql = "INSERT INTO `transfer_container_paginated` (" \
                "`project_id`," \
                "`project_name`," \
                "`container_name`," \
                "`marker`," \
                "`hostname`," \
                "`environment`," \
                "`object_count_swift`," \
                "`bytes_used_swift`," \
                "`count_error`," \
                "`object_count_gcp`," \
                "`bytes_used_gcp`," \
                "`initial_date`," \
                "`final_date`" \
            ") VALUES (" \
                "'%s'," \
                "'%s'," \
                "'%s'," \
                "%s," \
                "%s," \
                "'%s'," \
                "%s," \
                "%s," \
                "%s," \
                "%s," \
                "%s," \
                "%s," \
                "%s" \
            ");" % (
                legacy_swift_id,
                project_name,
                container_name,
                'NULL',
                'NULL',
                'pages',
                0,
                0,
                0,
                0,
                0,
                'NULL',
                'NULL'
            )

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
            # headers=headers, 
            limit=LIMIT
        )

        if (len(objects) > 0):
            count += 1
            print(f"{bcolors.OKGREEN}'{project_name}' - '{container_name}'{bcolors.ENDC} - {bcolors.OKCYAN}'{marker if marker else ''}'{bcolors.ENDC}")

            if applying:
                query = (sql)
                cursor_transfer.execute(query)
                cnx_transfer.commit()

            marker = objects[-1].get('name')

            if not marker:
                if 'subdir' in objects[-1].keys():
                    marker = objects[-1].get('subdir')

            # sql = f"INSERT INTO `transfer_container_paginated` (`project_id`, `project_name`, `container_name`, `marker`, `hostname`, `environment`, `object_count_swift`, `bytes_used_swift`, `count_error`, `object_count_gcp`, `bytes_used_gcp`, `initial_date`, `final_date`) VALUES ('{legacy_swift_id}', '{project_name}', '{container_name}', '{marker}', NULL, 'pages', 0, 0, 0, 0, 0, NULL, NULL);"
            sql = "INSERT INTO `transfer_container_paginated` (" \
                        "`project_id`," \
                        "`project_name`," \
                        "`container_name`," \
                        "`marker`," \
                        "`hostname`," \
                        "`environment`," \
                        "`object_count_swift`," \
                        "`bytes_used_swift`," \
                        "`count_error`," \
                        "`object_count_gcp`," \
                        "`bytes_used_gcp`," \
                        "`initial_date`," \
                        "`final_date`" \
                    ") VALUES (" \
                        "'%s'," \
                        "'%s'," \
                        "'%s'," \
                        "'%s'," \
                        "%s," \
                        "'%s'," \
                        "%s," \
                        "%s," \
                        "%s," \
                        "%s," \
                        "%s," \
                        "%s," \
                        "%s" \
                    ");" % (
                        legacy_swift_id,
                        project_name,
                        container_name,
                        marker,
                        'NULL',
                        'pages',
                        0,
                        0,
                        0,
                        0,
                        0,
                        'NULL',
                        'NULL'
                    )
        else:
            break

print(f"\n{bcolors.WARNING}Criadas {count} páginas{bcolors.ENDC}")
print(f"\n{bcolors.OKGREEN}ok...{bcolors.ENDC}")
