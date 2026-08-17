# EXAMPLE
# python scripts/pages/1_create_sp_project_bucket.py fee525a415c44147896903fab66d6855 alanvitor gglobo-s3-prod-hdg-prd False development prod

import time
import sys
import os
import uuid
import mysql.connector

from datetime import datetime

from swift_cloud_tools.server.utils import Keystone, Google
from swift_cloud_tools import create_app

from swiftclient import client as swift_client
from google.api_core.exceptions import NotFound, Conflict, Forbidden
from google.api_core.retry import Retry
from mysql.connector.errors import IntegrityError


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
legacy_swift_id = params[0]
legacy_swift_name = params[1]
cloud_project_id = params[2]
applying = eval(params[3])
environment = params[4]
suffix_env = params[5]

suffix = {
    "dev": "s4-dv-1e528d",
    "qa": "s4-qa-44dc8f",
    "prod": "s4-pd-6a79ea"
}

app = create_app(f"config/{environment}_config.py")
ctx = app.app_context()
ctx.push()

google = Google()
keystone = Keystone()
conn = keystone.get_keystone_connection()
storage_client = google.get_storage_client()
keystone_admin_url = os.environ.get("KEYSTONE_ADMIN_URL")

cnx = mysql.connector.connect(
    user=os.environ.get("MYSQL_USER"),
    password=os.environ.get("MYSQL_PASSWORD"),
    host=os.environ.get("MYSQL_HOST"),
    database=os.environ.get("MYSQL_NAME")
)
cursor = cnx.cursor(buffered=True)

cnx_transfer = mysql.connector.connect(
    user=os.environ.get("MYSQL_TRANSFER_USER"),
    password=os.environ.get("MYSQL_TRANSFER_PASSWORD"),
    host=os.environ.get("MYSQL_TRANSFER_HOST"),
    database=os.environ.get("MYSQL_TRANSFER_NAME")
)
cursor_transfer = cnx_transfer.cursor(buffered=True)

url = f"{keystone_admin_url}/v1/AUTH_{legacy_swift_id}"
# headers = {'X-Cloud-Bypass': '136f8e168edb41afbbad3da60d048c64'}
bucket_location = 'SOUTHAMERICA-EAST1'
container_count_gcp = 0
marker = None

http_conn = swift_client.http_connection(url, insecure=False, timeout=10800)

account_stat, containers = swift_client.get_account(
    url,
    conn.auth_token,
    marker=None,
    end_marker=None,
    full_listing=True,
    http_conn=http_conn,
    # headers=headers
)

container_count_dccm = int(account_stat.get('x-account-container-count'))
object_count_dccm = int(account_stat.get('x-account-object-count'))
bytes_used_dccm = int(account_stat.get('x-account-bytes-used'))

def _create_containers(*containers):
    global container_count_gcp
    http_conn_local = swift_client.http_connection(url, insecure=False, timeout=10800)
    for container in containers:
        container_name = container.get('name')

        if not container_name:
            continue

        if '.trash-' in container_name:
            continue

        if '_version_' in container_name:
            continue

        while True:
            try:
                meta, objects = swift_client.get_container(
                    url,
                    conn.auth_token,
                    container_name,
                    delimiter=None,
                    prefix=None,
                    marker=marker,
                    full_listing=False,
                    http_conn=http_conn_local,
                    # headers=headers,
                    limit=1
                )
                break
            except Exception as e:
                print(f"Problem with container {container_name}: {e}")
            time.sleep(1)

        # blob = bucket.blob(container_name + '/')

        read = False
        cors_origins = ''

        for item in meta.items():
            key, value = item
            key = key.lower()

            if key == 'x-container-read':
                if value:
                    read = True
                continue

            if key == 'x-container-meta-access-control-allow-origin':
                if value:
                    cors_origins = value
                continue

        if applying:
            data_atual = datetime.now()
            bucket_id = uuid.uuid4()

            try:
                sql = "INSERT INTO `bucket` (" \
                            "`id`," \
                            "`project_id`," \
                            "`name`," \
                            "`external`," \
                            "`cors_origins`," \
                            "`created_by`," \
                            "`updated_at`" \
                        ") VALUES (" \
                            "'%s'," \
                            "'%s'," \
                            "'%s'," \
                            "%s," \
                            "'%s'," \
                            "'%s'," \
                            "'%s'" \
                        ");" % (
                            bucket_id,
                            project_id,
                            container_name,
                            read,
                            cors_origins,
                            'admin',
                            data_atual
                        )
                query = (sql)
                cursor.execute(query)
                cnx.commit()
            except IntegrityError:
                pass

            # blob.upload_from_string('',
            #     content_type='application/directory',
            #     num_retries=3,
            #     timeout=30
            # )

        container_count_gcp += 1

        print(f"{bcolors.OKCYAN}Criando container '{container_name}'{bcolors.ENDC} - {bcolors.OKGREEN}{bcolors.BOLD}ok{bcolors.BOLD}{bcolors.ENDC} - {container_count_gcp}")

print(f"\n{bcolors.OKCYAN}PROJETO - {bcolors.ENDC}{bcolors.OKGREEN}{legacy_swift_name}{bcolors.ENDC}")
print(f"{bcolors.OKCYAN}==========================================={bcolors.ENDC}")

if applying:
    try:
        sql = "INSERT INTO `transfer_project` (" \
                    "`project_id`," \
                    "`project_name`," \
                    "`environment`," \
                    "`container_count_swift`," \
                    "`object_count_swift`," \
                    "`bytes_used_swift`," \
                    "`last_object`," \
                    "`count_error`," \
                    "`container_count_gcp`," \
                    "`object_count_gcp`," \
                    "`bytes_used_gcp`," \
                    "`initial_date`," \
                    "`final_date`" \
                ") VALUES (" \
                    "'%s'," \
                    "'%s'," \
                    "'pages'," \
                    "%s," \
                    "%s," \
                    "%s," \
                    "''," \
                    "0," \
                    "0," \
                    "0," \
                    "0," \
                    "NULL," \
                    "NULL" \
                ");" % (
                    legacy_swift_id,
                    legacy_swift_name,
                    container_count_dccm,
                    object_count_dccm,
                    bytes_used_dccm
                )
        query = (sql)
        cursor_transfer.execute(query)
        cnx_transfer.commit()
    except IntegrityError:
        pass

    try:
        project_id = uuid.uuid4()

        sql = "INSERT INTO `project` (" \
                    "`id`," \
                    "`cloud_id`," \
                    "`legacy_swift_id`," \
                    "`legacy_swift_name`," \
                    "`cloud_project_id`," \
                    "`team`," \
                    "`created_by`" \
                ") VALUES (" \
                    "'%s'," \
                    "%s," \
                    "'%s'," \
                    "'%s'," \
                    "'%s'," \
                    "'%s'," \
                    "'%s'" \
                ");" % (
                    project_id,
                    1,
                    legacy_swift_id,
                    legacy_swift_name,
                    cloud_project_id,
                    'storm',
                    'admin',
                )
        query = (sql)
        cursor.execute(query)
        cnx.commit()
    except IntegrityError:
        sql = "select id " \
              "from project " \
              "where cloud_id = 1 " \
              "and legacy_swift_id = '%s';" % legacy_swift_id

        query = (sql)
        cursor.execute(query)

        project_id = cursor.fetchone()[0]

    bucket_name = f"{legacy_swift_name}__{suffix.get(suffix_env)}"

    try:
        bucket = storage_client.get_bucket(
            bucket_name,
            timeout=30
        )
    except NotFound:
        try:
            bucket = storage_client.create_bucket(
                bucket_name,
                location=bucket_location
            )
            deadline = Retry(deadline=60)
            bucket.patch(timeout=10, retry=deadline)
        except Conflict:
            pass
    except Forbidden:
        print(f"{bcolors.FAIL}Bucket já existe em outro ambiente - '{bucket_name}'{bcolors.ENDC}")
        sys.exit()

# Para projetos com muitos containers, cria esses containers em paralelo
if len(containers) > 300:
    n_threads = 50
    parts = []
    page_size = len(containers) // n_threads

    start = 0
    end = page_size

    while True:
        res = list(itertools.islice(containers, start, end))
        start += page_size
        end += page_size
        if not res:
            break
        parts.append(res)

    threads = [None] * len(parts)

    for i in range(len(threads)):
        time.sleep(0.5)
        threads[i] = threading.Thread(target=_create_containers, args=(
            parts[i]
        ))
        threads[i].start()
    for i in range(len(threads)):
        threads[i].join()
else:
    _create_containers(*containers)

print(f"\n{bcolors.WARNING}Bucket {legacy_swift_name} criado{bcolors.ENDC}")
print(f"\n{bcolors.WARNING}Criados {container_count_gcp} de {container_count_dccm}{bcolors.ENDC}")
print(f"\n{bcolors.OKGREEN}ok...{bcolors.ENDC}")
