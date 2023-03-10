# EXAMPLE
# python scripts/pages/1_create_container_gcp.py 643f797035bf416ba8001e95947622c0 show_failover False production

import time
import sys
import itertools
import threading

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
bucket_location = 'SOUTHAMERICA-EAST1'
container_count_gcp = 0
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

container_count_dccm = int(account_stat.get('x-account-container-count'))
object_count_dccm = int(account_stat.get('x-account-object-count'))
bytes_used_dccm = int(account_stat.get('x-account-bytes-used'))

def _create_containers(*containers):
    global container_count_gcp
    for container in containers:
        container_name = container.get('name')

        if not container_name:
            continue

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
            limit=1
        )

        blob = bucket.blob(container_name + '/')
        metadata = {}

        metadata['object-count'] = meta.get('x-container-object-count', 0)
        metadata['bytes-used'] = meta.get('x-container-bytes-used', 0)

        for item in meta.items():
            key, value = item
            key = key.lower()
            prefix = key.split('x-container-meta-')

            if len(prefix) > 1:
                meta_key = f'meta-{prefix[1].lower()}'
                metadata[meta_key] = item[1].lower()
                continue

            if key == 'x-container-read':
                metadata["read"] = value
                continue

            if key == 'x-versions-location' or key == 'x-history-location':
                metadata["x-versions-location"] = value
                continue

            if key == 'x-undelete-enabled':
                metadata["x-container-sysmeta-undelete-enabled"] = value
                metadata["x-undelete-enabled"] = value
                continue

        blob.metadata = metadata

        if applying:
            blob.upload_from_string('',
                content_type='application/directory',
                num_retries=3,
                timeout=30
            )
        container_count_gcp += 1

        print(f"{bcolors.OKCYAN}Criando container '{container_name}'{bcolors.ENDC} - {bcolors.OKGREEN}{bcolors.BOLD}ok{bcolors.BOLD}{bcolors.ENDC} - {container_count_gcp}")

print(f"\n{bcolors.OKCYAN}PROJETO - {bcolors.ENDC}{bcolors.OKGREEN}{project_name}{bcolors.ENDC}")
print(f"{bcolors.OKCYAN}==========================================={bcolors.ENDC}")

if applying:
    sql = f"INSERT INTO `transfer_project` (`project_id`, `project_name`, `environment`, `container_count_swift`, `object_count_swift`, `bytes_used_swift`, `last_object`, `count_error`, `container_count_gcp`, `object_count_gcp`, `bytes_used_gcp`, `initial_date`, `final_date`) VALUES ('{project_id}', '{project_name}', 'pages', {container_count_dccm}, {object_count_dccm}, {bytes_used_dccm}, '', 0, 0, 0, 0, NULL, NULL);"
    query = db.session.execute(sql)

try:
    bucket = storage_client.get_bucket(
        account,
        timeout=30
    )
except NotFound:
    if applying:
        try:
            bucket = storage_client.create_bucket(
                account,
                location=bucket_location
            )

            labels = bucket.labels
            labels['account-meta-cloud'] = 'gcp'
            labels['container-count'] = container_count_dccm
            labels['object-count'] = object_count_dccm
            labels['bytes-used'] = bytes_used_dccm
            bucket.labels = labels

            deadline = Retry(deadline=60)
            bucket.patch(timeout=10, retry=deadline)
        except Conflict:
            pass
    else:
        print(f"{bcolors.FAIL}Bucket não encontrado - '{account}'{bcolors.ENDC}")
        sys.exit()

# Para projetos com muitos containers, cria esses containers em paralelo
if len(containers) > 300:
    n_threads = 10
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

print(f"\n{bcolors.WARNING}Criados {container_count_gcp} de {container_count_dccm}{bcolors.ENDC}")
print(f"\n{bcolors.OKGREEN}ok...{bcolors.ENDC}")
