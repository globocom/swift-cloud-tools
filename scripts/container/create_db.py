# EXAMPLE
# python scripts/container/create_db.py 58d78b787ec34892b5aaa0c7a146155f cartola-prod production

import subprocess
import json
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

result = subprocess.run(['curl', '-s', '-i', f'https://api.s3.globoi.com/v1/AUTH_{project_id}', '-H', f'X-Auth-Token: {conn.auth_token}'], stdout=subprocess.PIPE)
out = result.stdout.decode("utf-8").replace('\r\n', '|')
out = out.split('|')[-1]
out = '["' + out.replace('\n', '","')[:-2] + ']'
containers = json.loads(out)

sql = f"INSERT INTO `transfer_project` (`project_id`, `project_name`, `environment`, `container_count_swift`, `object_count_swift`, `bytes_used_swift`, `last_object`, `count_error`, `container_count_gcp`, `object_count_gcp`, `bytes_used_gcp`, `initial_date`, `final_date`) VALUES ('{project_id}', '{project_name}', 'container2', 0, 0, 0, '', 0, 0, 0, 0, NULL, NULL);"

print(f"{bcolors.OKGREEN}'{project_name}'{bcolors.ENDC}")
query = db.session.execute(sql)

for container in containers:
    sql = f"INSERT INTO `transfer_container` (`project_id`, `project_name`, `container_name`, `environment`, `container_count_swift`, `object_count_swift`, `bytes_used_swift`, `last_object`, `count_error`, `container_count_gcp`, `object_count_gcp`, `bytes_used_gcp`, `initial_date`, `final_date`) VALUES ('{project_id}', '{project_name}', '#container#', 'container2', 0, 0, 0, '', 0, 0, 0, 0, NULL, NULL);"
    sql = sql.replace('#container#', container)

    print(f"{bcolors.OKGREEN}'{project_name}'{bcolors.ENDC} - {bcolors.OKCYAN}'{container}'{bcolors.ENDC}")
    query = db.session.execute(sql)

print(f"{bcolors.OKGREEN}ok...{bcolors.ENDC}")
