# EXAMPLE
# python scripts/small/create_db.py False production

import subprocess
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
projetos = conn.projects.list()

for project in projetos:
    result = subprocess.run(['curl', '-s', '-I', f'https://api.s3.globoi.com/v1/AUTH_{project.id}', '-H', f'X-Auth-Token: {conn.auth_token}'], stdout=subprocess.PIPE)
    out = result.stdout.decode("utf-8").replace('\r\n', '|')[48:-4]
    outs = out.split('|')
    small = True

    sql = f"INSERT INTO transfer_project (`project_id`, `project_name`, `environment`, `container_count_swift`, `object_count_swift`, `bytes_used_swift`, `last_object`, `count_error`, `container_count_gcp`, `object_count_gcp`, `bytes_used_gcp`, `initial_date`, `final_date`) VALUES ('{project.id}', '{project.name}', 'small2', #container_count#, #object_count#, #bytes_used#, '', 0, 0, 0, 0, NULL, NULL);"

    for out in outs:
        item = out.split(':')
        if item[0] == 'x-account-container-count':
            sql = sql.replace('#container_count#', item[1])
        elif item[0] == 'x-account-object-count':
            if int(item[1]) > 480000:
                small = False
                break
            sql = sql.replace('#object_count#', item[1])
        elif item[0] == 'x-account-bytes-used':
            sql = sql.replace('#bytes_used#', item[1])

    if not small:
        continue

    print(f"{bcolors.OKCYAN}'{project.name}'{bcolors.ENDC} - {bcolors.OKGREEN}ok{bcolors.ENDC}")

    if applying:
        query = db.session.execute(sql)

print(f"{bcolors.OKGREEN}ok...{bcolors.ENDC}")
