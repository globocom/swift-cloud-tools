# EXAMPLE
# python scripts/pages/5_reset_thread.py 643f797035bf416ba8001e95947622c0 False production

import sys

from swift_cloud_tools.models import db
from swift_cloud_tools import create_app


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
applying = eval(params[1])
environment = params[2]

app = create_app(f"config/{environment}_config.py")
ctx = app.app_context()
ctx.push()

count = 0
minutes = 40

sql = f"SELECT project_name FROM `transfer_project` WHERE project_id='{project_id}';"
result = db.session.execute(sql)

if result.rowcount == 0:
    print(f"\n{bcolors.WARNING}O projeto {bcolors.ENDC}{bcolors.OKGREEN}'{project_id}'{bcolors.ENDC}{bcolors.WARNING} não está cadastrado na tabela {bcolors.ENDC}{bcolors.OKGREEN}`transfer_project`{bcolors.ENDC}")
    sys.exit()

row = dict(result.next())
project_name = row.get('project_name')

print(f"\n{bcolors.OKCYAN}PROJETO - {bcolors.ENDC}{bcolors.OKGREEN}{project_name}{bcolors.ENDC}")
print(f"{bcolors.OKCYAN}==========================================={bcolors.ENDC}")

sql = f"SELECT * FROM `transfer_container_paginated` WHERE project_id = '{project_id}' AND hostname IS NOT NULL AND initial_date IS NOT NULL AND final_date IS NULL AND date_add(initial_date,interval {minutes} minute) <= now();"
results = db.session.execute(sql)

for result in results:
    data = result.initial_date.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{bcolors.OKCYAN}id - {bcolors.ENDC}{bcolors.OKGREEN}{result.id}{bcolors.ENDC}{bcolors.OKCYAN}, container - {bcolors.ENDC}{bcolors.OKGREEN}{result.container_name}{bcolors.ENDC}{bcolors.OKCYAN}, initial_date - {bcolors.ENDC}{bcolors.OKGREEN}{data}{bcolors.ENDC}{bcolors.OKCYAN}, hostname - {bcolors.ENDC}{bcolors.OKGREEN}{result.hostname}{bcolors.ENDC}{bcolors.OKCYAN}, marker - {bcolors.ENDC}{bcolors.OKGREEN}{result.marker}{bcolors.ENDC}")

    sql = f"UPDATE `transfer_container_paginated` SET hostname = NULL, count_error = 0, object_count_gcp = 0, bytes_used_gcp = 0, initial_date = NULL WHERE id = {result.id};"

    if applying:
        query = db.session.execute(sql)
    count += 1

print(f"\n{bcolors.WARNING}{count} registros reiniciados{bcolors.ENDC}")
print(f"\n{bcolors.OKGREEN}ok...{bcolors.ENDC}")
