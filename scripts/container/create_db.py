# EXAMPLE
# python scripts/container/create_db.py 58d78b787ec34892b5aaa0c7a146155f cartola-prod

import subprocess
import json
import sys

from swift_cloud_tools.models import db
from swift_cloud_tools.server.utils import Keystone
from swift_cloud_tools import create_app

from swiftclient import client as swift_client


app = create_app('config/production_config.py')
ctx = app.app_context()
ctx.push()

keystone = Keystone()
conn = keystone.get_keystone_connection()
params = sys.argv[1:]

project_id = params[0]
project_name = params[1]

result = subprocess.run(['curl', '-s', '-i', 'https://api.s3.globoi.com/v1/AUTH_{}'.format(project_id), '-H', 'X-Auth-Token: {}'.format(conn.auth_token)], stdout=subprocess.PIPE)
out = result.stdout.decode("utf-8").replace('\r\n', '|')
out = out.split('|')[-1]
out = '["' + out.replace('\n', '","')[:-2] + ']'
containers = json.loads(out)

sql = "INSERT INTO `transfer_project` (`project_id`, `project_name`, `environment`, `container_count_swift`, `object_count_swift`, `bytes_used_swift`, `last_object`, `count_error`, `container_count_gcp`, `object_count_gcp`, `bytes_used_gcp`, `initial_date`, `final_date`) VALUES ('{}', '{}', 'container2', 0, 0, 0, '', 0, 0, 0, 0, NULL, NULL);".format(project_id, project_name)
print(sql)
print('------------------')
query = db.session.execute(sql)

for container in containers:
    sql = "INSERT INTO `transfer_container` (`project_id`, `project_name`, `container_name`, `environment`, `container_count_swift`, `object_count_swift`, `bytes_used_swift`, `last_object`, `count_error`, `container_count_gcp`, `object_count_gcp`, `bytes_used_gcp`, `initial_date`, `final_date`) VALUES ('{}', '{}', '#container#', 'container2', 0, 0, 0, '', 0, 0, 0, 0, NULL, NULL);".format(project_id, project_name)
    sql = sql.replace('#container#', container)

    print(sql)
    print('------------------')
    query = db.session.execute(sql)

print('ok...')
