# EXAMPLE
# python scripts/pages/create_marker_db.py 643f797035bf416ba8001e95947622c0 show_failover internal_valor.globo.com

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
container_name = params[2]

url = 'https://api.s3.globoi.com/v1/AUTH_{}'.format(project_id)
headers = {'X-Cloud-Bypass': '136f8e168edb41afbbad3da60d048c64'}
marker = None

http_conn = swift_client.http_connection(url, insecure=False, timeout=3600)

sql = "INSERT INTO `transfer_container_paginated` (`project_id`, `project_name`, `container_name`, `marker`, `hostname`, `environment`, `object_count_swift`, `bytes_used_swift`, `count_error`, `object_count_gcp`, `bytes_used_gcp`, `initial_date`, `final_date`) VALUES ('{}', '{}', '{}', NULL, NULL, 'pages2', 0, 0, 0, 0, 0, NULL, NULL);".format(project_id, project_name, container_name)

print(sql)
print('-----------')
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
        sql = "INSERT INTO `transfer_container_paginated` (`project_id`, `project_name`, `container_name`, `marker`, `hostname`, `environment`, `object_count_swift`, `bytes_used_swift`, `count_error`, `object_count_gcp`, `bytes_used_gcp`, `initial_date`, `final_date`) VALUES ('{}', '{}', '{}', '{}', NULL, 'pages2', 0, 0, 0, 0, 0, NULL, NULL);".format(project_id, project_name, container_name, marker)

        print(sql)
        print('------------------')
        query = db.session.execute(sql)
    else:
        break

print('ok...')
