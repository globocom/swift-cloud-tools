import subprocess

from swift_cloud_tools.models import db
from swift_cloud_tools.server.utils import Keystone
from swift_cloud_tools import create_app

from swiftclient import client as swift_client


app = create_app('config/production_config.py')
ctx = app.app_context()
ctx.push()

keystone = Keystone()
conn = keystone.get_keystone_connection()
projetos = conn.projects.list()

for project in projetos:
    result = subprocess.run(['curl', '-s', '-I', 'https://api.s3.globoi.com/v1/AUTH_{}'.format(project.id), '-H', 'X-Auth-Token: {}'.format(conn.auth_token)], stdout=subprocess.PIPE)
    out = result.stdout.decode("utf-8").replace('\r\n', '|')[48:-4]
    outs = out.split('|')
    sql = "INSERT INTO transfer_project (`project_id`, `project_name`, `environment`, `container_count_swift`, `object_count_swift`, `bytes_used_swift`, `last_object`, `count_error`, `container_count_gcp`, `object_count_gcp`, `bytes_used_gcp`, `initial_date`, `final_date`) VALUES ('" + project.id + "', '" + project.name + "', '#env#', #container_count#, #object_count#, #bytes_used#, '', 0, 0, 0, 0, NULL, NULL);"

    for out in outs:
        item = out.split(':')
        if item[0] == 'x-account-container-count':
            sql = sql.replace('#container_count#', item[1])
        elif item[0] == 'x-account-object-count':
            if int(item[1]) > 7000000:
                sql = sql.replace('#env#', 'container2')
            else:
                sql = sql.replace('#env#', 'small2')
            sql = sql.replace('#object_count#', item[1])
        elif item[0] == 'x-account-bytes-used':
            sql = sql.replace('#bytes_used#', item[1])

    print(sql)
    print('------------------')
    query = db.session.execute(sql)

print('ok...')
