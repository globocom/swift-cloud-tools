# -*- coding: utf-8 -*-
import os

from jinja2 import Environment, FileSystemLoader

def generate_conf(file):
    file_loader = FileSystemLoader('templates')
    env = Environment(loader=file_loader)

    template = env.get_template('swift_cloud_tools_template.conf')

    data = {
        'auth_url' : os.environ.get("KEYSTONE_URL"),
        'auth_uri' : os.environ.get("KEYSTONE_URL"),
        'username' : os.environ.get("KEYSTONE_SERVICE_USER"),
        'password' : os.environ.get("KEYSTONE_SERVICE_PASSWORD"),
        'project_name' : os.environ.get("KEYSTONE_SERVICE_PROJECT"),
        'database_uri' : os.environ.get("SQLALCHEMY_DATABASE_URI")
    }

    with open(file, 'w') as f:
        f.write(template.render(data=data))
    
if __name__ == '__main__':
    generate_conf('swift_cloud_tools.conf')
