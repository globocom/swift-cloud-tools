# -*- coding: utf-8 -*-
from jinja2 import Environment, FileSystemLoader


def generate_conf(file):
    file_loader = FileSystemLoader('templates')
    env = Environment(loader=file_loader)

    template = env.get_template('swift_cloud_tools_template.conf')

    with open(file, 'w') as f:
        f.write(template.render())
    
if __name__ == '__main__':
    generate_conf('swift_cloud_tools.conf')
