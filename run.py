import os

from generate_conf import generate_conf

generate_conf('swift_cloud_tools.conf')

here_dir = os.getcwd()

from paste.deploy import loadapp
wsgi_app = loadapp('config:swift_cloud_tools.conf', relative_to=here_dir)
wsgi_app.run(debug=True, port=int(os.getenv('PORT', '8888')))
