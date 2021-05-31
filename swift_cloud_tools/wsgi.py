# -*- coding: utf-8 -*-
# https://exploreflask.com/deployment.html

import os

here_dir = os.getcwd()

from paste.deploy import loadapp
application = loadapp('config:swift_cloud_tools.conf', relative_to=here_dir)
