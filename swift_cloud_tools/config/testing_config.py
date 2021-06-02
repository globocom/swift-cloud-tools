import os
import sys

basedir = os.path.abspath(os.path.dirname(__file__))


DEBUG = True
TESTING = True
SQLALCHEMY_DATABASE_URI = 'mysql://root:@' + \
                          os.environ.get("DATABASES_DEFAULT_HOST", 'localhost') + \
                          '/' + os.environ.get("DATABASES_DEFAULT_NAME", 'swift_cloud_tools')
SQLALCHEMY_TRACK_MODIFICATIONS = True
LOAD_MIDDLEWARES = False

SQLALCHEMY_POOL_RECYCLE = None
SQLALCHEMY_POOL_TIMEOUT = None
SQLALCHEMY_TRACK_MODIFICATIONS = False

KEYSTONE_HEALTHCHECK_TIMEOUT = 3
KEYSTONE_URL = os.environ.get("SWIFT_CLOUD_TOOLS_KEYSTONE_URL")

API_KEY = os.environ.get("API_KEY")
