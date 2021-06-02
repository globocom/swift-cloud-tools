import os
import sys

basedir = os.path.abspath(os.path.dirname(__file__))

DEBUG = True
TESTING = True
DATABASE_CONNECT_OPTIONS = {}
SQLALCHEMY_DATABASE_URI = 'mysql://root@localhost:3306/swift_cloud_tools_test'

SQLALCHEMY_POOL_RECYCLE = None
SQLALCHEMY_POOL_TIMEOUT = None
SQLALCHEMY_TRACK_MODIFICATIONS = False
LOAD_MIDDLEWARES = False

KEYSTONE_HEALTHCHECK_TIMEOUT = 3
KEYSTONE_URL = os.environ.get("KEYSTONE_URL")

API_KEY = os.environ.get("API_KEY")
