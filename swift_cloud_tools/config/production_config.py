import os
import sys

basedir = os.path.abspath(os.path.dirname(__file__))

DEBUG = False
TESTING = False
DATABASE_CONNECT_OPTIONS = {}
SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI")
SQLALCHEMY_MIGRATE_REPO = os.path.join(basedir, 'db_repository')
SQLALCHEMY_POOL_RECYCLE = 299
SQLALCHEMY_POOL_TIMEOUT = 20
SQLALCHEMY_TRACK_MODIFICATIONS = True
LOAD_MIDDLEWARES = True

KEYSTONE_HEALTHCHECK_TIMEOUT = 3
KEYSTONE_URL = os.environ.get("KEYSTONE_URL")
KEYSTONE_ADMIN_URL = os.environ.get("KEYSTONE_ADMIN_URL")

API_KEY = os.environ.get("API_KEY")
X_CLOUD_BYPASS = os.environ.get("X_CLOUD_BYPASS")

TSURU_USERNAME = os.environ.get("TSURU_USERNAME")
TSURU_PASSWORD = os.environ.get("TSURU_PASSWORD")

HOST_INFO_URL = os.environ.get("HOST_INFO_URL")

SSH_USERNAME = os.environ.get("SSH_USERNAME")
SSH_PASSWORD = os.environ.get("SSH_PASSWORD")
