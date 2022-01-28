import os
import sys

basedir = os.path.abspath(os.path.dirname(__file__))

DEBUG = True
TESTING = False
DATABASE_CONNECT_OPTIONS = {}
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI", "mysql://root@localhost:3306/swift_cloud_tools")

SQLALCHEMY_POOL_RECYCLE = None
SQLALCHEMY_POOL_TIMEOUT = None
SQLALCHEMY_TRACK_MODIFICATIONS = False
LOAD_MIDDLEWARES = False

KEYSTONE_HEALTHCHECK_TIMEOUT = 3
KEYSTONE_URL = os.environ.get("KEYSTONE_URL", "http://localhost:5000/v3")
KEYSTONE_ADMIN_URL = os.environ.get("KEYSTONE_ADMIN_URL")

API_KEY = os.environ.get("API_KEY", "toolsapikey")
X_CLOUD_BYPASS = os.environ.get("X_CLOUD_BYPASS", "xcloudbypass")

TSURU_USERNAME = os.environ.get("TSURU_USERNAME")
TSURU_PASSWORD = os.environ.get("TSURU_PASSWORD")

HOST_INFO_URL = os.environ.get("HOST_INFO_URL")
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL")

# SSH_USERNAME = os.environ.get("SSH_USERNAME")
# SSH_PASSWORD = os.environ.get("SSH_PASSWORD")

AWS_HOSTED_ZONE = os.environ.get('AWS_HOSTED_ZONE')

ACL_SERVICE_INSTANCE = os.environ.get('ACL_SERVICE_INSTANCE')

HEALTH_DNS = os.environ.get("HEALTH_DNS", "s3fe.storm.")
HEALTH_DCCM_IP = os.environ.get("HEALTH_DCCM_IP", "10.0.0.1")
HEALTH_GCP_IP = os.environ.get("HEALTH_GCP_IP", "10.0.0.2")
HEALTH_VALUES = os.environ.get("HEALTH_VALUES")
HEALTH_INTERVAL = os.environ.get("HEALTH_INTERVAL", "240")
HEALTH_DRY_RUN = os.environ.get("HEALTH_DRY_RUN", "False")
