# EXAMPLE
# python scripts/container/create_container_gcp.py 643f797035bf416ba8001e95947622c0 internal_valor.globo.com

import sys

from swift_cloud_tools.server.utils import Keystone, Google
from swift_cloud_tools import create_app

from swiftclient import client as swift_client
from google.api_core.exceptions import NotFound, Conflict
from google.api_core.retry import Retry


app = create_app('config/production_config.py')
ctx = app.app_context()
ctx.push()

google = Google()
keystone = Keystone()
conn = keystone.get_keystone_connection()
storage_client = google.get_storage_client()
params = sys.argv[1:]

project_id = params[0]
container_name = params[1]

url = 'https://api.s3.globoi.com/v1/AUTH_{}'.format(project_id)
headers = {'X-Cloud-Bypass': '136f8e168edb41afbbad3da60d048c64'}
account = 'auth_{}'.format(project_id)
bucket_location = 'SOUTHAMERICA-EAST1'
marker = None

http_conn = swift_client.http_connection(url, insecure=False, timeout=3600)

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
    limit=1
)

try:
    bucket = storage_client.get_bucket(
        account,
        timeout=30
    )
except NotFound:
    try:
        bucket = storage_client.create_bucket(
            account,
            location=bucket_location
        )

        labels = bucket.labels
        labels['account-meta-cloud'] = 'gcp'
        labels['container-count'] = 0
        labels['object-count'] = 0
        labels['bytes-used'] = 0
        bucket.labels = labels

        deadline = Retry(deadline=60)
        bucket.patch(timeout=10, retry=deadline)
    except Conflict:
        pass

blob = bucket.blob(container_name + '/')
metadata = {}

metadata['object-count'] = meta.get('x-container-object-count', 0)
metadata['bytes-used'] = meta.get('x-container-bytes-used', 0)

for item in meta.items():
    key, value = item
    key = key.lower()
    prefix = key.split('x-container-meta-')

    print(key +' - '+ value)

    if len(prefix) > 1:
        meta_key = 'meta-{}'.format(prefix[1].lower())
        metadata[meta_key] = item[1].lower()
        continue

    if key == 'x-container-read':
        metadata["read"] = value
        continue

    if key == 'x-versions-location' or key == 'x-history-location':
        metadata["x-versions-location"] = value
        continue

    if key == 'x-undelete-enabled':
        metadata["x-container-sysmeta-undelete-enabled"] = value
        metadata["x-undelete-enabled"] = value
        continue

blob.metadata = metadata

blob.upload_from_string('',
    content_type='application/directory',
    num_retries=3,
    timeout=30
)

print('ok...')
