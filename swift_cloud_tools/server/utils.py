# -*- coding: utf-8 -*-
import json
import os

from flask import current_app as app
from flask import Response
from keystoneclient.v3 import client as keystone_client
from swiftclient import client as swift_client
from google.oauth2 import service_account
from google.cloud import storage


class Google():

    def _get_credentials(self):
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(os.environ.get("GCP_CREDENTIALS"))
        )

        return credentials.with_scopes(
            ['https://www.googleapis.com/auth/cloud-platform']
        )

    def get_gcp_client(self):
        credentials = self._get_credentials()
        return storage.Client(credentials=credentials)


class Keystone():

    def get_keystone_connection(self):
        return keystone_client.Client(
            insecure=False,
            username=os.environ.get("KEYSTONE_SERVICE_USER"),
            password=os.environ.get("KEYSTONE_SERVICE_PASSWORD"),
            project_name=os.environ.get("KEYSTONE_SERVICE_PROJECT"),
            auth_url=os.environ.get("KEYSTONE_URL"),
            user_domain_name='Default',
            project_domain_name='Default',
            timeout=3
        )


class Swift():

    def __init__(self, connection, project_id):
        self.project_id = project_id
        admin_url = app.config.get('KEYSTONE_ADMIN_URL')
        self.storage_url = '{}/v1/AUTH_{}'.format(admin_url, project_id)
        self.http_conn = swift_client.http_connection(self.storage_url, insecure=False)
        self.conn = connection
        self.x_cloud_bypass = app.config.get('X_CLOUD_BYPASS')

    def set_account_meta_cloud(self):
        try:
            swift_client.post_account(
                self.storage_url,
                self.conn.auth_token,
                headers={'X-Account-Meta-Cloud': 'gcp'}
            )
        except swift_client.ClientException as err:
            app.logger.info('[API] 500 GET Synchronize: {}'.format(err))
            return Response(err, mimetype="text/plain", status=500)

    def get_account(self):
        try:
            return swift_client.get_account(
                self.storage_url,
                self.conn.auth_token,
                full_listing=False,
                http_conn=self.http_conn,
                headers={'X-Cloud-Bypass': self.x_cloud_bypass}
            )
        except swift_client.ClientException as err:
            app.logger.info('[API] 500 GET Synchronize: {}'.format(err))
            return Response(err, mimetype="text/plain", status=500)

    def get_container(self, container, prefix=None):
        try:
            return swift_client.get_container(
                self.storage_url,
                self.conn.auth_token,
                container,
                delimiter='/',
                prefix=prefix,
                full_listing=False,
                http_conn=self.http_conn,
                headers={'X-Cloud-Bypass': self.x_cloud_bypass}
            )
        except swift_client.ClientException as err:
            app.logger.info('[API] 500 GET Synchronize: {}'.format(err))
            return Response(err, mimetype="text/plain", status=500)
