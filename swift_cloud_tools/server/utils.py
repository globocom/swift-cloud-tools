# -*- coding: utf-8 -*-
import json
import os

from flask import current_app as app
from flask import Response
from keystoneclient.v3 import client as keystone_client
from swiftclient import client as swift_client
from google.oauth2 import service_account
from google.cloud import storage


class Transfer():

    def __init__(self, last_object='', get_error=0, object_count_gcp=0, bytes_used_gcp=0):
        self.last_object = last_object
        self.get_error = get_error
        self.object_count_gcp = object_count_gcp
        self.bytes_used_gcp = bytes_used_gcp


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
        self.http_conn = swift_client.http_connection(self.storage_url, insecure=False, timeout=3600)
        self.conn = connection
        self.x_cloud_bypass = app.config.get('X_CLOUD_BYPASS')

    def set_account_meta_cloud(self):
        try:
            resp = {}
            swift_client.post_account(
                self.storage_url,
                self.conn.auth_token,
                response_dict=resp,
                headers={'X-Account-Meta-Cloud': 'gcp'}
            )
        except Exception:
            pass

        return resp['status'], resp['reason']

    def get_account(self):
        try:
            return swift_client.get_account(
                self.storage_url,
                self.conn.auth_token,
                full_listing=False,
                http_conn=self.http_conn,
                headers={'X-Cloud-Bypass': self.x_cloud_bypass}
            )
        except Exception as err:
            raise err

    def get_container(self, container, prefix=None, marker=None):
        try:
            return swift_client.get_container(
                self.storage_url,
                self.conn.auth_token,
                container,
                delimiter='/',
                prefix=prefix,
                marker=marker,
                full_listing=True,
                http_conn=self.http_conn,
                headers={'X-Cloud-Bypass': self.x_cloud_bypass}
            )
        except Exception as err:
            raise err

    def put_container(self, container, headers):
        try:
            resp = {}
            swift_client.put_container(
                self.storage_url,
                self.conn.auth_token,
                container,
                headers=headers,
                http_conn=self.http_conn,
                response_dict=resp
            )
        except Exception as err:
            raise err

        return resp['status'], resp['reason']

    def get_object(self, container, obj):
        try:
            return swift_client.get_object(
                self.storage_url,
                self.conn.auth_token,
                container,
                obj,
                http_conn=self.http_conn,
                headers={'X-Cloud-Bypass': self.x_cloud_bypass}
            )
        except Exception as err:
            raise err

    def put_object(self, container, name, content, content_length, content_type, headers):
        try:
            resp = {}
            swift_client.put_object(
                self.storage_url,
                token=self.conn.auth_token,
                container=container,
                name=name,
                contents=content,
                content_length=content_length,
                content_type=content_type,
                headers=headers,
                http_conn=self.http_conn,
                response_dict=resp
            )
        except Exception as err:
            raise err

        return resp['status'], resp['reason']
