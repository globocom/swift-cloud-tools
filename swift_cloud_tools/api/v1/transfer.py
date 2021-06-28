# -*- coding: utf-8 -*-
import json
import os

from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from flask import request, Response
from flask_restplus import Resource
from flask import current_app as app
from keystoneclient.v3 import client as keystone_client
from swiftclient import client as swift_client

from swift_cloud_tools.api.v1 import api
from swift_cloud_tools.decorators import is_authenticated

ns = api.namespace('transfer', description='Transfer')


@ns.route('/synchronize/<project_id>/')
class SynchronizeProjects(Resource):

    # @is_authenticated
    def get(self, project_id):
        """Get projects in swift."""

        conn = self._get_keystone_connection()
        projects = conn.projects.list()[0]

        for project in [projects]:
            admin_url = app.config.get('KEYSTONE_ADMIN_URL')
            storage_url = '{}/v1/AUTH_{}'.format(admin_url, project_id)
            http_conn = swift_client.http_connection(storage_url, insecure=False)
            x_cloud_bypass = app.config.get('X_CLOUD_BYPASS')

            try:
                account_stat, containers = swift_client.get_account(
                    storage_url,
                    conn.auth_token,
                    full_listing=False,
                    http_conn=http_conn,
                    headers={'X-Cloud-Bypass': x_cloud_bypass}
                )
            except swift_client.ClientException as err:
                app.logger.info('[API] 500 GET Synchronize: {}'.format(err))
                return Response(err, mimetype="text/plain", status=500)

            containers = self._hide_containers_with_prefixes(containers)

            container_count = account_stat.get('x-account-container-count')
            object_count = account_stat.get('x-account-object-count')
            bytes_used = account_stat.get('x-account-bytes-used')

            print('container_count: ' + container_count)
            print('object_count: ' + object_count)
            print('bytes_used: ' + bytes_used)

            if len(containers) > 0:
                gcp_client = self._get_gcp_client()
                try:
                    bucket = gcp_client.get_bucket('auth_{}'.format(project_id))
                except NotFound:
                    pass


            for container in containers:
                print('***** {} *****'.format(container.get('name')))
                if container.get('bytes') > 0:
                    try:
                        meta, objects = swift_client.get_container(
                            storage_url,
                            conn.auth_token,
                            container.get('name'),
                            delimiter='/',
                            prefix=None,
                            full_listing=False,
                            http_conn=http_conn
                        )
                    except swift_client.ClientException as err:
                        # import ipdb;ipdb.set_trace()
                        app.logger.info('[API] 500 GET Synchronize: {}'.format(err))

                    if len(objects) > 0:
                        self._get_container(
                            storage_url,
                            conn.auth_token,
                            container.get('name'),
                            http_conn,
                            objects
                        )

    def _get_container(self, storage_url, auth_token, container, http_conn, objects):
        for obj in objects:
            if obj.get('subdir'):
                prefix = obj.get('subdir')
                try:
                    meta, objects = swift_client.get_container(
                        storage_url,
                        auth_token,
                        container,
                        delimiter='/',
                        prefix=prefix,
                        full_listing=False,
                        http_conn=http_conn
                    )
                except swift_client.ClientException as err:
                    app.logger.info('[API] 500 GET Synchronize: {}'.format(err))

                if len(objects):
                    self._get_container(
                        storage_url,
                        auth_token,
                        container,
                        http_conn,
                        objects
                    )
            else:
                if obj.get('content_type') != 'application/directory':
                    print('==> {}'.format(obj.get('name')))

    def _get_keystone_connection(self):
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

    def _get_credentials(self):
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(os.environ.get("GCP_CREDENTIALS"))
        )

        return credentials.with_scopes(
            ['https://www.googleapis.com/auth/cloud-platform']
        )

    def _get_gcp_client(self):
        credentials = self._get_credentials()
        return storage.Client(credentials=credentials)

    def _hide_containers_with_prefixes(self, containers):
        """ Hide containers that starts with prefixes listed in hide_prefixes """

        hide_prefixes = ['.trash', '_version_']
        if hide_prefixes:
            for prefix in hide_prefixes:
                containers = [
                    obj for obj in containers if not obj['name'].startswith(prefix)]

        return containers
