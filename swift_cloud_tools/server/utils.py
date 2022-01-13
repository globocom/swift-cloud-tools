# -*- coding: utf-8 -*-
import paramiko
import requests
import subprocess
import boto3
import json
import os
import logging

from socket import timeout

from flask import current_app as app
from flask import Response
from keystoneclient.v3 import client as keystone_client
from swiftclient import client as swift_client
from google.oauth2 import service_account
from google.cloud import storage

logging.getLogger("paramiko").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)


class Transfer():

    def __init__(self, last_object='', count_error=0, container_count_gcp=0, object_count_gcp=0,
                 bytes_used_gcp=0):
        self.last_object = last_object
        self.count_error = count_error
        self.container_count_gcp = container_count_gcp
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
        return storage.Client(
            credentials=credentials,
            client_options={'api_endpoint': os.environ.get("GCS_API_ACCESS_ENDPOINT")}
        )


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

    def get_account(self, marker=None, end_marker=None):
        try:
            return swift_client.get_account(
                self.storage_url,
                self.conn.auth_token,
                marker=marker,
                end_marker=end_marker,
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


class Health():

    def __init__(self):
        self.hostinfo_url = app.config.get('HOST_INFO_URL')
        self.ssh_username = app.config.get('SSH_USERNAME')
        self.ssh_password = app.config.get('SSH_PASSWORD')

    def _get_fe_hosts(self):
        response = requests.get(self.hostinfo_url)
        hosts = []

        if response.status_code != 200:
            print(response.content)
            return None

        for host in response.json():
            hosts.append(host.get('IP'))

        return hosts

    def acl_update(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.load_system_host_keys()
        hosts = self._get_fe_hosts()
        acl_service_instance = app.config.get('ACL_SERVICE_INSTANCE')

        if not hosts:
            return 'error'

        for host in hosts:
            try:
                client.connect(
                    host,
                    username=self.ssh_username,
                    password=self.ssh_password,
                    timeout=5
                )
            except timeout:
                process = subprocess.Popen(['tsuru',
                                            'acl',
                                            'rules',
                                            'add',
                                            'acl',
                                            acl_service_instance,
                                            '--ip',
                                            '{}/32'.format(host),
                                            '--port',
                                            'tcp:22'],
                                           stdout=subprocess.PIPE,
                                           universal_newlines=True)

                while True:
                    output = process.stdout.readline()
                    print(output.strip())
                    # Do something else
                    return_code = process.poll()
                    if return_code is not None:
                        print('RETURN CODE', return_code)
                        # Process has finished, read rest of the output
                        for output in process.stdout.readlines():
                            print(output.strip())
                        break

    def load_avg(self, host_ssh_client):
        _, stdout, stderr = host_ssh_client.exec_command("cat /proc/loadavg")
        out = stdout.read().decode("utf-8")
        err = stderr.read().decode("utf-8")

        if  err != "":
            self._log(f'Load Average Error: {err}')
            return 'error'

        return float(out.split(" ")[:3][0])

    def open_connections(self, host_ssh_client):
        _, stdout, stderr = host_ssh_client.exec_command(
            "/usr/sbin/ss -o state established '( sport = :8080 or sport = :8043 )' | wc -l")
        out = stdout.read().decode("utf-8")
        err = stderr.read().decode("utf-8")

        if  err != "":
            self._log(f'Open Connections Error: {err}')
            return 'error'

        return int(out.replace("\n", ""))

    def cpu_usage(self, host_ssh_client):
        _, stdout, stderr = host_ssh_client.exec_command("mpstat")
        out = stdout.read().decode("utf-8")
        err = stderr.read().decode("utf-8")

        if  err != "":
            self._log(f'CPU Usage Error: {err}')
            return 'error'

        idle = float(out[-6:].replace("\n", ""))

        return float("{:.2f}".format(100 - idle))

    def stats(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # ssh_client.load_system_host_keys()
        hosts = self._get_fe_hosts()

        for host in hosts:
            try:
                ssh_client.connect(
                    host,
                    username=self.ssh_username,
                    password=self.ssh_password,
                    timeout=5
                )
            except timeout:
                continue

            yield {
                "host": host,
                "cpu": self.cpu_usage(ssh_client),
                "connections": self.open_connections(ssh_client)
            }

    def set_dns_weight(self, weight_dccm, weight_gcp):
        client = boto3.client("route53")

        change = client.change_resource_record_sets(
            HostedZoneId=app.config.get("AWS_HOSTED_ZONE"),
            ChangeBatch={
            "Comment": "",
            "Changes": [
              {
                "Action": "UPSERT",
                "ResourceRecordSet": {
                  "Name": app.config.get("HEALTH_DNS"),
                  "Type": "A",
                  "SetIdentifier": "Globo",
                  "Weight": weight_dccm,
                  "TTL": 60,
                  "ResourceRecords": [
                      {
                          "Value": app.config.get("HEALTH_DCCM_IP")
                      }
                  ]
                }
              },
              {
                "Action": "UPSERT",
                "ResourceRecordSet": {
                  "Name": app.config.get("HEALTH_DNS"),
                  "Type": "A",
                  "SetIdentifier": "GCP",
                  "Weight": weight_gcp,
                  "TTL": 60,
                  "ResourceRecords": [
                      {
                          "Value": app.config.get("HEALTH_GCP_IP")
                      }
                  ]
                }
              }
            ]
        })

        resp = change.get('ResponseMetadata')

        if not resp:
            return {'message': 'error'}

        status = resp.get('HTTPStatusCode')

        if status != 200:
            return {'message': 'error'}

        return resp.get('ChangeInfo')

    def get_dns_weight(self):
        client = boto3.client('route53')

        res = client.list_resource_record_sets(
            HostedZoneId=app.config.get("AWS_HOSTED_ZONE"),
            StartRecordName=app.config.get("HEALTH_DNS"),
            StartRecordType="A"
        )

        records = res.get("ResourceRecordSets", [])

        weight_dccm = None
        weight_gcp = None

        for r in records:
            if r.get("Type") == "A" and r.get("SetIdentifier") == "Globo":
                weight_dccm = r.get("Weight")

            if r.get("Type") == "A" and r.get("SetIdentifier") == "GCP":
                weight_gcp = r.get("Weight")

        return weight_dccm, weight_gcp
