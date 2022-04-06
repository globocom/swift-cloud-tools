# -*- coding: utf-8 -*-
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
from google.cloud import billing_v1
from prometheus_api_client import PrometheusConnect

logger = logging.getLogger(__name__)
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

    def get_storage_client(self):
        credentials = self._get_credentials()
        return storage.Client(
            credentials=credentials,
            client_options={'api_endpoint': os.environ.get("GCS_API_ACCESS_ENDPOINT")}
        )

    def get_billing_client(self):
        credentials = self._get_credentials()
        return billing_v1.CloudCatalogClient(
            credentials=credentials
        )

    def get_sku_price_from_service(self, service, sku, amount):
        billing_client = self.get_billing_client()
        request = billing_v1.ListSkusRequest(parent='services/{}'.format(service),)
        skus = billing_client.list_skus(request=request)
        name = 'services/{}/skus/{}'.format(service, sku)
        price = 0.0
        currency = ''

        for item in skus:
            if item.name == name:
                pricing_info = item.pricing_info[0]
                tiered_rates = pricing_info.pricing_expression.tiered_rates[0]
                unit_price = tiered_rates.unit_price
                price = unit_price.nanos / 1000000000
                currency = unit_price.currency_code
                break

        amount_gb = ((int(amount) / 1024) / 1024) / 1024
        total = price * amount_gb

        return {'currency': currency, 'price': f'{total:.6f}'}


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

    def set_account_meta_cloud_migration(self):
        try:
            resp = {}
            swift_client.post_account(
                self.storage_url,
                self.conn.auth_token,
                response_dict=resp,
                headers={'X-Account-Meta-Cloud-Migration': 'x'}
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
        self.prometheus = PrometheusConnect(url=app.config.get('PROMETHEUS_URL'), disable_ssl=False)

    def _get_fe_hosts(self):
        hosts = []

        try:
            response = requests.get(self.hostinfo_url)
            if response.status_code == 200:
                for host in response.json():
                    hosts.append(host.get('Nome'))
            else:
                logger.error(f'Hostinfo error: {response.content}')
        except Exception as err:
            logger.error(f'Hostinfo connection error: {err}')

        return hosts

    def load_avg(self, host_ssh_client):
        _, stdout, stderr = host_ssh_client.exec_command("cat /proc/loadavg")
        out = stdout.read().decode("utf-8")
        err = stderr.read().decode("utf-8")

        if  err != "":
            self._log(f'Load Average Error: {err}', "error")
            return 'error'

        return float(out.split(" ")[:3][0])

    def open_connections(self, host, retry=0):
        label_config = {'instance': '{}:9100'.format(host)}
        metric = self.prometheus.get_current_metric_value(
            metric_name='node_netstat_Tcp_CurrEstab',
            label_config=label_config)

        try:
            connections = int(metric[0].get('value')[1])
        except Exception as err:
            if retry < 3:
                retry += 1
                self._log(f'Open connections error: {err}', "error")
                self._log(f'Open connections metric: {metric}', "error")
                self._log(f'Retrying open connections... {retry}', "error")
                return self.open_connections(host, retry)

            return 'error'

        return connections

    def cpu_usage(self, host, retry=0):
        query = '100 - (avg (rate(node_cpu_seconds_total{instance="' + host + ':9100", mode="idle"}[1m])) * 100)'
        metric = self.prometheus.custom_query(query=query)

        try:
            idle = float(metric[0].get('value')[1])
        except Exception as err:
            if retry < 3:
                retry += 1
                self._log(f'Cpu usage error: {err}', "error")
                self._log(f'Cpu usage metric: {metric}', "error")
                self._log(f'Retry cpu usage... {retry}', "error")
                return self.cpu_usage(host, retry)

            return 'error'

        return float("{:.2f}".format(idle))

    def stats(self):
        hosts = self._get_fe_hosts()

        for host in hosts:
            yield {
                "host": host,
                "cpu": self.cpu_usage(host),
                "connections": self.open_connections(host)
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
