# -*- coding: utf-8 -*-
import requests
import flask_migrate
import json

from flask_testing import LiveServerTestCase

from swift_cloud_tools import create_app as create_app_orig
from tests.utils import Utils


class Expirer(LiveServerTestCase):

    def create_app(self):
        self.headers = {'Content-type': 'application/json'}
        return create_app_orig(config_module='config/testing_config.py')

    def setUp(self):
        flask_migrate.upgrade()
        self.utils = Utils(self.get_server_url())

    def tearDown(self):
        flask_migrate.downgrade()

    def test_post_expirer(self):
        data = {
            "account": "auth_792079638c6441bca02071501f4eb273",
            "container": "test",
            "object": "test.jpeg",
            "date": "2021-06-01 12:15:00"
        }
        response = requests.post(
            '{}/v1/expirer/'.format(
                self.get_server_url()
            ), data=json.dumps(data), headers=self.headers
        )
        content = json.loads(response.content)

        self.assertEqual(response.status_code, 200)
        self.assertEqual('ok', content)

    def test_post_expirer_empty_params(self):
        data = {}
        response = requests.post(
            '{}/v1/expirer/'.format(
                self.get_server_url()
            ), data=json.dumps(data), headers=self.headers
        )
        content = json.loads(response.content)

        self.assertEqual(response.status_code, 422)
        self.assertEqual('incorrect parameters', content)

    def test_post_expirer_without_date(self):
        data = {
            "account": "auth_792079638c6441bca02071501f4eb273",
            "container": "test",
            "object": "test.jpeg"
        }
        response = requests.post(
            '{}/v1/expirer/'.format(
                self.get_server_url()
            ), data=json.dumps(data), headers=self.headers
        )
        content = json.loads(response.content)

        self.assertEqual(response.status_code, 422)
        self.assertEqual('incorrect parameters', content)

    def test_post_expirer_short_date(self):
        data = {
            "account": "auth_792079638c6441bca02071501f4eb273",
            "container": "test",
            "object": "test.jpeg",
            "date": "2021-06-01"
        }
        response = requests.post(
            '{}/v1/expirer/'.format(
                self.get_server_url()
            ), data=json.dumps(data), headers=self.headers
        )
        content = json.loads(response.content)

        self.assertEqual(response.status_code, 422)
        self.assertEqual('invalid date format: YYYY-MM-DD HH:MM:SS', content)

    def test_post_expirer_wrong_date(self):
        data = {
            "account": "auth_792079638c6441bca02071501f4eb273",
            "container": "test",
            "object": "test.jpeg",
            "date": "2021-06-01T12:15:00"
        }
        response = requests.post(
            '{}/v1/expirer/'.format(
                self.get_server_url()
            ), data=json.dumps(data), headers=self.headers
        )
        content = json.loads(response.content)

        self.assertEqual(response.status_code, 422)
        self.assertEqual('invalid date format: YYYY-MM-DD HH:MM:SS', content)

    def test_delete_expirer(self):
        self.utils.insert_expirer()

        data = {
            "account": "auth_792079638c6441bca02071501f4eb273",
            "container": "test",
            "object": "test.jpeg"
        }
        response = requests.delete(
            '{}/v1/expirer/'.format(
                self.get_server_url()
            ), data=json.dumps(data), headers=self.headers
        )
        content = json.loads(response.content)

        self.assertEqual(response.status_code, 200)
        self.assertEqual('ok', content)

    def test_delete_expirer_empty_params(self):
        data = {}
        response = requests.delete(
            '{}/v1/expirer/'.format(
                self.get_server_url()
            ), data=json.dumps(data), headers=self.headers
        )
        content = json.loads(response.content)

        self.assertEqual(response.status_code, 422)
        self.assertEqual('incorrect parameters', content)

    def test_delete_expirer_without_object(self):
        data = {
            "account": "auth_792079638c6441bca02071501f4eb273",
            "container": "test"
        }
        response = requests.delete(
            '{}/v1/expirer/'.format(
                self.get_server_url()
            ), data=json.dumps(data), headers=self.headers
        )
        content = json.loads(response.content)

        self.assertEqual(response.status_code, 422)
        self.assertEqual('incorrect parameters', content)

    def test_delete_expirer_not_found(self):
        data = {
            "account": "auth_792079638c6441bca02071501f4eb273",
            "container": "test",
            "object": "test.jpeg"
        }
        response = requests.delete(
            '{}/v1/expirer/'.format(
                self.get_server_url()
            ), data=json.dumps(data), headers=self.headers
        )
        content = json.loads(response.content)

        self.assertEqual(response.status_code, 404)
        self.assertEqual('not found', content)
