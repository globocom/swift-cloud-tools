# -*- coding: utf-8 -*-
import requests

from flask_testing import LiveServerTestCase

from swift_cloud_tools import create_app as create_app_orig


class Healthcheck(LiveServerTestCase):

    def create_app(self):
        return create_app_orig(config_module='config/testing_config.py')

    def test_get_healthcheck_ok(self):
        response = requests.get('{}/v1/healthcheck/'.format(self.get_server_url()))

        self.assertEqual(response.status_code, 200)
