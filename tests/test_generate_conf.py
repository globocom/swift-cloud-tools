# -*- coding: utf-8 -*-
import os
import unittest

from mock import patch
from generate_conf import generate_conf


class GenerateConfTestCase(unittest.TestCase):

    def tearDown(self):
        if os.path.exists("test.conf"):
            os.remove("test.conf")

    @patch.dict(os.environ, {}, clear=True)
    def test_generate_conf_without_environment_variables(self):
        generate_conf("test.conf")
        with open("test.conf", "r") as f:
            expected_piece = "\n".join([
                                 "auth_url = None",
                                 "auth_uri = None",
                                 "username = None",
                                 "password = None",
                                 "project_name = None",
                                 "service_token_roles_required = True"
                             ])
            expected_piece_db = "\n".join([
                                 "[filter:swift_cloud_tools_keystone_middleware]",
                                 "paste.filter_factory = swift_cloud_tools.middleware:swift_cloud_tools_keystone_factory"
                                ])
            result = str(f.read())
            self.assertIn(expected_piece, result)
            self.assertIn(expected_piece_db, result)

    @patch.dict(os.environ, {
            "KEYSTONE_URL": "https://auth.s3.globoi.com:5000/v2.0",
            "KEYSTONE_URL": "https://auth.s3.globoi.com:5000/v2.0",
            "KEYSTONE_SERVICE_USER": "u_test",
            "KEYSTONE_SERVICE_PASSWORD": "very_secret",
            "KEYSTONE_SERVICE_PROJECT": "TstProj"
        }, clear=True)
    def test_generate_conf_with_environment_variables(self):
        generate_conf("test.conf")
        with open("test.conf", "r") as f:
            expected_piece = "\n".join([
                                 "auth_url = https://auth.s3.globoi.com:5000/v2.0",
                                 "auth_uri = https://auth.s3.globoi.com:5000/v2.0",
                                 "username = u_test",
                                 "password = very_secret",
                                 "project_name = TstProj",
                                 "service_token_roles_required = True"
                             ])
            expected_piece_db = "\n".join([
                                 "[filter:swift_cloud_tools_keystone_middleware]",
                                 "paste.filter_factory = swift_cloud_tools.middleware:swift_cloud_tools_keystone_factory"
                                ])
            result = str(f.read())
            self.assertIn(expected_piece, result)
            self.assertIn(expected_piece_db, result)
