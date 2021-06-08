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
            expected_piece_db = "\n".join([
                                 "[app:swift_cloud_tools]",
                                 "paste.app_factory = swift_cloud_tools:app_factory"
                                ])
            result = str(f.read())
            self.assertIn(expected_piece_db, result)
