# -*- coding: utf-8 -*-
import unittest

from swift_cloud_tools import create_app
from swift_cloud_tools.models import db

class DatabaseTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = create_app('config/testing_config.py')
        cls.context = cls.app.app_context()
        cls.context.push()

        cls.client = cls.app.test_client()
        cls._drop_db()
        db.create_all()

    @classmethod
    def tearDownClass(cls):
        cls._drop_db()
        cls.context.pop()

    @classmethod
    def _drop_db(cls):
        db.session.remove()
        db.drop_all()
