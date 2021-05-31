# -- coding: utf-8 --
import json
import unittest

from mock import patch
from webob import Request, Response

from tests.database_testcase import DatabaseTestCase
from swift_cloud_tools.middleware import HealthcheckMiddleware


class TestApp(object):
    config = {'SQLALCHEMY_DATABASE_URI': 'sqlite:////tmp/test.db'}

    def __call__(self, env, start_response):
        return Response(body="Test App")(env, start_response)

    @property
    def wsgi_app(self):
        return self

class HealthcheckMiddlewareTest(DatabaseTestCase):

    @classmethod
    def setUpClass(cls):
        super(HealthcheckMiddlewareTest, cls).setUpClass()
        cls.app.wsgi_app = HealthcheckMiddleware(cls.app.wsgi_app, {})
        cls.environ = {'REQUEST_METHOD': 'GET'}

    @patch("swift_cloud_tools.middleware.HealthcheckMiddleware._is_keystone_ok")
    @patch("swift_cloud_tools.middleware.HealthcheckMiddleware._is_db_ok")
    def test_middleware_request_with_healthcheck_path(self, mock_is_db_ok, mock_is_keystone_ok):
        mock_is_db_ok.return_value = True
        mock_is_keystone_ok.return_value = True
        resp = Request.blank('/v1/healthcheck',
                             environ=self.environ).get_response(self.app)

        self.assertEqual(resp.body.decode('utf-8'), "WORKING")

    @patch("swift_cloud_tools.middleware.HealthcheckMiddleware._is_keystone_ok")
    @patch("swift_cloud_tools.middleware.HealthcheckMiddleware._is_db_ok")
    def test_middleware_request_with_database_not_working(self, mock_is_db_ok, _):
        mock_is_db_ok.return_value = False
        resp = Request.blank('/v1/healthcheck',
                             environ=self.environ).get_response(self.app)

        self.assertEqual(resp.body.decode('utf-8'), "db_fail")

    @patch("swift_cloud_tools.middleware.HealthcheckMiddleware._is_keystone_ok")
    @patch("swift_cloud_tools.middleware.HealthcheckMiddleware._is_db_ok")
    def test_middleware_request_with_keystone_not_working(self, _, mock_is_keystone_ok):
        mock_is_keystone_ok.return_value = False
        resp = Request.blank('/v1/healthcheck',
                             environ=self.environ).get_response(self.app)

        self.assertEqual(resp.body.decode('utf-8'), "keystone_fail")

    @patch("swift_cloud_tools.middleware.HealthcheckMiddleware._is_db_ok")
    @patch("swift_cloud_tools.middleware.HealthcheckMiddleware._is_keystone_ok")
    def test_middleware_request_with_database_and_keystone_not_working(self, mock_is_db_ok, mock_is_keystone_ok):
        mock_is_db_ok.return_value = False
        mock_is_keystone_ok.return_value = False
        resp = Request.blank('/v1/healthcheck',
                             environ=self.environ).get_response(self.app)

        self.assertEqual(resp.body.decode('utf-8'), "db_fail:keystone_fail")
