# -- coding: utf-8 --
from webob import Request
from tests.database_testcase import DatabaseTestCase

from swift_cloud_tools.middleware import SwiftCloudToolsKeystoneMiddleware
from swift_cloud_tools.models import ExpiredObject, db


class SwiftCloudToolsKeystoneMiddlewareTest(DatabaseTestCase):

    @classmethod
    def setUpClass(cls):
        super(SwiftCloudToolsKeystoneMiddlewareTest, cls).setUpClass()
        cls.headers = {'X-Auth-Token': cls.app.config.get('API_KEY')}
        cls.environ = {'REQUEST_METHOD': 'GET',
                       'HTTP_X_IDENTITY_STATUS': 'Confirmed',
                       'HTTP_X_SERVICE_IDENTITY_STATUS': 'Confirmed',
                       'HTTP_X_USER_NAME': 'u_testproject',
                       'HTTP_X_TENANT_NAME': 'testproject',
                       'HTTP_X_TENANT_ID': '12345'}
        cls.app.wsgi_app = SwiftCloudToolsKeystoneMiddleware(cls.app.wsgi_app, {})
        cls.app.testing = False

    @classmethod
    def tearDownClass(cls):
        super(SwiftCloudToolsKeystoneMiddlewareTest, cls).tearDownClass()
        cls.app.testing = True

    def setUp(self):
        ExpiredObject(
            account='auth_test',
            container='test',
            obj='test.jpg',
            date='2021-06-01 12:15:00'
        ).save()

    def tearDown(self):
        ExpiredObject.query.delete()

    def test_middleware_get_healthcheck(self):
        env = {}
        resp = Request.blank('/v1/healthcheck/', environ=env).get_response(self.app)

        self.assertEqual(resp.status_code, 200)

    def test_middleware_post_expirer_with_unconfirmed_identity_returns_401(self):
        env = self.environ.copy()
        env.update({
            'HTTP_X_IDENTITY_STATUS': None,
            'HTTP_X_SERVICE_IDENTITY_STATUS': None,
            'REQUEST_METHOD': 'POST',
        })
        resp = Request.blank('/v1/expirer/', environ=env).get_response(self.app)

        self.assertEqual(resp.status_code, 401)
        self.assertEqual(resp.body, b'Unauthenticated')

    def test_middleware_post_expirer_with_allowed_identity(self):
        env = self.environ.copy()
        env.update({
            'REQUEST_METHOD': 'POST'
        })
        data = {
            'account': 'auth_test1',
            'container': 'test1',
            'object': 'test1.jpg',
            'date': '2021-06-01 12:15:00'
        }
        resp = Request.blank(
            '/v1/expirer/',
            environ=env,
            json=data,
            headers=self.headers
        ).get_response(self.app)

        self.assertEqual(resp.status_code, 201)
        body = "Expired object '{}/{}/{}' created".format(
            data.get('account'), data.get('container'), data.get('object'))
        self.assertEqual(resp.body, body.encode('utf-8'))

    def test_middleware_post_expirer_with_allowed_identity_duplicate_entry(self):
        env = self.environ.copy()
        env.update({
            'REQUEST_METHOD': 'POST'
        })
        data = {
            'account': 'auth_test',
            'container': 'test',
            'object': 'test.jpg',
            'date': '2021-06-01 12:15:00'
        }
        resp = Request.blank(
            '/v1/expirer/',
            environ=env,
            json=data,
            headers=self.headers
        ).get_response(self.app)
        db.session.rollback()

        self.assertEqual(resp.status_code, 409)
        self.assertEqual(resp.body, b'Duplicate entry')

    def test_middleware_post_expirer_with_allowed_identity_empty_params(self):
        env = self.environ.copy()
        env.update({
            'REQUEST_METHOD': 'POST'
        })
        data = {}
        resp = Request.blank(
            '/v1/expirer/',
            environ=env,
            json=data,
            headers=self.headers
        ).get_response(self.app)

        self.assertEqual(resp.status_code, 422)
        self.assertEqual(resp.body, b'incorrect parameters')

    def test_middleware_post_expirer_with_allowed_identity_without_date(self):
        env = self.environ.copy()
        env.update({
            'REQUEST_METHOD': 'POST'
        })
        data = {
            'account': 'auth_test1',
            'container': 'test1',
            'object': 'test1.jpg'
        }
        resp = Request.blank(
            '/v1/expirer/',
            environ=env,
            json=data,
            headers=self.headers
        ).get_response(self.app)

        self.assertEqual(resp.status_code, 422)
        self.assertEqual(resp.body, b'incorrect parameters')

    def test_middleware_post_expirer_with_allowed_identity_short_date(self):
        env = self.environ.copy()
        env.update({
            'REQUEST_METHOD': 'POST'
        })
        data = {
            'account': 'auth_test1',
            'container': 'test1',
            'object': 'test1.jpg',
            'date': '2021-06-01'
        }
        resp = Request.blank(
            '/v1/expirer/',
            environ=env,
            json=data,
            headers=self.headers
        ).get_response(self.app)

        self.assertEqual(resp.status_code, 422)
        self.assertEqual(resp.body, b'invalid date format: YYYY-MM-DD HH:MM:SS')

    def test_middleware_post_expirer_with_allowed_identity_wrong_date(self):
        env = self.environ.copy()
        env.update({
            'REQUEST_METHOD': 'POST'
        })
        data = {
            'account': 'auth_test1',
            'container': 'test1',
            'object': 'test1.jpg',
            'date': '2021-06-01T12:15:00'
        }
        resp = Request.blank(
            '/v1/expirer/',
            environ=env,
            json=data,
            headers=self.headers
        ).get_response(self.app)

        self.assertEqual(resp.status_code, 422)
        self.assertEqual(resp.body, b'invalid date format: YYYY-MM-DD HH:MM:SS')

    def test_middleware_delete_expirer_with_unconfirmed_identity_returns_401(self):
        env = self.environ.copy()
        env.update({
            'HTTP_X_IDENTITY_STATUS': None,
            'HTTP_X_SERVICE_IDENTITY_STATUS': None,
            'REQUEST_METHOD': 'DELETE',
        })
        resp = Request.blank(
            '/v1/expirer/',
            environ=env
        ).get_response(self.app)

        self.assertEqual(resp.status_code, 401)
        self.assertEqual(resp.body, b'Unauthenticated')

    def test_middleware_delete_expirer_with_allowed_identity(self):
        env = self.environ.copy()
        env.update({
            'REQUEST_METHOD': 'DELETE'
        })
        data = {
            'account': 'auth_test',
            'container': 'test',
            'object': 'test.jpg'
        }
        resp = Request.blank(
            '/v1/expirer/',
            environ=env,
            json=data,
            headers=self.headers
        ).get_response(self.app)

        self.assertEqual(resp.status_code, 200)
        body = "Expired object '{}/{}/{}' deleted".format(
            data.get('account'), data.get('container'), data.get('object'))
        self.assertEqual(resp.body, body.encode('utf-8'))

    def test_middleware_delete_expirer_with_allowed_identity_empty_params(self):
        env = self.environ.copy()
        env.update({
            'REQUEST_METHOD': 'DELETE'
        })
        data = {}
        resp = Request.blank(
            '/v1/expirer/',
            environ=env,
            json=data,
            headers=self.headers
        ).get_response(self.app)

        self.assertEqual(resp.status_code, 422)
        self.assertEqual(resp.body, b'incorrect parameters')

    def test_middleware_delete_expirer_with_allowed_identity_without_object(self):
        env = self.environ.copy()
        env.update({
            'REQUEST_METHOD': 'DELETE'
        })
        data = {
            'account': 'auth_test',
            'container': 'test'
        }
        resp = Request.blank(
            '/v1/expirer/',
            environ=env,
            json=data,
            headers=self.headers
        ).get_response(self.app)

        self.assertEqual(resp.status_code, 422)
        self.assertEqual(resp.body, b'incorrect parameters')

    def test_middleware_delete_expirer_with_allowed_identity_not_found(self):
        env = self.environ.copy()
        env.update({
            'REQUEST_METHOD': 'DELETE'
        })
        data = {
            'account': 'auth_test',
            'container': 'test',
            'object': 'test.gif'
        }
        resp = Request.blank(
            '/v1/expirer/',
            environ=env,
            json=data,
            headers=self.headers
        ).get_response(self.app)

        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.body, b'not found')
