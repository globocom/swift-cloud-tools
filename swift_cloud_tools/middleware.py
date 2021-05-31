# -*- coding: utf-8 -*-
import requests

from webob import Request, Response
from sqlalchemy import create_engine

# logger = log.get_logger('middleware')

def healthcheck_factory(global_conf, **local_conf):
    def filter(app):
        app.wsgi_app = HealthcheckMiddleware(app.wsgi_app, local_conf)
        return app
    return filter


class HealthcheckMiddleware(object):

    def __init__(self, app, conf):
        # logger.info('Starting HealthcheckMiddleware')
        self.app = app
        self.conf = conf
        self.url = conf.get("url", "/healthcheck")
        self.message = conf.get("message", "WORKING")

    def __call__(self, environ, start_response):
        req = Request(environ)

        if req.path == self.url:
            return self.GET(req)(environ, start_response)

        return self.app(environ, start_response)

    def GET(self, req):
        fails = []

        if not self._is_db_ok():
            fails.append('db_fail')

        if not self._is_keystone_ok():
            fails.append('keystone_fail')

        msg = ':'.join(fails) or self.message

        return Response(request=req,
                        body=msg,
                        content_type="text/plain")

    def _is_db_ok(self):
        try:
            engine = create_engine(self.conf.get("database_uri"))
        except Exception as e:
            # logger.error('Fail to create_engine: {}'.format(e))
            return False

        try:
            _ = engine.execute('SELECT 1+1 FROM dual')
        except Exception as e:
            # logger.error('Fail to healthcheck db: {}'.format(e))
            return False

        return True

    def _is_keystone_ok(self):
        url = '{}/healthcheck'.format(self.conf.get("auth_url"))
        try:
            requests.get(url, timeout=int(self.conf['KEYSTONE_HEALTHCHECK_TIMEOUT']))
        except requests.exceptions.ConnectTimeout:
            # logger.error('Connection to {} timed out.'.format(url))
            return False

        return True
