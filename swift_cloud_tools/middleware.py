# -*- coding: utf-8 -*-
from keystonemiddleware import auth_token


def keystone_factory(global_conf, **local_conf):
    def filter(app):
        app.wsgi_app = auth_token.AuthProtocol(app.wsgi_app, local_conf)
        return app
    return filter

def swift_cloud_tools_keystone_factory(global_conf, **local_conf):
    def filter(app):
        app.wsgi_app = SwiftCloudToolsKeystoneMiddleware(app.wsgi_app, local_conf)
        return app
    return filter


class SwiftCloudToolsKeystoneMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf

    def __call__(self, environ, start_response):
        identity = self.get_keystone_identity(environ)

        environ['identity'] = identity

        return self.app(environ, start_response)

    def get_keystone_identity(self, environ):
        roles = environ.get('HTTP_X_ROLES', '')
        return {
            'user': environ.get('HTTP_X_USER_NAME'),
            'tenant_id': environ.get('HTTP_X_TENANT_ID'),
            'tenant_name': environ.get('HTTP_X_TENANT_NAME'),
            'roles': [i.strip().lower() for i in roles.split(',') if i.strip()]
        }
