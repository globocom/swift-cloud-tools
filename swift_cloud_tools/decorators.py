# -*- coding: utf-8 -*-
from flask import request, Response
from flask import current_app as app
from functools import wraps


def is_authenticated(f):
    """Decorator to verify the user is authenticated."""

    @wraps(f)
    def dec(*args, **kwargs):

        if app.testing:
            return f(*args, **kwargs)

        if not request.headers.get('x-auth-token'):
            return Response('Unauthenticated', status=401)

        if request.headers.get('x-auth-token') != app.config.get('API_KEY'):
            return Response('Unauthenticated', status=401)

        return f(*args, **kwargs)

    return dec
