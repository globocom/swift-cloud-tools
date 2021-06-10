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

        if (request.environ.get('HTTP_X_IDENTITY_STATUS') != 'Confirmed' or request.environ.get(
                'HTTP_X_SERVICE_IDENTITY_STATUS') not in (None, 'Confirmed')):
            return Response('Unauthenticated', status=401)

        return f(*args, **kwargs)

    return dec
