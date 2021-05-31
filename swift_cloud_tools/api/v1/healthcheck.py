# -*- coding: utf-8 -*-
import json
import flask
import six

from flask import Response, Blueprint
from flask_restplus import Resource
from flask import current_app as app
from sqlalchemy import create_engine

from swift_cloud_tools.api.v1 import api
from swift_cloud_tools.models import db

ns = api.namespace('healthcheck', description='Healthcheck')

def text(data, code, headers=None):
    return flask.make_response(six.text_type(data))


@ns.route('/')
class healthcheck(Resource):
    representations = {
        'text/plain': text,
    }

    @api.doc(responses={
        200: 'Success',
    })
    def get(self):
        return checklist()


def checklist():
    fails = []

    if not _is_db_ok():
        fails.append('db_fail')

    msg = ':'.join(fails) or "OK"

    return Response(json.dumps(msg), mimetype="text/plain", status=200)


def _is_db_ok():
    try:
        engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
    except Exception as e:
        # logger.error('Failed to create_engine: {}'.format(e))
        return False

    try:
        _ = engine.execute('SELECT 1+1 FROM dual')
    except Exception as e:
        # logger.error('Failed to healthcheck db: {}'.format(e))
        return False

    return True
