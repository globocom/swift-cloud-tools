# -*- coding: utf-8 -*-
import json
import flask
import six

from flask_restplus import Resource
from flask import current_app as app
from sqlalchemy import create_engine

from swift_cloud_tools.api.v1 import api
from swift_cloud_tools.models import db

ns = api.namespace('healthcheck', description='Healthcheck')


@ns.route('/')
class healthcheck(Resource):

    def get(self):
        msg, status = checklist()
        app.logger.info('[API] {} GET Healthcheck: {}'.format(status, msg))
        return msg, status


def checklist():
    msg, status = _is_db_ok()
    return msg, status


def _is_db_ok():
    msg = 'ok'

    try:
        engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
    except Exception as e:
        msg = 'Failed to create_engine: {}'.format(str(e))
        return msg, 500

    try:
        _ = engine.execute('SELECT 1+1 FROM dual')
    except Exception as e:
        msg = 'Failed to healthcheck db: {}'.format(e)
        return msg, 500

    return msg, 200
