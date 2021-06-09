# -*- coding: utf-8 -*-
from flask_restplus import Resource
from flask import current_app as app
from sqlalchemy import create_engine

from swift_cloud_tools.api.v1 import api

ns = api.namespace('healthcheck', description='Healthcheck')


@ns.route('/')
class Healthcheck(Resource):

    def get(self):
        msg, status = self.checklist()
        app.logger.info('[API] {} GET Healthcheck: {}'.format(status, msg))
        return msg, status


    def checklist(self):
        msg, status = self._is_db_ok()
        return msg, status


    def _is_db_ok(self):
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
