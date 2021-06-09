# -*- coding: utf-8 -*-
import json

from datetime import datetime
from flask import request
from flask_restplus import Resource
from flask import current_app as app

from swift_cloud_tools.api.v1 import api
from swift_cloud_tools.models import db, ExpiredObject
from swift_cloud_tools.decorators import is_authenticated

ns = api.namespace('expirer', description='Expirer')


@ns.route('/')
class Expirer(Resource):

    @is_authenticated
    def post(self):
        """Create expirer register in DB."""

        params = request.get_json()

        if not params:
            return "incorrect parameters", 422

        account = params.get('account')
        container = params.get('container')
        obj = params.get('object')
        date = params.get('date')

        if not account or not container or not obj or not date:
            return "incorrect parameters", 422

        if len(date) == 19:
            try:
                date_obj = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return "invalid date format: YYYY-MM-DD HH:MM:SS", 422
        else:
            return "invalid date format: YYYY-MM-DD HH:MM:SS", 422

        expired_object = ExpiredObject(
            account=account,
            container=container,
            obj=obj,
            date=date
        )
        msg, status = expired_object.save()

        app.logger.info('[API] {} POST Expirer: {}'.format(status, {
            'account': account,
            'container': container,
            'obj': obj,
            'date': date
        }))
        return msg, status

    @is_authenticated
    def delete(self):
        """Delete expirer register by account, container and obj."""

        params = request.get_json()

        if not params:
            return "incorrect parameters", 422

        account = params.get('account')
        container = params.get('container')
        obj = params.get('object')

        if not account or not container or not obj:
            return "incorrect parameters", 422

        expired_object = ExpiredObject.find_expired_object(account, container, obj)

        if not expired_object:
            msg, status = 'not found', 404
        else:
            msg, status = expired_object.delete()

        app.logger.info('[API] {} DELETE Expirer: {}'.format(status, {
            'account': account,
            'container': container,
            'obj': obj
        }))
        return msg, status
