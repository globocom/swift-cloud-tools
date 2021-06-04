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
            return "Incorrect parameters", 422

        account = params.get('account')
        container = params.get('container')
        obj = params.get('object')
        date = params.get('date')

        if not account or not container or not obj or not date:
            return "Incorrect parameters", 422

        if len(date) == 19:
            try:
                date_obj = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return "Invalid date format: YYYY-MM-DD HH:MM:SS", 422
        else:
            return "Invalid date format: YYYY-MM-DD HH:MM:SS", 422

        expired_object = ExpiredObject(
            account=account,
            container=container,
            obj=obj,
            date=date
        )
        app.logger.info('[API] POST Expirer: {}'.format({
            'account': account,
            'container': container,
            'obj': obj,
            'date': date
        }))
        return expired_object.save()

    @is_authenticated
    def delete(self):
        """Delete expirer register by account, container and obj."""

        params = request.get_json()

        if not params:
            return "Incorrect parameters", 422

        account = params.get('account')
        container = params.get('container')
        obj = params.get('object')

        if not account or not container or not obj:
            return "Incorrect parameters", 422

        expired_object = ExpiredObject.find_expired_object(account, container, obj)

        app.logger.info('[API] DELETE Expirer: {}'.format({
            'account': account,
            'container': container,
            'obj': obj
        }))

        if not expired_object:
            return 'Not found', 404
        else:
            return expired_object.delete()
