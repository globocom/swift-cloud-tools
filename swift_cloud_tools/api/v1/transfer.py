# -*- coding: utf-8 -*-
import json

from flask import request, Response
from flask_restplus import Resource
from flask import current_app as app

from swift_cloud_tools.api.v1 import api
from swift_cloud_tools.models import TransferProject
from swift_cloud_tools.decorators import is_authenticated

ns = api.namespace('transfer', description='Transfer')


@ns.route('/')
class Transfer(Resource):

    @is_authenticated
    def post(self):
        """Create transfer register in DB."""

        params = request.get_json()

        if not params and request.data:
            params = json.loads(request.data)

        if not params:
            msg = 'incorrect parameters'
            return Response(msg, mimetype="text/plain", status=422)

        project_id = params.get('project_id')
        project_name = params.get('project_name')
        environment = params.get('environment')

        if not project_id or not project_name or not environment:
            msg = 'incorrect parameters'
            return Response(msg, mimetype="text/plain", status=422)

        transfer_object = TransferProject(
            project_id=project_id,
            project_name=project_name,
            environment=environment
        )
        msg, status = transfer_object.save()

        app.logger.info('[API] {} POST Transfer: {}'.format(status, {
            'project_id': project_id,
            'project_name': project_name,
            'environment': environment
        }))
        return Response(msg, mimetype="text/plain", status=status)


@ns.route('/<string:project_id>')
class TransferItem(Resource):

    @is_authenticated
    def get(self, project_id):
        """Returns a project transfer item."""

        tp = TransferProject.find_transfer_project(project_id)

        if not tp:
            return {}, 404

        return tp.to_dict(), 200
