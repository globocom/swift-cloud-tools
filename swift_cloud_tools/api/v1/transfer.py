# -*- coding: utf-8 -*-
import json

from flask import request, Response
from flask_restplus import Resource
from flask import current_app as app
from datetime import datetime

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

        item = tp.to_dict()

        return item, 200


@ns.route('/status/<string:project_id>')
class TransferStatus(Resource):

    @is_authenticated
    def get(self, project_id):
        """Returns a project transfer item."""

        status = None
        tp = TransferProject.find_transfer_project(project_id)

        if not tp:
            return {'status': 'Migração não inicializada'}, 200

        item = tp.to_dict()

        if not item.get('initial_date') and not item.get('final_date'):
            status = {'status': 'Aguardando migração'}

        if item.get("initial_date") and not item.get("final_date"):
            count_swift = item.get('object_count_swift')
            count_gcp = item.get('count_error') + item.get('object_count_gcp')
            progress = 100
            if count_swift > 0:
                progress = (100 * count_gcp) / count_swift
            status = {'status': 'Migrando', 'progress': int(progress)}

        if item.get("final_date"):
            status = {'status': 'Migração concluída'}

        return status, 200


@ns.route('/status')
class TransferStatusOverall(Resource):

    @is_authenticated
    def get(self):
        """Returns an overall status of transfers."""

        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        tp = TransferProject.query.paginate(page, per_page)

        return {
            "page": page,
            "per_page": per_page,
            "pages": tp.pages,
            "total": tp.total,
            "items": [i.to_dict() for i in tp.items]
        }

    @is_authenticated
    def post(self):
        """Returns an overall status of transfers filtered by projects."""

        projects = request.get_json()

        if not projects and request.data:
            projects = json.loads(request.data)

        if not projects:
            msg = 'incorrect parameters'
            return Response(msg, mimetype="text/plain", status=422)

        tp = TransferProject.query.filter(
            TransferProject.project_id.in_(projects)).all()

        return [i.to_dict() for i in tp]
