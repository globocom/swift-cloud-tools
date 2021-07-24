import json
from datetime import datetime

from flask import request, Response
from flask_restplus import Resource
from flask import current_app as app

from swift_cloud_tools.api.v1 import api
from swift_cloud_tools.models import ContainerInfo
from swift_cloud_tools.decorators import is_authenticated

ns = api.namespace("container-info", description="Container Info")


@ns.route("/")
class ContainerInfoAdd(Resource):
    
    @is_authenticated
    def post(self):
        """Adds or updates container information."""

        params = request.get_json()

        if not params and request.data:
            params = json.loads(request.data)

        if not params:
            msg = "incorrect parameters"
            return Response(msg, mimetype="text/plain", status=422)

        project_id = params.get("project_id")
        container_name = params.get("container_name")
        size = int(params.get("size"))
        remove = params.get("remove", False)

        current = ContainerInfo.find_container_info(project_id, container_name)

        if not current:
            c_info = ContainerInfo(project_id=project_id,
                                   container_name=container_name,
                                   updated=datetime.utcnow())
            c_info.object_count = 1
            c_info.bytes_used = size
            msg, status = c_info.save()
            return Response(msg, mimetype="text/plain", status=status)

        if remove:
            current.object_count = max(0, current.object_count - 1)
            current.bytes_used = max(0, current.bytes_used - size)
        else:
            current.object_count = current.object_count + 1
            current.bytes_used = current.bytes_used + size

        current.updated = datetime.utcnow()

        msg, status = current.save()
        return Response(msg, mimetype="text/plain", status=status)


@ns.route("/<string:project_id>/<string:container_name>")
class ContainerInfoGet(Resource):
    
    @is_authenticated
    def get(self, project_id, container_name):
        c_info = ContainerInfo.find_container_info(project_id, container_name)

        if not c_info:
            return {}, 404

        return c_info.to_dict(), 200


@ns.route("/<string:project_id>")
class ContainerInfoByAccount(Resource):
    
    @is_authenticated
    def get(self, project_id):
        acc_data = ContainerInfo.account_data(project_id)

        if not acc_data:
            return {}, 404

        return acc_data, 200