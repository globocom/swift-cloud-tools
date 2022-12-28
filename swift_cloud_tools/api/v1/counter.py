# -*- coding: utf-8 -*-
import json
import os

from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

from flask import request, Response
from flask_restplus import Resource
from flask import current_app as app

from swift_cloud_tools.server.utils import Google
from swift_cloud_tools.api.v1 import api
from swift_cloud_tools.decorators import is_authenticated

ns = api.namespace('counter', description='Counter')

TOPIC = 'updates'
KEYS = set(['action', 'kind', 'account', 'container'])


@ns.route('/')
class Counter(Resource):

    @is_authenticated
    def post(self):
        """Create counter register by container and object"""

        params = request.get_json()

        if not params and request.data:
            params = json.loads(request.data)

        if not params:
            msg = "incorrect parameters ['action', 'kind', 'account', 'container']"
            return Response(msg, mimetype="text/plain", status=422)

        keys = set([*params.keys()])

        if not keys.issubset(KEYS):
            msg = "incorrect parameters ['action', 'kind', 'account', 'container']"
            return Response(msg, mimetype="text/plain", status=422)

        action = params.get('action')
        kind = params.get('kind')
        account = params.get('account')
        container = params.get('container')

        if not action or not kind or not account:
            msg = "incorrect parameters ['action', 'kind', 'account', 'container']"
            return Response(msg, mimetype="text/plain", status=422)

        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, TOPIC)

        if action == 'CREATE':
            future = publisher.publish(topic_path, b'CREATE', params=json.dumps(params))
            print(future.result())
        else:
            msg = 'incorrect parameter \'action\' [CREATE, DELETE])'
            return Response(msg, mimetype="text/plain", status=422)

        return Response('ok', mimetype="text/plain", status=200)

    @is_authenticated
    def delete(self):
        """Delete counter register by container and object"""

        params = request.get_json()

        if not params and request.data:
            params = json.loads(request.data)

        if not params:
            msg = "incorrect parameters ['action', 'kind', 'account', 'container']"
            return Response(msg, mimetype="text/plain", status=422)

        keys = set([*params.keys()])

        if not keys.issubset(KEYS):
            msg = "incorrect parameters ['action', 'kind', 'account', 'container']"
            return Response(msg, mimetype="text/plain", status=422)

        action = params.get('action')
        kind = params.get('kind')
        account = params.get('account')
        container = params.get('container')

        if not action or not kind or not account:
            msg = "incorrect parameters ['action', 'kind', 'account', 'container']"
            return Response(msg, mimetype="text/plain", status=422)

        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, TOPIC)

        if action == 'DELETE':
            future = publisher.publish(topic_path, b'DELETE', params=json.dumps(params))
            print(future.result())
        else:
            msg = 'incorrect parameter \'action\' [CREATE, DELETE])'
            return Response(msg, mimetype="text/plain", status=422)

        return Response('ok', mimetype="text/plain", status=200)
