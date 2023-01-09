# -*- coding: utf-8 -*-
import json
import time
import os

from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound

from flask import request, Response
from flask_restplus import Resource

from swift_cloud_tools.server.utils import Google
from swift_cloud_tools.api.v1 import api
from swift_cloud_tools.decorators import is_authenticated

ns = api.namespace('counter', description='Counter')

TOPIC = 'updates'
KEYS = set(['action', 'kind', 'account', 'container', 'bytes-used', 'counter'])


@ns.route('/')
class Counter(Resource):

    @is_authenticated
    def post(self):
        """Create counter register by container and object"""

        params = request.get_json()
        msg = "incorrect parameters ['action', 'kind', 'account', 'container', 'bytes-used', 'counter']"

        if not params and request.data:
            params = json.loads(request.data)

        if not params:
            return Response(msg, mimetype="text/plain", status=422)

        keys = set([*params.keys()])

        if not keys.issubset(KEYS):
            return Response(msg, mimetype="text/plain", status=422)

        action = params.get('action')
        kind = params.get('kind')
        account = params.get('account')
        container = params.get('container')
        bytes_used = params.get('bytes-used')

        if not action or not kind or not account:
            return Response(msg, mimetype="text/plain", status=422)

        if kind == 'container' and (not container or bytes_used == None):
            return Response(msg, mimetype="text/plain", status=422)

        google = Google()
        credentials = google.get_client()
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
        publisher = pubsub_v1.PublisherClient(credentials=credentials)
        topic_path = publisher.topic_path(project_id, TOPIC)

        if action == 'CREATE':
            body = bytes(action, 'utf-8')
            future = publisher.publish(topic_path, body, params=json.dumps(params))

            try:
                future.result()
            except NotFound:
                publisher.create_topic(name=topic_path)
                subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
                subscription_path = subscriber.subscription_path(project_id, TOPIC)
                subscriber.create_subscription(
                    request={
                        "name": subscription_path,
                        "topic": topic_path,
                        "ack_deadline_seconds": 300,
                        "enable_exactly_once_delivery": True
                    }
                )
                time.sleep(2)
                future = publisher.publish(topic_path, body, params=json.dumps(params))
                future.result()
        else:
            msg = 'incorrect parameter \'action\' [CREATE])'
            return Response(msg, mimetype="text/plain", status=422)

        return Response('ok', mimetype="text/plain", status=200)

    @is_authenticated
    def delete(self):
        """Delete counter register by container and object"""

        params = request.get_json()
        msg = "incorrect parameters ['action', 'kind', 'account', 'container', 'bytes-used', 'counter']"

        if not params and request.data:
            params = json.loads(request.data)

        if not params:
            return Response(msg, mimetype="text/plain", status=422)

        keys = set([*params.keys()])

        if not keys.issubset(KEYS):
            return Response(msg, mimetype="text/plain", status=422)

        action = params.get('action')
        kind = params.get('kind')
        account = params.get('account')
        container = params.get('container')
        bytes_used = params.get('bytes-used')

        if not action or not kind or not account:
            return Response(msg, mimetype="text/plain", status=422)

        if kind == 'container' and (not container or bytes_used == None):
            return Response(msg, mimetype="text/plain", status=422)

        google = Google()
        credentials = google.get_client()
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
        publisher = pubsub_v1.PublisherClient(credentials=credentials)
        topic_path = publisher.topic_path(project_id, TOPIC)

        if action in ['DELETE', 'RESET']:
            body = bytes(action, 'utf-8')
            future = publisher.publish(topic_path, body, params=json.dumps(params))

            try:
                future.result()
            except NotFound:
                publisher.create_topic(name=topic_path)
                subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
                subscription_path = subscriber.subscription_path(project_id, TOPIC)
                subscriber.create_subscription(
                    request={
                        "name": subscription_path,
                        "topic": topic_path,
                        "ack_deadline_seconds": 300,
                        "enable_exactly_once_delivery": True
                    }
                )
                time.sleep(2)
                future = publisher.publish(topic_path, body, params=json.dumps(params))
                future.result()
        else:
            msg = 'incorrect parameter \'action\' [DELETE, RESET])'
            return Response(msg, mimetype="text/plain", status=422)

        return Response('ok', mimetype="text/plain", status=200)
