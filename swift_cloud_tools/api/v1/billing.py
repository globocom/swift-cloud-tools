# -*- coding: utf-8 -*-
from flask import Response
from flask_restplus import Resource
from flask import current_app as app

from swift_cloud_tools.server.utils import Google
from swift_cloud_tools.api.v1 import api

ns = api.namespace('billing', description='Billing')


@ns.route('/sku_price_from_service/service/<string:service>/sku/<string:sku>/amount/<string:amount>')
class BillingSkuPriceFromService(Resource):

    def get(self, service, sku, amount):
        google = Google()

        if not service or not sku:
            app.logger.error('[API] {} GET Billing invalid parameters'.format(422))
            return Response('Invalid parameters', mimetype="text/plain", status=422)

        try:
            price = google.get_sku_price_from_service(service, sku, amount)
        except Exception as err:
            app.logger.error('[API] {} GET Billing sku price from service: {}'.format(500, err))
            return Response(err, mimetype="text/plain", status=500)

        app.logger.error('[API] {} GET Billing sku price from service: {}'.format(200, price))
        return Response(str(price), mimetype="text/plain", status=200)
