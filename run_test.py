# encoding: utf-8
import logging

from logging.handlers import RotatingFileHandler
from os import environ
from swift_cloud_tools import create_app


if __name__ == '__main__':
    application = create_app('config/testing_config.py')
    handler = RotatingFileHandler('swift-cloud-tools.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.DEBUG)
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(threadName)s %(levelname)s %(message)s'
    )
    application.logger.addHandler(handler)
    application.run('0.0.0.0', int(environ.get('PORT', '5000')), threaded=True)
