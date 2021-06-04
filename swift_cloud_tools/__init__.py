# encoding: utf-8
import logging

from os import environ
from flask import Flask
from flask_cors import CORS
from flask_migrate import Migrate
from logging.handlers import RotatingFileHandler

from swift_cloud_tools.models import db
from swift_cloud_tools.api.v1.api import blueprint as api_v1


def create_app(config_module=None):
    app = Flask(__name__)
    app.secret_key = environ.get('SECRET_KEY')
    if config_module:
        config_file = config_module
    elif environ.get('FLASK_CONFIG'):
        config_file = 'config/' + environ.get('FLASK_CONFIG') + '_config.py'
    else:
        config_file = 'config/development_config.py'
    app.config.from_pyfile(config_file)

    db.init_app(app)
    migrate = Migrate(app, db)

    CORS(app, resources={r"/v1/*": {"origins": "*"}})

    app.register_blueprint(api_v1)

    handler = RotatingFileHandler('swift-cloud-tools.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.DEBUG)
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(threadName)s %(levelname)s %(message)s'
    )
    app.logger.addHandler(handler)

    return app

def app_factory(global_config, **local_conf):
	wsgi_app = create_app()
	return wsgi_app
