from flask import Blueprint
from flask_restplus import Api


blueprint = Blueprint('APIv1', __name__, url_prefix='/v1')
api = Api(blueprint, version='1.0', title='Swift-Cloud Tools',
          description='Swift-Cloud Tools')
