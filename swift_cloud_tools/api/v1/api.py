__all__ = ['api', 'blueprint']
from swift_cloud_tools.api.v1 import api
from swift_cloud_tools.api.v1 import blueprint
from swift_cloud_tools.api.v1.healthcheck import ns as healthcheck_ns
from swift_cloud_tools.api.v1.expirer import ns as expirer_ns

api.add_namespace(healthcheck_ns)
api.add_namespace(expirer_ns)
