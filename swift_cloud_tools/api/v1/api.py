__all__ = ['api', 'blueprint']
from swift_cloud_tools.api.v1 import api
from swift_cloud_tools.api.v1 import blueprint
from swift_cloud_tools.api.v1.healthcheck import ns as healthcheck_ns
from swift_cloud_tools.api.v1.counter import ns as counter_ns
from swift_cloud_tools.api.v1.expirer import ns as expirer_ns
from swift_cloud_tools.api.v1.transfer import ns as transfer_ns
from swift_cloud_tools.api.v1.billing import ns as billing_ns
# from swift_cloud_tools.api.v1.container_info import ns as container_info_ns

api.add_namespace(healthcheck_ns)
api.add_namespace(counter_ns)
api.add_namespace(expirer_ns)
api.add_namespace(transfer_ns)
api.add_namespace(billing_ns)
# api.add_namespace(container_info_ns)
