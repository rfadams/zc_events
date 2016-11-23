import re
from django.conf import settings


class TaskRouter(object):
    def route_for_task(self, task, args=None, kwargs=None):
        if re.match('^microservice.*', task) is not None:
            return {'exchange': 'microservice-events',
                    'exchange_type': 'fanout',
                    'routing_key': ''}
        else:
            return {'exchange': 'default',
                    'exchange_type': 'direct',
                    'routing_key': settings.DEFAULT_QUEUE_NAME}
