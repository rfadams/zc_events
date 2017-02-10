import re
from django.conf import settings


class TaskRouter(object):

    def route_for_task(self, task, args=None, kwargs=None):
        if re.match('^microservice.event', task) is not None:
            return {'exchange': settings.EVENTS_EXCHANGE,
                    'exchange_type': 'fanout',
                    'routing_key': ''}
        elif re.match('^microservice.notification', task) is not None:
            return {'exchange': settings.NOTIFICATIONS_EXCHANGE,
                    'exchange_type': 'topic',
                    'routing_key': 'microservice.notification.*'}
        else:
            return {'exchange': 'default',
                    'exchange_type': 'direct',
                    'routing_key': settings.DEFAULT_QUEUE_NAME}
