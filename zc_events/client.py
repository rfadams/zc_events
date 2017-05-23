from __future__ import division

import logging
import math
import ujson
import urllib
import uuid

import pika
import pika_pool
import redis
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from inflection import underscore

from zc_events.aws import save_string_contents_to_s3
from zc_events.django_request import structure_response, create_django_request_object
from zc_events.email import generate_email_data
from zc_events.event import ResourceRequestEvent
from zc_events.exceptions import EmitEventException
from zc_events.utils import notification_event_payload

SERVICE_ACTOR = 'service'
ANONYMOUS_ACTOR = 'anonymous'

SERVICE_ROLES = [SERVICE_ACTOR]
ANONYMOUS_ROLES = [ANONYMOUS_ACTOR]

logger = logging.getLogger('django')


class MethodNotAllowed(Exception):
    status_code = 405
    default_detail = 'Method "{method}" not allowed.'

    def __init__(self, method, detail=None):
        if detail is not None:
            self.detail = detail
        else:
            self.detail = self.default_detail.format(method=method)

    def __str__(self):
        return self.detail


class EventClient(object):

    def __init__(self):
        pool = redis.ConnectionPool().from_url(settings.REDIS_URL, db=0)
        self.redis_client = redis.Redis(connection_pool=pool)

        pika_params = pika.URLParameters(settings.BROKER_URL)
        pika_params.socket_timeout = 5
        self.pika_pool = pika_pool.QueuedPool(
            create=lambda: pika.BlockingConnection(parameters=pika_params),
            max_size=10,
            max_overflow=10,
            timeout=10,
            recycle=3600,
            stale=45,
        )

        self.events_exchange = settings.EVENTS_EXCHANGE
        self.notifications_exchange = getattr(settings, 'NOTIFICATIONS_EXCHANGE', None)

    def emit_microservice_message(self, exchange, routing_key, event_type, priority=0, *args, **kwargs):
        task_id = str(uuid.uuid4())

        keyword_args = {'task_id': task_id}
        keyword_args.update(kwargs)

        task = 'microservice.notification' if routing_key else 'microservice.event'

        message = {
            'task': task,
            'id': task_id,
            'args': [event_type] + list(args),
            'kwargs': keyword_args
        }

        event_queue_name = '{}-events'.format(settings.SERVICE_NAME)
        event_body = ujson.dumps(message)

        logger.info('{}::EMIT: Emitting [{}:{}] event for object ({}:{}) and user {}'.format(
            exchange.upper(), event_type, task_id, kwargs.get('resource_type'), kwargs.get('resource_id'),
            kwargs.get('user_id')))

        queue_arguments = {
            'x-max-priority': 10
        }
        with self.pika_pool.acquire() as cxn:
            cxn.channel.queue_declare(queue=event_queue_name, durable=True, arguments=queue_arguments)
            response = cxn.channel.basic_publish(
                exchange,
                routing_key,
                event_body,
                pika.BasicProperties(
                    content_type='application/json',
                    content_encoding='utf-8',
                    priority=priority
                )
            )

        if not response:
            logger.info(
                '''{}::EMIT_FAILURE: Failure emitting [{}:{}] event for object ({}:{}) and user {}'''.format(
                    exchange.upper(), event_type, task_id, kwargs.get('resource_type'),
                    kwargs.get('resource_id'), kwargs.get('user_id')))
            raise EmitEventException("Message may have failed to deliver")

        return response

    def emit_microservice_event(self, event_type, *args, **kwargs):
        return self.emit_microservice_message(self.events_exchange, '', event_type, *args, **kwargs)

    def emit_microservice_email_notification(self, event_type, *args, **kwargs):
        return self.emit_microservice_message(
            self.notifications_exchange, 'microservice.notification.email', event_type, *args, **kwargs)

    def emit_microservice_text_notification(self, event_type, *args, **kwargs):
        return self.emit_microservice_message(
            self.notifications_exchange, 'microservice.notification.text', event_type, *args, **kwargs)

    def wait_for_response(self, response_key):
        response = self.redis_client.blpop(response_key, 60)
        return response

    def _get_handler_for_viewset(self, viewset, is_detail):
        if is_detail:
            methods = [
                ('get', 'retrieve'),
                ('put', 'update'),
                ('patch', 'partial_update'),
                ('delete', 'destroy'),
            ]
        else:
            methods = [
                ('get', 'list'),
                ('post', 'create'),
            ]
        actions = {}
        for method, action in methods:
            if hasattr(viewset, action):
                actions[method] = action

        return viewset.as_view(actions)

    def handle_request_event(self, event, view=None, viewset=None, relationship_viewset=None):
        """
        Method to handle routing request event to appropriate view by constructing
        a request object based on the parameters of the event.
        """
        request = create_django_request_object(
            roles=event.get('roles'),
            query_string=event.get('query_string'),
            method=event.get('method'),
            user_id=event.get('user_id', None),
            body=event.get('body', None),
            http_host=event.get('http_host', None)
        )

        if not any([view, viewset, relationship_viewset]):
            raise ImproperlyConfigured('handle_request_event must be passed either a view or viewset')

        response_key = event.get('response_key')
        pk = event.get('pk', None)
        relationship = event.get('relationship', None)
        related_resource = event.get('related_resource', None)

        handler_kwargs = {}
        if view:
            handler = view.as_view()
        elif pk:
            handler_kwargs['pk'] = pk
            if relationship:
                # Relationship views expect this kwarg as 'related_field'. See https://goo.gl/WW4ePd
                handler_kwargs['related_field'] = relationship
                handler = relationship_viewset.as_view()
            elif related_resource:
                handler = viewset.as_view({'get': related_resource})
                handler_kwargs['related_resource'] = related_resource
            else:
                handler = self._get_handler_for_viewset(viewset, is_detail=True)
        else:
            handler = self._get_handler_for_viewset(viewset, is_detail=False)

        result = handler(request, **handler_kwargs)

        # Takes result and drops it into Redis with the key passed in the event
        self.redis_client.rpush(response_key, structure_response(result.status_code, result.rendered_content))
        self.redis_client.expire(response_key, 60)

    def async_resource_request(self, resource_type, resource_id=None, user_id=None, query_string=None, method=None,
                               data=None, related_resource=None, roles=None, priority=5):

        roles = roles or ANONYMOUS_ROLES

        event = ResourceRequestEvent(
            self,
            '{}_request'.format(underscore(resource_type)),
            method=method,
            user_id=user_id,
            roles=roles,
            pk=resource_id,
            query_string=query_string,
            related_resource=related_resource,
            body=data,
            priority=priority
        )

        event.emit()

        return event

    def make_service_request(self, resource_type, resource_id=None, user_id=None, query_string=None, method=None,
                             data=None, related_resource=None):

        roles = SERVICE_ROLES
        event = self.async_resource_request(resource_type, resource_id=resource_id, user_id=user_id,
                                            query_string=query_string, method=method,
                                            data=data, related_resource=related_resource, roles=roles)
        return event.wait()

    def get_remote_resource_async(self, resource_type, pk=None, user_id=None, include=None, page_size=None,
                                  related_resource=None, query_params=None, roles=None, priority=None):
        """
        Function called by services to make a request to another service for a resource.
        """
        query_string = None
        params = query_params or {}
        method = 'GET'

        if pk and isinstance(pk, (list, set)):
            params['filter[id__in]'] = ','.join([str(_) for _ in pk])
            pk = None
        if include:
            params['include'] = include

        if page_size:
            params['page_size'] = page_size

        if params:
            query_string = urllib.urlencode(params)

        event = self.async_resource_request(resource_type, resource_id=pk, user_id=user_id,
                                            query_string=query_string, method=method,
                                            related_resource=related_resource, roles=roles, priority=priority)

        return event

    def get_remote_resource(self, resource_type, pk=None, user_id=None, include=None, page_size=None,
                            related_resource=None, query_params=None, roles=None):

        event = self.get_remote_resource_async(resource_type, pk=pk, user_id=user_id, include=include,
                                               page_size=page_size, related_resource=related_resource,
                                               query_params=query_params, roles=roles)

        wrapped_resource = event.complete()
        return wrapped_resource

    def get_remote_resource_data(self, resource_type, pk=None, user_id=None, include=None, page_size=None,
                                 related_resource=None, query_params=None, roles=None):

        priority = 9
        event = self.get_remote_resource_async(resource_type, pk=pk, user_id=user_id, include=include,
                                               page_size=page_size, related_resource=related_resource,
                                               query_params=query_params, roles=roles, priority=priority)
        data = event.wait()
        return data

    def send_email(self, *args, **kwargs):

        email_uuid = uuid.uuid4()

        to = kwargs.get('to')
        from_email = kwargs.get('from_email')
        attachments = kwargs.get('attachments')
        files = kwargs.get('files')

        if logger:
            msg = '''MICROSERVICE_SEND_EMAIL: Upload email with UUID {}, to {}, from {},
            with attachments {} and files {}'''
            logger.info(msg.format(email_uuid, to, from_email, attachments, files))

        event_data = generate_email_data(email_uuid, *args, **kwargs)

        if logger:
            logger.info('MICROSERVICE_SEND_EMAIL: Sent email with UUID {} and data {}'.format(
                email_uuid, event_data
            ))

        self.emit_microservice_email_notification('send_email', **event_data)

    def emit_index_rebuild_event(self, event_name, resource_type, model, batch_size, serializer, queryset=None):
        """
        A special helper method to emit events related to index_rebuilding.
        Note: AWS_INDEXER_BUCKET_NAME must be present in your settings.

        We loop over the table and each turn, we take `batch_size` objects and emit an event for them.
        """

        if queryset is None:
            queryset = model.objects.all()

        objects_count = queryset.count()
        total_events_count = int(math.ceil(objects_count / batch_size))
        emitted_events_count = 0

        while emitted_events_count < total_events_count:
            start_index = emitted_events_count * batch_size
            end_index = start_index + batch_size
            data = []

            for instance in queryset.order_by('id')[start_index:end_index]:
                instance_data = serializer(instance)
                data.append(instance_data)

            stringified_data = ujson.dumps(data)
            filename = save_string_contents_to_s3(stringified_data, settings.AWS_INDEXER_BUCKET_NAME)
            payload = notification_event_payload(resource_type=resource_type, resource_id=None, user_id=None,
                                                 meta={'s3_key': filename})

            self.emit_microservice_event(event_name, **payload)
            emitted_events_count += 1
