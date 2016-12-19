from __future__ import division

import logging
import math
import urllib
import uuid
import zlib

import pika
import pika_pool
import redis
import ujson
from django.conf import settings
from inflection import underscore

from zc_events.exceptions import EmitEventException, RequestTimeout, ServiceRequestException
from zc_events.request import wrap_resource_from_response
from zc_events.email import generate_email_data
from zc_events.aws import save_string_contents_to_s3
from zc_events.utils import notification_event_payload
from zc_events.django_request import structure_response, create_django_request_object


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

        self.exchange = settings.EVENTS_EXCHANGE

    def emit_microservice_event(self, event_type, *args, **kwargs):
        task_id = str(uuid.uuid4())

        keyword_args = {'task_id': task_id}
        keyword_args.update(kwargs)

        message = {
            'task': 'microservice.event',
            'id': task_id,
            'args': [event_type] + list(args),
            'kwargs': keyword_args
        }

        event_queue_name = '{}-events'.format(settings.SERVICE_NAME)
        event_body = ujson.dumps(message)

        logger.info('MICROSERVICE_EVENT::EMIT: Emitting [{}:{}] event for object ({}:{}) and user {}'.format(
            event_type, task_id, kwargs.get('resource_type'), kwargs.get('resource_id'),
            kwargs.get('user_id')))

        with self.pika_pool.acquire() as cxn:
            cxn.channel.queue_declare(queue=event_queue_name, durable=True)
            response = cxn.channel.basic_publish(
                self.exchange,
                '',
                event_body,
                pika.BasicProperties(
                    content_type='application/json',
                    content_encoding='utf-8'
                )
            )

        if not response:
            logger.info(
                '''MICROSERVICE_EVENT::EMIT_FAILURE: Failure emitting [{}:{}] event \
                for object ({}:{}) and user {}'''.format(event_type, task_id, kwargs.get('resource_type'),
                                                         kwargs.get('resource_id'), kwargs.get('user_id')))
            raise EmitEventException("Message may have failed to deliver")

        return response

    def emit_request_event(self, event_type, method, user_id, roles, **kwargs):
        """Emit microservice request event."""
        response_key = 'request-{}'.format(uuid.uuid4())

        self.emit_microservice_event(
            event_type,
            method=method,
            user_id=user_id,
            roles=roles,
            response_key=response_key,
            **kwargs
        )

        return response_key

    def handle_request_event(self, event, viewset=None, relationship_viewset=None):
        """
        Method to handle routing request event to appropriate view by constructing
        a request object based on the parameters of the event.
        """
        request = create_django_request_object(event)

        # Call the viewset passing the appropriate params
        if event.get('id') and event.get('relationship'):
            result = relationship_viewset.as_view()(request, pk=event.get('id'),
                                                    related_field=event.get('relationship'))
        elif request.method == 'GET' and event.get('id') and event.get('related_resource'):
            result = viewset.as_view({'get': event.get('related_resource')})(request, pk=event.get('id'))
        elif request.method == 'GET' and event.get('id'):
            result = viewset.as_view({'get': 'retrieve'})(request, pk=event.get('id'))
        elif request.method == 'PUT' and event.get('id'):
            result = viewset.as_view({'put': 'update'})(request, pk=event.get('id'))
        elif request.method == 'PATCH' and event.get('id'):
            result = viewset.as_view({'patch': 'partial_update'})(request, pk=event.get('id'))
        elif request.method == 'DELETE' and event.get('id'):
            result = viewset.as_view({'delete': 'destroy'})(request, pk=event.get('id'))
        elif request.method == 'GET':
            result = viewset.as_view({'get': 'list'})(request)
        elif request.method == 'POST':
            result = viewset.as_view({'post': 'create'})(request)
        elif request.method == 'OPTIONS' and event.get('id'):
            result = viewset.as_view({
                'get': 'retrieve',
                'put': 'update',
                'patch': 'partial_update',
                'delete': 'destroy'
            })(request, pk=event.get('id'))
        elif request.method == 'OPTIONS':
            result = viewset.as_view({
                'get': 'list',
                'post': 'create',
            })(request)
        else:
            raise MethodNotAllowed(request.method)

        # Takes result and drops it into Redis with the key passed in the event
        self.redis_client.rpush(event['response_key'], structure_response(result.status_code, result.rendered_content))
        self.redis_client.expire(event['response_key'], 60)

    def get_request_event_response(self, response_key):
        """
        Blocking read on Redis to retrieve the result of a request event.
        """
        result = self.redis_client.blpop(response_key, 5)
        if not result:
            raise RequestTimeout

        return ujson.loads(zlib.decompress(result[1]))

    def fetch_remote_resource(self, resource_type, resource_id=None, user_id=None, query_string=None, method=None,
                              data=None, related_resource=None):
        """
        Emit a request event on behalf of a service.
        """
        key = self.emit_request_event(
            '{}_request'.format(underscore(resource_type)),
            method,
            user_id,
            ['service'],
            id=resource_id,
            query_string=query_string,
            related_resource=related_resource,
            body=data,
        )
        response = self.get_request_event_response(key)

        return response

    def make_service_request(self, resource_type, resource_id=None, user_id=None, query_string=None, method=None,
                             data=None, related_resource=None):

        response = self.fetch_remote_resource(resource_type, resource_id=resource_id, user_id=user_id,
                                              query_string=query_string, method=method,
                                              data=data, related_resource=related_resource)

        if 400 <= response['status'] < 600:
            error_msg = '{} Error: [{}] request for {}. Error Content: {}'.format(
                response['status'], method, ':'.join([resource_type, str(resource_id), str(query_string)]),
                response['body'])
            raise ServiceRequestException(error_msg)

        return response

    def get_remote_resource(self, resource_type, pk=None, user_id=None, include=None, page_size=None,
                            related_resource=None):
        """
        Function called by services to make a request to another service for a resource.
        """
        query_string = None
        params = {}
        if pk and isinstance(pk, (list, set)):
            params['filter[id__in]'] = ','.join([str(_) for _ in pk])
            pk = None
        if include:
            params['include'] = include

        if page_size:
            params['page_size'] = page_size

        if params:
            query_string = urllib.urlencode(params)

        response = self.make_service_request(resource_type, resource_id=pk,
                                             user_id=user_id, query_string=query_string, method='GET',
                                             related_resource=related_resource)
        wrapped_resource = wrap_resource_from_response(response)
        return wrapped_resource

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

        self.emit_microservice_event('send_email', **event_data)

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
