import logging
import urllib
import uuid
import zlib

import pika
import pika_pool
import redis
import ujson
from django.conf import settings
from django.http import HttpRequest, QueryDict
from inflection import underscore
from rest_framework.exceptions import MethodNotAllowed

from zc_common.jwt_auth.utils import jwt_encode_handler
from zc_events.exceptions import EmitEventException, RequestTimeout, ServiceRequestException
from zc_events.request import wrap_resource_from_response


logger = logging.getLogger('django')


def structure_response(status, data):
    return zlib.compress(ujson.dumps({
        'status': status,
        'body': data
    }))


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
                'microservice-events',
                '',
                event_body,
                pika.BasicProperties(
                    content_type='application/json',
                    content_encoding='utf-8'
                )
            )

        if not response:
            logger.info(
                'MICROSERVICE_EVENT::EMIT_FAILURE: Failure emitting [{}:{}] event for object ({}:{}) and user {}'.format(
                    event_type, task_id, kwargs.get('resource_type'), kwargs.get('resource_id'), kwargs.get('user_id')))
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
        # Creates appropriate request object
        if 'service' in event['roles']:
            jwt_payload = {'roles': event['roles']}
        else:
            jwt_payload = {'id': event['user_id'], 'roles': event['roles']}

        request = HttpRequest()
        request.GET = QueryDict(event.get('query_string'))
        request.POST = QueryDict(event.get('body'))
        request.encoding = 'utf-8'
        request.method = event['method'].upper()
        request.META = {
            'HTTP_AUTHORIZATION': 'JWT {}'.format(jwt_encode_handler(jwt_payload)),
            'QUERY_STRING': event.get('query_string'),
            'HTTP_HOST': event.get('http_host', 'local.zerocater.com'),
        }

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
        3 second blocking read on Redis to retrieve the result of a request event.
        """
        result = self.redis_client.blpop(response_key, 5)
        if not result:
            raise RequestTimeout

        return ujson.loads(zlib.decompress(result[1]))

    def make_service_request(self, resource_type, resource_id=None, user_id=None, query_string=None, method='GET',
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

        if 400 <= response['status'] < 600:
            error_msg = '{} Error: [{}] request for {}. Error Content: {}'.format(
                response['status'], method, ':'.join([resource_type, str(resource_id), query_string]),
                response['body'])
            raise ServiceRequestException(error_msg)

        return response

    def get_remote_resource(self, resource_type, pk, user_id=None, include=None, page_size=None, related_resource=None):
        """
        Function called by services to make a request to another service for a resource.
        """
        query_string = None
        params = {}
        if include:
            params['include'] = include

        if page_size:
            params['page_size'] = page_size

        if params:
            query_string = urllib.urlencode(params)

        response = self.make_service_request(resource_type, pk, user_id, query_string)
        wrapped_resource = wrap_resource_from_response(response)
        return wrapped_resource
