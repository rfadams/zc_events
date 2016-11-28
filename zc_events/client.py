import urllib

from django.http import HttpRequest, QueryDict
import redis
import ujson

from zc_common.jwt_auth.utils import jwt_encode_handler
from zc_events.exceptions import RequestTimeout, ServiceRequestException
from zc_events.request import emit_request_event, wrap_resource_from_response


def structure_response(status, data):
    return ujson.dumps({
        'status': status,
        'body': data
    })


class MethodNotAllowed(Exception):
    status_code = 405
    default_detail = _('Method "{method}" not allowed.')

    def __init__(self, method, detail=None):
        if detail is not None:
            self.detail = force_text(detail)
        else:
            self.detail = force_text(self.default_detail).format(method=method)

    def __str__(self):
        return self.detail


class EventClient(object):

    def __init__(self, redis_url):
        pool = redis.ConnectionPool().from_url(redis_url, db=0)
        self.redis_client = redis.Redis(connection_pool=pool)

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
            'HTTP_HOST': event.get('http_host', 'LOCAL_SERVER'),
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
        result = self.redis_client.blpop(response_key, 3)
        if not result:
            raise RequestTimeout

        return ujson.loads(result[1])

    def make_service_request(self, resource_type, resource_id=None, user_id=None, query_string=None, method='GET',
                             data=None):
        """
        Emit a request event on behalf of a service.
        """
        key = emit_request_event(
            '{}_request'.format(resource_type.lower()),
            method,
            user_id,
            ['service'],
            id=resource_id,
            query_string=query_string,
            body=data,
        )
        response = self.get_request_event_response(key)

        if 400 <= response['status'] < 600:
            error_msg = '{} Error: [{}] request for {}. Error Content: {}'.format(
                response['status'], method, ':'.join([resource_type, str(resource_id), query_string]),
                response['body'])
            raise ServiceRequestException(error_msg)

        return response

    def get_remote_resource(self, resource_type, pk, user_id=None, include=None, page_size=None):
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
