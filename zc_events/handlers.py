import ujson
from django.http import HttpRequest, QueryDict
from rest_framework.exceptions import MethodNotAllowed

from zc_common.jwt_auth.utils import jwt_encode_handler
from slots_and_orders.redis_config import redis_client  # Needs to change


def structure_response(status, data):
    return ujson.dumps({
        'status': status,
        'body': data
    })


def handle_request_event(event, viewset=None, relationship_viewset=None):
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
    request.method = event['method']
    request.META = {
        'HTTP_AUTHORIZATION': 'JWT {}'.format(jwt_encode_handler(jwt_payload)),
        'QUERY_STRING': event.get('query_string'),
        'HTTP_HOST': event.get('http_host', 'TEST_SERVER'),
    }

    # Call the viewset passing the appropriate params
    if event.get('id') and event.get('relationship'):
        result = relationship_viewset.as_view()(request, pk=event.get('id'), related_field=event.get('relationship'))
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
    redis_client.rpush(event['response_key'], structure_response(result.status_code, result.rendered_content))
    redis_client.expire(event['response_key'], 60)
