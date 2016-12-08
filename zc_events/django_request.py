import ujson
import zlib

try:
    from zc_common.jwt_auth.utils import jwt_encode_handler
except ImportError:
    pass

from django.http import HttpRequest, QueryDict


def structure_response(status, data):
    """
    Compress a JSON object with zlib for inserting into redis.
    """
    return zlib.compress(ujson.dumps({
        'status': status,
        'body': data
    }))


def create_django_request_object(event):
    """
    Create a Django HTTPRequest object with the appropriate attributes pulled
    from the event.
    """
    if 'service' in event['roles']:
        jwt_payload = {'roles': event['roles']}
    else:
        jwt_payload = {'id': event['user_id'], 'roles': event['roles']}

    request = HttpRequest()
    request.GET = QueryDict(event.get('query_string'))
    if event.get('body'):
        request.read = lambda: ujson.dumps(event.get('body'))

    request.encoding = 'utf-8'
    request.method = event['method'].upper()
    request.META = {
        'HTTP_AUTHORIZATION': 'JWT {}'.format(jwt_encode_handler(jwt_payload)),
        'QUERY_STRING': event.get('query_string'),
        'HTTP_HOST': event.get('http_host', 'local.zerocater.com'),
        'CONTENT_TYPE': 'application/vnd.api+json',
        'CONTENT_LENGTH': '99999',
    }

    return request
