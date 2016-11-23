import ujson
import urllib

from rest_framework.status import HTTP_408_REQUEST_TIMEOUT
from rest_framework.exceptions import APIException
from inflection import underscore

from zc_events.emit import emit_microservice_event


class RequestTimeout(APIException):
    status_code = HTTP_408_REQUEST_TIMEOUT
    default_detail = 'Request timed out.'
    default_code = 'request_timeout'


def emit_request_event(event_type, method, user_id, roles, **kwargs):
    import uuid
    response_key = 'request-{}'.format(uuid.uuid4())

    emit_microservice_event(
        event_type,
        method=method,
        user_id=user_id,
        roles=roles,
        response_key=response_key,
        **kwargs
    )

    return response_key


def get_request_event_response(response_key):
    result = redis_client.blpop(response_key, 3)
    if not result:
        raise RequestTimeout

    return ujson.loads(result[1])


# Requests that can be made to another service
GET = 'get'
POST = 'post'
PUT = 'put'
PATCH = 'patch'


class UnsupportedHTTPMethodException(Exception):
    pass


class RouteNotFoundException(Exception):
    pass


class ServiceRequestException(Exception):
    pass


class RemoteResourceException(Exception):
    pass


class RemoteResourceWrapper(object):

    def __init__(self, data, included=None):
        result = self._get_from_include(included, data)
        self.data = result if result else data
        self.create_properties_from_data(included)

    def _get_from_include(self, included, obj):
        if included:
            res = included.get((obj['type'], obj['id']))
            return res
        return None

    def create_properties_from_data(self, included):
        accepted_keys = ('id', 'type', 'self', 'related')

        for key in self.data.keys():
            if key in accepted_keys:
                setattr(self, key, self.data.get(key))

        if 'attributes' in self.data:
            attributes = self.data['attributes']
            for key in attributes.keys():
                setattr(self, underscore(key), attributes[key])

        if 'relationships' in self.data:
            relationships = self.data['relationships']

            for key in relationships.keys():
                if isinstance(relationships[key]['data'], list):
                    setattr(self, underscore(key), RemoteResourceListWrapper(relationships[key]['data'], included))
                else:
                    got = None
                    if included:
                        got = self._get_from_include(included, relationships[key]['data'])

                    if got:
                        setattr(self, underscore(key), RemoteResourceWrapper(got, included))
                    else:
                        setattr(self, underscore(key), RemoteResourceWrapper(relationships[key]['data'], included))

                if 'links' in relationships[key]:
                    setattr(getattr(self, underscore(key)), 'links',
                            RemoteResourceWrapper(relationships[key]['links'], None))


class RemoteResourceListWrapper(list):

    def __init__(self, seq, included=None):
        super(RemoteResourceListWrapper, self).__init__()
        self.data = seq
        self.add_items_from_data(included)

    def add_items_from_data(self, included):
        map(lambda x: self.append(RemoteResourceWrapper(x, included)), self.data)


def wrap_resource_from_response(response):
    json_response = ujson.loads(response['body'])

    if 'data' not in json_response:
        msg = 'Error retrieving resource. Url: {0}. Content: {1}'.format(response.request.url, response.content)
        raise RemoteResourceException(msg)

    resource_data = json_response['data']
    included_raw = json_response.get('included')
    included_data = _included_to_dict(included_raw)
    if isinstance(resource_data, list):
        return RemoteResourceListWrapper(resource_data, included_data)
    return RemoteResourceWrapper(resource_data, included_data)


def _included_to_dict(included):
    data = {}

    if not included:
        return data

    for item in included:
        data[(item['type'], item['id'])] = item

    return data


def make_service_request(resource_type, resource_id=None, user_id=None, query_string=None, method='GET', data=None):
    key = emit_request_event(
        '{}_request'.format(resource_type.lower()),
        method,
        user_id,
        ['service'],
        id=resource_id,
        query_string=query_string,
        body=data,
    )
    response = get_request_event_response(key)

    if 400 <= response['status'] < 600:
        error_msg = '{} Error: [{}] request for {}. Error Content: {}'.format(
            response['status'], method, ':'.join([resource_type, str(resource_id), query_string]), response['body'])
        raise ServiceRequestException(error_msg)

    return response


# Also...include user_id? !!
def get_remote_resource(resource_type, pk, user_id=None, include=None, page_size=None):
    query_string = None
    params = {}
    if include:
        params['include'] = include

    if page_size:
        params['page_size'] = page_size

    if params:
        query_string = urllib.urlencode(params)

    response = make_service_request(resource_type, pk, user_id, query_string)
    wrapped_resource = wrap_resource_from_response(response)
    return wrapped_resource
