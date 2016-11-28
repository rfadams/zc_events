import uuid

from inflection import underscore
import ujson

from zc_events.emit import emit_microservice_event
from zc_events.exceptions import RemoteResourceException


def emit_request_event(event_type, method, user_id, roles, **kwargs):
    """Emit microservice request event."""
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


def _included_to_dict(included):
    data = {}

    if not included:
        return data

    for item in included:
        data[(item['type'], item['id'])] = item

    return data


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

