import mock
import pytest

from zc_events.client import structure_response, MethodNotAllowed, EventClient


def test_structure_response():
    import ujson
    import zlib
    status = 200
    body = {'data': {'type': 'User', 'id': '1234'}}

    compressed_response = structure_response(status, body)

    response = ujson.loads(zlib.decompress(compressed_response))

    assert response['status'] == status
    assert response['body'] == body


class TestHandleRequestEvent:

    def setup(self):
        self.event_client = EventClient()
        self.mock_viewset = mock.Mock()

        self.base_event = {
            'event_type': 'order_request',
            'method': 'GET',
            'user_id': '1234',
            'roles': ['service'],
            'id': None,
            'query_string': 'include=order_items',
            'related_resource': None,
            'body': None,
            'response_key': 'ba954cb5-0682-4c3e-ab0d-9cb55e7a279a',
        }

    @mock.patch('zc_events.client.redis.client.StrictRedis.execute_command')
    def test_get_list(self, mock_redis):
        self.event_client.handle_request_event(self.base_event, viewset=self.mock_viewset)
        self.mock_viewset.as_view.assert_called_with({'get': 'list'})

    @mock.patch('zc_events.client.redis.client.StrictRedis.execute_command')
    def test_relationship_view(self, mock_redis):
        self.base_event['id'] = '115'
        self.base_event['relationship'] = 'user'
        self.event_client.handle_request_event(self.base_event, relationship_viewset=self.mock_viewset)
        assert self.mock_viewset.as_view.called

    @mock.patch('zc_events.client.redis.client.StrictRedis.execute_command')
    def test_related_resource_get(self, mock_redis):
        self.base_event['id'] = '115'
        self.base_event['related_resource'] = 'user'
        self.event_client.handle_request_event(self.base_event, viewset=self.mock_viewset)

        self.mock_viewset.as_view.assert_called_with({'get': self.base_event['related_resource']})

    @mock.patch('zc_events.client.redis.client.StrictRedis.execute_command')
    def test_get_detail(self, mock_redis):
        self.base_event['id'] = '115'
        self.event_client.handle_request_event(self.base_event, viewset=self.mock_viewset)

        self.mock_viewset.as_view.assert_called_with({'get': 'retrieve'})

    @mock.patch('zc_events.client.redis.client.StrictRedis.execute_command')
    def test_put(self, mock_redis):
        self.base_event['method'] = 'PUT'
        self.base_event['id'] = '115'
        self.event_client.handle_request_event(self.base_event, viewset=self.mock_viewset)

        self.mock_viewset.as_view.assert_called_with({'put': 'update'})

    @mock.patch('zc_events.client.redis.client.StrictRedis.execute_command')
    def test_patch(self, mock_redis):
        self.base_event['method'] = 'PATCH'
        self.base_event['id'] = '115'
        self.event_client.handle_request_event(self.base_event, viewset=self.mock_viewset)

        self.mock_viewset.as_view.assert_called_with({'patch': 'partial_update'})

    @mock.patch('zc_events.client.redis.client.StrictRedis.execute_command')
    def test_delete(self, mock_redis):
        self.base_event['method'] = 'DELETE'
        self.base_event['id'] = '115'
        self.event_client.handle_request_event(self.base_event, viewset=self.mock_viewset)

        self.mock_viewset.as_view.assert_called_with({'delete': 'destroy'})

    @mock.patch('zc_events.client.redis.client.StrictRedis.execute_command')
    def test_post(self, mock_redis):
        self.base_event['method'] = 'POST'
        self.event_client.handle_request_event(self.base_event, viewset=self.mock_viewset)

        self.mock_viewset.as_view.assert_called_with({'post': 'create'})

    @mock.patch('zc_events.client.redis.client.StrictRedis.execute_command')
    def test_options_detail(self, mock_redis):
        self.base_event['method'] = 'OPTIONS'
        self.base_event['id'] = '115'
        self.event_client.handle_request_event(self.base_event, viewset=self.mock_viewset)

        self.mock_viewset.as_view.assert_called_with({
            'get': 'retrieve',
            'put': 'update',
            'patch': 'partial_update',
            'delete': 'destroy'
        })

    @mock.patch('zc_events.client.redis.client.StrictRedis.execute_command')
    def test_options_list(self, mock_redis):
        self.base_event['method'] = 'OPTIONS'
        self.event_client.handle_request_event(self.base_event, viewset=self.mock_viewset)

        self.mock_viewset.as_view.assert_called_with({
            'get': 'list',
            'post': 'create',
        })

    @mock.patch('zc_events.client.redis.client.StrictRedis.execute_command')
    def test_invalid_method(self, mock_redis):
        self.base_event['method'] = 'FAKE_METHOD'

        with pytest.raises(MethodNotAllowed):
            self.event_client.handle_request_event(self.base_event, viewset=self.mock_viewset)
