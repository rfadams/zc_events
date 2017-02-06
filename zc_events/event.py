import logging
import uuid
import zlib
import ujson

from zc_events.exceptions import RequestTimeout, ServiceRequestException
from zc_events.request import wrap_resource_from_response


class Event(object):

    def __init__(self, event_client, event_type, *args, **kwargs):
        self.event_client = event_client
        self.event_type = event_type
        self.args = args
        self.kwargs = kwargs
        self._emit = False
        self._wait = False
        self._complete = False
        self._reponse = None

    def emit(self):
        event_type = self.event_type
        args = self.args
        kwargs = self.kwargs

        return self.event_client.emit_microservice_event(event_type, *args, **kwargs)

    def wait(self):
        raise NotImplementedError("Base Event does not support this method")

    def complete(self):
        raise NotImplementedError("Base Event does not support this method")


class RequestEvent(Event):

    def __init__(self, *args, **kwargs):
        self.response_key = 'request-{}'.format(uuid.uuid4())
        if kwargs.get('response_key'):
            raise AttributeError("kwargs should not include reserved key 'response_key'")

        kwargs['response_key'] = self.response_key

        super(RequestEvent, self).__init__(*args, **kwargs)

    def wait(self):
        if self._wait:
            return self._reponse

        result = self.event_client.wait_for_response(self.response_key)
        if not result:
            raise RequestTimeout

        self._response = ujson.loads(zlib.decompress(result[1]))
        self._wait = True

        return self._response

    def complete(self):
        if not self._wait:
            self._response = self.wait()

        if 400 <= self._response['status'] < 600:
            raise ServiceRequestException(self._response['body'])

        self._complete = True

        return self._response


class ResourceRequestEvent(RequestEvent):

    def complete(self):
        super(ResourceRequestEvent, self).complete()

        wrapped_resource = wrap_resource_from_response(self._response)
        return wrapped_resource
