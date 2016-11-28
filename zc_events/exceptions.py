from rest_framework.status import HTTP_408_REQUEST_TIMEOUT
from rest_framework.exceptions import APIException


class RequestTimeout(APIException):
    status_code = HTTP_408_REQUEST_TIMEOUT
    default_detail = 'Request timed out.'
    default_code = 'request_timeout'


class EmitEventException(Exception):
    pass


class ServiceRequestException(Exception):
    pass


class RemoteResourceException(Exception):
    pass
