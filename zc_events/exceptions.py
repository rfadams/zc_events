class RequestTimeout(Exception):
    status_code = 408
    default_detail = 'Request timed out.'
    default_code = 'request_timeout'

    def __init__(self, detail=None):
        if detail is not None:
            self.detail = detail
        else:
            self.detail = self.default_detail

    def __str__(self):
        return self.detail


class EmitEventException(Exception):
    pass


class ServiceRequestException(Exception):
    pass


class RemoteResourceException(Exception):
    pass
