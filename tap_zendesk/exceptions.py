class ZendeskError(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response

class ZendeskBackoffError(ZendeskError):
    pass

class ZendeskBadRequestError(ZendeskError):
    pass

class ZendeskUnauthorizedError(ZendeskError):
    pass

class ZendeskForbiddenError(ZendeskError):
    pass

class ZendeskNotFoundError(ZendeskError):
    pass

class ZendeskConflictError(ZendeskBackoffError):
    pass

class ZendeskUnprocessableEntityError(ZendeskError):
    pass

class ZendeskRateLimitError(ZendeskBackoffError):
    pass

class ZendeskInternalServerError(ZendeskBackoffError):
    pass

class ZendeskNotImplementedError(ZendeskBackoffError):
    pass

class ZendeskBadGatewayError(ZendeskBackoffError):
    pass

class ZendeskServiceUnavailableError(ZendeskBackoffError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": ZendeskBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": ZendeskUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": ZendeskForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": ZendeskNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": ZendeskConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": ZendeskUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": ZendeskRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": ZendeskInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": ZendeskNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": ZendeskBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": ZendeskServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}
