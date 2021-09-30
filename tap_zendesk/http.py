from time import sleep
import backoff
import requests
import singer


LOGGER = singer.get_logger()


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

class ZendeskConflictError(ZendeskError):
    pass

class ZendeskUnprocessableEntityError(ZendeskError):
    pass

class ZendeskRateLimitError(ZendeskBackoffError):
    pass

class ZendeskInternalServerError(ZendeskError):
    pass

class ZendeskNotImplementedError(ZendeskError):
    pass

class ZendeskBadGatewayError(ZendeskError):
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
        "message": "There is no help desk configured at this address. This means that the address is available and that you can claim it at http://www.zendesk.com/signup"
    },
    409: {
        "raise_exception": ZendeskConflictError,
        "message": "The request does not match our state in some way."
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
def is_fatal(exception):
    status_code = exception.response.status_code

    if status_code == 429:
        sleep_time = int(exception.response.headers['Retry-After'])
        LOGGER.info("Caught HTTP 429, retrying request in %s seconds", sleep_time)
        sleep(sleep_time)
        return False
    elif status_code == 503:
        sleep_time = int(exception.response.headers['Retry-After'])
        LOGGER.info("Caught HTTP 503, retrying request in %s seconds", sleep_time)
        sleep(sleep_time)
        return False
    return 400 <=status_code < 500

def raise_for_error(response):
    LOGGER.error('ERROR %s: %s, REASON: %s', response.status_code,
                                             response.text,
                                             response.reason)
    try:
        response_json = response.json()
    except Exception: # pylint: disable=broad-except
        response_json = {}
    if response.status_code != 200:
        if response_json.get('error'):
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('error')}"
        else:
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('message', ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get('message', 'Unknown Error'))}"
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(
            response.status_code, {}).get("raise_exception", ZendeskError)
        raise exc(message, response) from None

@backoff.on_exception(backoff.expo,
                      ZendeskBackoffError,
                      max_tries=10,
                      giveup=is_fatal)
def call_api(url, params, headers):
    response = requests.get(url, params=params, headers=headers)
    raise_for_error(response)
    return response



def get_cursor_based(url, access_token, cursor=None, **kwargs):
    # something like this
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Bearer {}'.format(access_token),
        **kwargs.get('headers', {})
    }

    params = {
        'page[size]': 100,
        **kwargs.get('params', {})
    }

    if cursor:
        params['page[after]'] = cursor

    response = call_api(url, params=params, headers=headers)
    response_json = response.json()

    yield response_json

    has_more = response_json['meta']['has_more']

    while has_more:
        cursor = response_json['meta']['after_cursor']
        params['page[after]'] = cursor

        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        response_json = response.json()

        yield response_json
        has_more = response_json['meta']['has_more']
