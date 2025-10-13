from time import sleep
from asyncio import sleep as async_sleep
import backoff
import requests
import singer
from requests.exceptions import Timeout, HTTPError, ChunkedEncodingError, ConnectionError
from aiohttp import ContentTypeError
from urllib3.exceptions import ProtocolError
from tap_zendesk.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    ZendeskError,
    ZendeskBackoffError
)


LOGGER = singer.get_logger()
# Default wait time for 429 and 5xx error
DEFAULT_WAIT = 60
# Default wait time for backoff for conflict error
DEFAULT_WAIT_FOR_CONFLICT_ERROR = 10

BASE_URL = "https://{subdomain}.zendesk.com/api/v2/"

def is_fatal(exception):
    status_code = exception.response.status_code

    if status_code == 429:
        sleep_time = int(exception.response.headers['Retry-After'])
        LOGGER.info("Caught HTTP 429, retrying request in %s seconds", sleep_time)
        sleep(sleep_time)
        return False

    if status_code == 409:
        # retry ZendeskConflictError for at-least 10 times
        return False

    return 400 <=status_code < 500

def raise_for_error(response):
    """ Error handling method which throws custom error. Class for each error defined above which extends `ZendeskError`.
    This method map the status code with `ERROR_CODE_EXCEPTION_MAPPING` dictionary and accordingly raise the error.
    If status_code is 200 then simply return json response.
    """
    try:
        response_json = response.json()
    except Exception: # pylint: disable=broad-except
        response_json = {}
    if response.status_code != 200:
        if response_json.get('error'):
            message = "HTTP-error-code: {}, Error: {}".format(response.status_code, response_json.get('error'))
        else:
            message = "HTTP-error-code: {}, Error: {}".format(
                response.status_code,
                response_json.get("message", ERROR_CODE_EXCEPTION_MAPPING.get(
                    response.status_code, {}).get("message", "Unknown Error")))
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(
            response.status_code, {}).get("raise_exception", ZendeskError)
        raise exc(message, response) from None

def build_headers(access_token: str, additional_headers: dict = None) -> dict:
    """
    Build standard headers for API requests with optional overrides.
    """
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    if additional_headers:
        headers.update(additional_headers)

    return headers

@backoff.on_exception(backoff.expo,
                      (HTTPError, ZendeskError), # Added support of backoff for all unhandled status codes.
                      max_tries=10,
                      giveup=is_fatal)
@backoff.on_exception(backoff.expo,
                    (ConnectionError, ConnectionResetError, Timeout, ChunkedEncodingError, ProtocolError),#As ConnectionError error and timeout error does not have attribute status_code,
                    max_tries=5, # here we added another backoff expression.
                    factor=2)
def call_api(url, request_timeout, params, headers):
    response = requests.get(url, params=params, headers=headers, timeout=request_timeout) # Pass request timeout
    raise_for_error(response)
    return response

def get_cursor_based(url, access_token, request_timeout, page_size, cursor=None, **kwargs):
    """
    Cursor-based pagination generator.
    Yields:
        dict: Response JSON for each page.
    """
    custom_headers = kwargs.pop('headers', {})
    query_params = kwargs.pop('params', {})

    headers = build_headers(access_token=access_token, additional_headers=custom_headers)
    params = {
        'page[size]': page_size,
        **query_params
    }

    if cursor:
        params['page[after]'] = cursor

    while True:
        response = call_api(
            url,
            request_timeout,
            params=params,
            headers=headers
        )
        response_json = response.json()

        yield response_json

        meta = response_json.get('meta', {})
        if not meta.get('has_more'):
            break

        params['page[after]'] = meta.get('after_cursor')

def get_offset_based(url, access_token, request_timeout, page_size, **kwargs):
    """
    Offset-based pagination generator.
    Yields:
        dict: Parsed JSON response from each page.
    """
    custom_headers = kwargs.pop('headers', {})
    query_params = kwargs.pop('params', {})

    headers = build_headers(access_token=access_token, additional_headers=custom_headers)
    next_url = url
    params = {
        "per_page": page_size,
        **query_params
    }

    while next_url:
        response = call_api(
            next_url,
            request_timeout,
            params=params if next_url == url else None,
            headers=headers
        )

        response_json = response.json()
        yield response_json

        next_url = response_json.get('next_page') or response_json.get('after_url')
        params = None

async def raise_for_error_for_async(response):
    """
    Error handling method which throws custom error. Class for each error defined above which extends `ZendeskError`.
    """
    try:
        response_json = await response.json()
    except (ContentTypeError, ValueError):
        # Invalid JSON response
        response_json = {}

    if response.status == 200:
        return response_json
    elif response.status == 429 or response.status >= 500:
        # Get the 'Retry-After' header value, defaulting to 60 seconds if not present.
        retry_after = int(response.headers.get("Retry-After", "0")) or DEFAULT_WAIT
        LOGGER.warning("Caught HTTP %s, retrying request in %s seconds", response.status, retry_after)
        # Wait for the specified time before retrying the request.
        await async_sleep(int(retry_after))
    elif response.status == 409:
        LOGGER.warning(
            "Caught HTTP 409, retrying request in %s seconds",
            DEFAULT_WAIT_FOR_CONFLICT_ERROR,
        )
        # Wait for the specified time before retrying the request.
        await async_sleep(DEFAULT_WAIT_FOR_CONFLICT_ERROR)

    # Prepare the error message and raise the appropriate exception.
    if response_json.get("error"):
        message = "HTTP-error-code: {}, Error: {}".format(
            response.status, response_json.get("error")
        )
    else:
        message = "HTTP-error-code: {}, Error: {}".format(
            response.status,
            response_json.get(
                "message",
                ERROR_CODE_EXCEPTION_MAPPING.get(response.status, {}).get(
                    "message", "Unknown Error"
                ),
            ),
        )

    DEFAULT_ERROR_OBJECT = ZendeskError if response.status < 500 else ZendeskBackoffError
    exc = ERROR_CODE_EXCEPTION_MAPPING.get(response.status, {}).get(
        "raise_exception", DEFAULT_ERROR_OBJECT
    )
    raise exc(message, response) from None


@backoff.on_exception(
    backoff.constant,
    ZendeskBackoffError,
    max_tries=5,
    interval=0
)
@backoff.on_exception(
    backoff.expo,
    (
        ConnectionError,
        ConnectionResetError,
        Timeout,
        ChunkedEncodingError,
        ProtocolError,
    ),
    max_tries=5,
    factor=2,
)
async def call_api_async(session, url, request_timeout, params, headers):
    """
    Perform an asynchronous GET request
    """
    async with session.get(
        url, params=params, headers=headers, timeout=request_timeout
    ) as response:
        response_json = await raise_for_error_for_async(response)

        return response_json

async def paginate_ticket_audits(session, url, access_token, request_timeout, page_size, **kwargs):
    """
    Paginate through the ticket audits API endpoint and return the aggregated results
    """
    custom_headers = kwargs.pop('headers', {})
    query_params = kwargs.pop('params', {})

    headers = build_headers(access_token=access_token, additional_headers=custom_headers)
    params = {
        'per_page': page_size,
        **query_params
    }

    # Make the initial asynchronous API call
    final_response = await call_api_async(session, url, request_timeout, params=params, headers=headers)

    next_url = final_response.get('next_page')

    # Fetch next pages of results.
    while next_url:

        # An asynchronous API call to fetch the next page of results.
        response = await call_api_async(session, next_url, request_timeout, params=None, headers=headers)

        # Extend the final response with the audits from the current page.
        final_response["audits"].extend(response["audits"])

        # Get the URL for the next page
        next_url = response.get('next_page')

    # Return the final aggregated response
    return final_response

def get_incremental_export(url, access_token, request_timeout, start_time, side_load):
    """
    Generator to handle Zendesk's incremental export API.
    """
    headers = build_headers(access_token=access_token)

    if not isinstance(start_time, int):
        start_time = int(start_time.timestamp())

    params = {
        'start_time': start_time,
        'include': side_load
    }

    while True:
        response = call_api(url, request_timeout, params=params, headers=headers)
        response_json = response.json()
        yield response_json

        if response_json.get('end_of_stream'):
            break

        cursor = response_json.get('after_cursor')
        if not cursor:
            raise ValueError("Missing 'after_cursor' in response during pagination.")

        params = {
            'cursor': cursor,
            'include': side_load
        }

def get_incremental_export_offset(url, access_token, request_timeout, page_size, start_time):
    """
    Generator to handle Zendesk's incremental export API using offset pagination.
    """
    headers = build_headers(access_token=access_token)
    next_url = url

    if not isinstance(start_time, int):
        start_time = int(start_time.timestamp())

    params = {
        'start_time': start_time,
        'per_page': page_size
    }

    while next_url:
        response = call_api(
            next_url,
            request_timeout,
            params=params if next_url == url else None,
            headers=headers
        )

        response_json = response.json()
        yield response_json

        if response_json.get('end_of_stream'):
            break

        next_url = response_json.get('next_page') or response_json.get('after_url')
        params = None
