import base64
import logging
import traceback
import re
from time import sleep
from asyncio import sleep as async_sleep
from urllib.parse import urlsplit
import backoff
import requests
import singer
from requests.exceptions import Timeout, HTTPError, ChunkedEncodingError, ConnectionError
from aiohttp import ContentTypeError
from urllib3.exceptions import ProtocolError
from tap_zendesk.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    ZendeskError,
    ZendeskBackoffError,
    ZendeskForbiddenError,
    ZendeskNotFoundError,
)


LOGGER = singer.get_logger()
# Default wait time for 429 and 5xx error
DEFAULT_WAIT = 60
# Default wait time for backoff for conflict error
DEFAULT_WAIT_FOR_CONFLICT_ERROR = 10

BASE_URL = "https://{subdomain}.zendesk.com/api/v2/"
BASIC_AUTH_PREFIX = "Basic "

def validate_subdomain(subdomain):
    """Require a single ASCII DNS label, not a URL or hostname."""
    if not isinstance(subdomain, str) or not re.fullmatch(
            r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", subdomain):
        raise ValueError("Zendesk subdomain must be a single DNS label of 1-63 letters, digits, or hyphens.")
    return subdomain

def validate_url(url, expected_url=None):
    """Reject credential destinations outside HTTPS Zendesk tenant origins."""
    if not isinstance(url, str) or not url.isascii() or any(
            char.isspace() or ord(char) < 32 or ord(char) == 127 for char in url) or "\\" in url:
        raise ValueError("Invalid Zendesk request URL.")
    parsed = urlsplit(url)
    hostname = parsed.hostname or ""
    suffix = ".zendesk.com"
    if not hostname.endswith(suffix):
        raise ValueError("Zendesk requests must target a Zendesk tenant origin.")
    validate_subdomain(hostname[:-len(suffix)])
    if (parsed.scheme != "https"
            or parsed.netloc.lower() not in (hostname, hostname + ":443")
            or "#" in url):
        raise ValueError("Zendesk requests require HTTPS without user information, fragments, or nonstandard ports.")
    origin = "https://" + hostname
    if expected_url is not None and origin != validate_url(expected_url):
        raise ValueError("Zendesk request URL must remain on the configured tenant origin.")
    return origin

def reject_redirect(status):
    """Do not forward credentials or OAuth POST bodies via redirects."""
    if status in range(300, 400):
        raise ValueError("Zendesk request redirects are not supported; refusing to forward credentials.")

class ZendeskSession(requests.Session):
    """Keep Zenpy requests, including pagination, on the configured tenant."""

    def __init__(self, subdomain):
        super().__init__()
        self.origin = f"https://{validate_subdomain(subdomain)}.zendesk.com"

    def send(self, request, **kwargs):
        validate_url(request.url, self.origin)
        kwargs["allow_redirects"] = False
        response = super().send(request, **kwargs)
        reject_redirect(response.status_code)
        return response

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

    # API-token credentials arrive as a complete `Basic ...` header value (see get_credential)
    if access_token and access_token.startswith(BASIC_AUTH_PREFIX):
        headers['Authorization'] = access_token

    if additional_headers:
        headers.update(additional_headers)

    return headers

def get_credential(config: dict) -> str:
    """
    Return the credential to pass to the request helpers' `access_token` argument.

    OAuth has precedence, as when creating the Zenpy client: the access token is returned
    and sent as a Bearer token. Otherwise email + api_token are sent as Basic authentication
    (`{email}/token:{api_token}`), which Zendesk documents for API tokens.
    """
    if config.get('access_token'):
        return config['access_token']

    if config.get('email') and config.get('api_token'):
        credentials = f"{config['email']}/token:{config['api_token']}"
        return BASIC_AUTH_PREFIX + base64.b64encode(credentials.encode('utf-8')).decode('ascii')

    raise ValueError("No suitable authentication keys provided. Set 'access_token', or 'email' and 'api_token'.")

@backoff.on_exception(backoff.expo,
                      (HTTPError, ZendeskError), # Added support of backoff for all unhandled status codes.
                      max_tries=10,
                      giveup=is_fatal)
@backoff.on_exception(backoff.expo,
                    (ConnectionError, ConnectionResetError, Timeout, ChunkedEncodingError, ProtocolError),#As ConnectionError error and timeout error does not have attribute status_code,
                    max_tries=5, # here we added another backoff expression.
                    factor=2)
def call_api(url, request_timeout, params, headers):
    return _request(url, request_timeout, params, headers)

def _request(url, request_timeout, params, headers):
    validate_url(url)
    response = requests.get(url, params=params, headers=headers, timeout=request_timeout, allow_redirects=False)
    reject_redirect(response.status_code)
    raise_for_error(response)
    return response

BACKOFF_LOGGER = logging.getLogger("backoff")

def _format_exception(exc):
    return traceback.format_exception_only(type(exc), exc)[-1].rstrip("\n")

def _log_probe_backoff(details):
    BACKOFF_LOGGER.info("Backing off %s(...) for %.1fs (%s)",
                        details["target"].__name__, details["wait"], _format_exception(details["exception"]))

def _log_probe_giveup(details):
    """Log at WARNING only for errors the probe's caller handles; everything else stays at ERROR."""
    exc = details["exception"]
    expected = (ZendeskForbiddenError, ZendeskNotFoundError) if details["kwargs"].get("not_found_ok") \
        else (ZendeskForbiddenError,)
    level = logging.WARNING if isinstance(exc, expected) else logging.ERROR
    BACKOFF_LOGGER.log(level, "Giving up %s(...) after %d tries (%s)",
                       details["target"].__name__, details["tries"], _format_exception(exc))

@backoff.on_exception(backoff.expo,
                      (HTTPError, ZendeskError),
                      max_tries=10,
                      giveup=is_fatal,
                      logger=None,
                      on_backoff=_log_probe_backoff,
                      on_giveup=_log_probe_giveup)
@backoff.on_exception(backoff.expo,
                      (ConnectionError, ConnectionResetError, Timeout, ChunkedEncodingError, ProtocolError),
                      max_tries=5,
                      factor=2)
def probe_api(url, request_timeout, params, headers, not_found_ok=False): # pylint: disable=unused-argument
    """call_api for check_access probes. 403 (and 404 when not_found_ok) are expected and handled by the caller."""
    return _request(url, request_timeout, params, headers)

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

    seen_cursors = {cursor} if cursor else set()
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

        next_cursor = meta.get('after_cursor')
        if not next_cursor:
            raise ValueError("Missing 'after_cursor' while 'has_more' is true.")
        if next_cursor in seen_cursors:
            raise ValueError("Repeated 'after_cursor' while 'has_more' is true.")
        seen_cursors.add(next_cursor)
        params['page[after]'] = next_cursor

def get_offset_based(url, access_token, request_timeout, page_size, **kwargs):
    """
    Offset-based pagination generator.
    Yields:
        dict: Parsed JSON response from each page.
    """
    validate_url(url)
    custom_headers = kwargs.pop('headers', {})
    query_params = kwargs.pop('params', {})

    headers = build_headers(access_token=access_token, additional_headers=custom_headers)
    next_url = url
    params = {
        "per_page": page_size,
        **query_params
    }

    while next_url:
        validate_url(next_url, url)
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
    validate_url(url)
    async with session.get(
        url, params=params, headers=headers, timeout=request_timeout, allow_redirects=False
    ) as response:
        reject_redirect(response.status)
        response_json = await raise_for_error_for_async(response)

        return response_json

async def paginate_cursor_async(session, url, access_token, request_timeout, page_size, item_key, **kwargs): # pylint: disable=too-many-arguments,too-many-positional-arguments
    """
    Generic async cursor-based pagination. Fetches all pages for the given
    endpoint and returns the aggregated list of records found at `item_key`.
    """
    custom_headers = kwargs.pop('headers', {})
    query_params = kwargs.pop('params', {})

    headers = build_headers(access_token=access_token, additional_headers=custom_headers)
    params = {
        'page[size]': page_size,
        **query_params
    }

    all_records = []
    has_more = True
    while has_more:
        response = await call_api_async(session, url, request_timeout, params=params, headers=headers)
        all_records.extend(response.get(item_key, []))

        has_more = response.get('meta', {}).get('has_more', False)
        if has_more:
            cursor = response.get('meta', {}).get('after_cursor')
            if not cursor:
                LOGGER.info("Cursor not found, stopping pagination.")
                break
            params['page[after]'] = cursor

    return all_records

async def paginate_ticket_audits(session, url, access_token, request_timeout, page_size, **kwargs):
    """
    Paginate through the ticket audits API endpoint and return the aggregated results
    """
    custom_headers = kwargs.pop('headers', {})
    query_params = kwargs.pop('params', {})

    headers = build_headers(access_token=access_token, additional_headers=custom_headers)
    params = {
        'page[size]': page_size,
        **query_params
    }

    initial_response = await call_api_async(session, url, request_timeout, params=params, headers=headers)

    has_more = initial_response['meta']['has_more']

    if has_more:
        cursor = initial_response['meta']['after_cursor']
        params['page[after]'] = cursor

    while has_more:

        response = await call_api_async(session, url, request_timeout, params=params, headers=headers)

        initial_response["audits"].extend(response["audits"])
        has_more = response['meta']['has_more']

        if has_more:
            try:
                cursor = response['meta']['after_cursor']
                params['page[after]'] = cursor
            except KeyError:
                LOGGER.info("Cursor not found, stopping pagination.")
                has_more = False

    return initial_response

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

    seen_cursors = set()
    while True:
        response = call_api(url, request_timeout, params=params, headers=headers)
        response_json = response.json()
        yield response_json

        if response_json.get('end_of_stream'):
            break

        cursor = response_json.get('after_cursor')
        if not cursor:
            raise ValueError("Missing 'after_cursor' in response during pagination.")
        if cursor in seen_cursors:
            raise ValueError("Repeated 'after_cursor' in response during pagination.")
        seen_cursors.add(cursor)

        params = {
            'cursor': cursor,
            'include': side_load
        }

def get_incremental_export_offset(url, access_token, request_timeout, page_size, start_time):
    """
    Generator to handle Zendesk's incremental export API using offset pagination.
    """
    validate_url(url)
    headers = build_headers(access_token=access_token)
    next_url = url

    if not isinstance(start_time, int):
        start_time = int(start_time.timestamp())

    params = {
        'start_time': start_time,
        'per_page': page_size
    }

    while next_url:
        validate_url(next_url, url)
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
