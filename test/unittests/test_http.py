import unittest
import requests
from unittest.mock import AsyncMock, Mock, patch
from urllib3.exceptions import ProtocolError
from requests.exceptions import ChunkedEncodingError, ConnectionError
import asyncio
from aiohttp import ClientSession
import zenpy
from tap_zendesk import http
from tap_zendesk.streams import abstracts
from tap_zendesk.exceptions import (
    ZendeskBadRequestError,
    ZendeskUnauthorizedError,
    ZendeskNotFoundError,
    ZendeskConflictError,
    ZendeskUnprocessableEntityError,
    ZendeskInternalServerError,
    ZendeskNotImplementedError,
    ZendeskBadGatewayError,
    ZendeskError,
    ZendeskServiceUnavailableError,
    ZendeskRateLimitError,
    ZendeskBackoffError
)



class Mockresponse:
    def __init__(
        self, resp, status_code, content=[""], headers=None, raise_error=False, text={}
    ):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text
        self.reason = "error"

    def prepare(self):
        return (
            self.json_data,
            self.status_code,
            self.content,
            self.headers,
            self.raise_error,
        )

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("mock sample message")

    def json(self):
        return self.text


SINGLE_RESPONSE = {"meta": {"has_more": False}}

PAGINATE_RESPONSE = {"meta": {"has_more": True, "after_cursor": "some_cursor"}}

PAGE_SIZE = 100
REQUEST_TIMEOUT = 300


def mocked_get(*args, **kwargs):
    fake_response = requests.models.Response()
    fake_response.headers.update(kwargs.get("headers", {}))
    fake_response.status_code = kwargs["status_code"]

    # We can't set the content or text of the Response directly, so we mock a function
    fake_response.json = Mock()
    fake_response.json.side_effect = lambda: kwargs.get("json", {})

    return fake_response


def mock_send_409(*args, **kwargs):
    return Mockresponse("", 409, raise_error=True)


@patch("time.sleep")
class TestBackoff(unittest.TestCase):
    """Test that we can make single requests to the API and handle cursor based pagination.

    Note: Because `get_cursor_based` is a generator, we have to consume it
    in the test before making assertions
    """

    @patch(
        "requests.get", side_effect=[mocked_get(status_code=200, json=SINGLE_RESPONSE)]
    )
    def test_get_cursor_based_gets_one_page(self, mock_get, mock_sleep):
        responses = [
            response
            for response in http.get_cursor_based(url="some_url",
                                                  access_token="some_token",
                                                  request_timeout=REQUEST_TIMEOUT,
                                                  page_size=PAGE_SIZE)
        ]
        actual_response = responses[0]
        self.assertDictEqual(SINGLE_RESPONSE, actual_response)

        expected_call_count = 1
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)

    @patch(
        "requests.get",
        side_effect=[
            mocked_get(status_code=200, json={"key1": "val1", **PAGINATE_RESPONSE}),
            mocked_get(status_code=200, json={"key2": "val2", **SINGLE_RESPONSE}),
        ],
    )
    def test_get_cursor_based_can_paginate(self, mock_get, mock_sleep):
        responses = [
            response
            for response in http.get_cursor_based(url="some_url",
                                                  access_token="some_token",
                                                  request_timeout=REQUEST_TIMEOUT,
                                                  page_size=PAGE_SIZE)
        ]

        self.assertDictEqual({"key1": "val1", **PAGINATE_RESPONSE}, responses[0])
        self.assertDictEqual({"key2": "val2", **SINGLE_RESPONSE}, responses[1])

        expected_call_count = 2
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)

    @patch(
        "requests.get",
        side_effect=[
            mocked_get(
                status_code=429,
                headers={"Retry-After": "1"},
                json={"key3": "val3", **SINGLE_RESPONSE},
            ),
            mocked_get(
                status_code=429,
                headers={"retry-after": 1},
                json={"key2": "val2", **SINGLE_RESPONSE},
            ),
            mocked_get(status_code=200, json={"key1": "val1", **SINGLE_RESPONSE}),
        ],
    )
    def test_get_cursor_based_handles_429(self, mock_get, mock_sleep):
        """Test that the tap:
        - can handle 429s
        - requests uses a case insensitive dict for the `headers`
        - can handle either a string or an integer for the retry header
        """
        responses = [
            response
            for response in http.get_cursor_based(url="some_url",
                                                  access_token="some_token",
                                                  request_timeout=REQUEST_TIMEOUT,
                                                  page_size=PAGE_SIZE)
        ]
        actual_response = responses[0]
        self.assertDictEqual({"key1": "val1", **SINGLE_RESPONSE}, actual_response)

        expected_call_count = 3
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)

    @patch(
        "requests.get", side_effect=[mocked_get(status_code=400, json={"key1": "val1"})]
    )
    def test_get_cursor_based_handles_400(self, mock_get, mock_sleep):
        try:
            responses = [
                response
                for response in http.get_cursor_based(url="some_url",
                                                      access_token="some_token",
                                                      request_timeout=300,
                                                      page_size=PAGE_SIZE)
            ]

        except ZendeskBadRequestError as e:
            expected_error_message = (
                "HTTP-error-code: 400, Error: A validation exception has occurred."
            )
            # Verify the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        # Verify the request calls only 1 time
        self.assertEqual(mock_get.call_count, 1)

    @patch(
        "requests.get",
        side_effect=[
            mocked_get(status_code=400, json={"error": "Couldn't authenticate you"})
        ],
    )
    def test_get_cursor_based_handles_400_api_error_message(self, mock_get, mock_sleep):
        try:
            responses = [
                response
                for response in http.get_cursor_based(url="some_url",
                                                      access_token="some_token",
                                                      request_timeout=300,
                                                      page_size=PAGE_SIZE)
            ]

        except ZendeskBadRequestError as e:
            expected_error_message = (
                "HTTP-error-code: 400, Error: Couldn't authenticate you"
            )
            # Verify the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        # Verify the request calls only 1 time
        self.assertEqual(mock_get.call_count, 1)

    @patch(
        "requests.get", side_effect=[mocked_get(status_code=401, json={"key1": "val1"})]
    )
    def test_get_cursor_based_handles_401(self, mock_get, mock_sleep):
        try:
            responses = [
                response
                for response in http.get_cursor_based(url="some_url",
                                                      access_token="some_token",
                                                      request_timeout=300,
                                                      page_size=PAGE_SIZE)
            ]
        except ZendeskUnauthorizedError as e:
            expected_error_message = (
                "HTTP-error-code: 401, Error: The access token provided is expired, revoked,"
                " malformed or invalid for other reasons."
            )
            # Verify the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        # Verify the request calls only 1 time
        self.assertEqual(mock_get.call_count, 1)

    @patch(
        "requests.get", side_effect=[mocked_get(status_code=404, json={"key1": "val1"})]
    )
    def test_get_cursor_based_handles_404(self, mock_get, mock_sleep):
        try:
            responses = [
                response
                for response in http.get_cursor_based(url="some_url",
                                                      access_token="some_token",
                                                      request_timeout=300,
                                                      page_size=PAGE_SIZE)
            ]
        except ZendeskNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: The resource you have specified cannot be found."
            # Verify the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        # Verify the request calls only 1 time
        self.assertEqual(mock_get.call_count, 1)

    @patch("requests.get", side_effect=mock_send_409)
    def test_get_cursor_based_handles_409(self, mocked_request, mock_api_token):
        """
        Test that `request` method retry 409 error 10 times
        """

        with self.assertRaises(ZendeskConflictError) as e:
            responses = [
                response
                for response in http.get_cursor_based(url="some_url",
                                                      access_token="some_token",
                                                      request_timeout=300,
                                                      page_size=PAGE_SIZE)
            ]
            expected_error_message = "HTTP-error-code: 409, Error: The API request cannot be completed because the requested operation would conflict with an existing item."
            self.assertEqual(str(e), expected_error_message)

        # Verify that requests.Session.request called 10 times
        self.assertEqual(mocked_request.call_count, 10)

    @patch(
        "requests.get", side_effect=[mocked_get(status_code=422, json={"key1": "val1"})]
    )
    def test_get_cursor_based_handles_422(self, mock_get, mock_sleep):
        try:
            responses = [
                response
                for response in http.get_cursor_based(url="some_url",
                                                      access_token="some_token",
                                                      request_timeout=300,
                                                      page_size=PAGE_SIZE)
            ]
        except ZendeskUnprocessableEntityError as e:
            expected_error_message = "HTTP-error-code: 422, Error: The request content itself is not processable by the server."
            # Verify the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        # Verify the request calls only 1 time
        self.assertEqual(mock_get.call_count, 1)

    @patch(
        "requests.get",
        side_effect=10 * [mocked_get(status_code=500, json={"key1": "val1"})],
    )
    def test_get_cursor_based_handles_500(self, mock_get, mock_sleep):
        """
        Test that the tap can handle 500 error and retry it 10 times
        """
        try:
            responses = [
                response
                for response in http.get_cursor_based(url="some_url",
                                                      access_token="some_token",
                                                      request_timeout=300,
                                                      page_size=PAGE_SIZE)
            ]
        except ZendeskInternalServerError as e:
            expected_error_message = (
                "HTTP-error-code: 500, Error: The server encountered an unexpected condition which prevented"
                " it from fulfilling the request."
            )
            # Verify the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        # Verify the request retry 10 times
        self.assertEqual(mock_get.call_count, 10)

    @patch(
        "requests.get",
        side_effect=10 * [mocked_get(status_code=501, json={"key1": "val1"})],
    )
    def test_get_cursor_based_handles_501(self, mock_get, mock_sleep):
        """
        Test that the tap can handle 501 error and retry it 10 times
        """
        try:
            responses = [
                response
                for response in http.get_cursor_based(url="some_url",
                                                      access_token="some_token",
                                                      request_timeout=300,
                                                      page_size=PAGE_SIZE)
            ]
        except ZendeskNotImplementedError as e:
            expected_error_message = "HTTP-error-code: 501, Error: The server does not support the functionality required to fulfill the request."
            # Verify the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        # Verify the request retry 10 times
        self.assertEqual(mock_get.call_count, 10)

    @patch(
        "requests.get",
        side_effect=10 * [mocked_get(status_code=502, json={"key1": "val1"})],
    )
    def test_get_cursor_based_handles_502(self, mock_get, mock_sleep):
        """
        Test that the tap can handle 502 error and retry it 10 times
        """
        try:
            responses = [
                response
                for response in http.get_cursor_based(url="some_url",
                                                      access_token="some_token",
                                                      request_timeout=300,
                                                      page_size=PAGE_SIZE)
            ]
        except ZendeskBadGatewayError as e:
            expected_error_message = (
                "HTTP-error-code: 502, Error: Server received an invalid response."
            )
            # Verify the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        # Verify the request retry 10 times
        self.assertEqual(mock_get.call_count, 10)

    @patch("requests.get")
    def test_get_cursor_based_handles_444(self, mock_get, mock_sleep):
        fake_response = requests.models.Response()
        fake_response.status_code = 444

        mock_get.side_effect = [fake_response]
        try:
            responses = [
                response
                for response in http.get_cursor_based(url="some_url",
                                                      access_token="some_token",
                                                      request_timeout=300,
                                                      page_size=PAGE_SIZE)
            ]
        except ZendeskError as e:
            expected_error_message = "HTTP-error-code: 444, Error: Unknown Error"
            # Verify the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        self.assertEqual(mock_get.call_count, 1)

    @patch("tap_zendesk.streams.abstracts.LOGGER.warning")
    def test_raise_or_log_zenpy_apiexception(self, mocked_logger, mock_sleep):
        schema = {}
        stream = "test_stream"
        error_string = '{"error": "Forbidden", "description": "Missing the following required scopes: read"}'
        e = zenpy.lib.exception.APIException(error_string)
        abstracts.raise_or_log_zenpy_apiexception(schema, stream, e)
        # Verify the raise_or_log_zenpy_apiexception Log expected message
        mocked_logger.assert_called_with(
            "The account credentials supplied do not have access to `%s` custom fields.",
            stream,
        )

    @patch("requests.get")
    def test_call_api_handles_timeout_error(self, mock_get, mock_sleep):
        mock_get.side_effect = requests.exceptions.Timeout

        try:
            responses = http.call_api(
                url="some_url", request_timeout=300, params={}, headers={}
            )
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch("requests.get")
    def test_call_api_handles_connection_error(self, mock_get, mock_sleep):
        mock_get.side_effect = ConnectionError

        try:
            responses = http.call_api(
                url="some_url", request_timeout=300, params={}, headers={}
            )
        except ConnectionError as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch(
        "requests.get",
        side_effect=10 * [mocked_get(status_code=524, json={"key1": "val1"})],
    )
    def test_get_cursor_based_handles_524(self, mock_get, mock_sleep):
        """
        Test that the tap can handle 524 error and retry it 10 times
        """
        try:
            responses = [
                response
                for response in http.get_cursor_based(url="some_url",
                                                      access_token="some_token",
                                                      request_timeout=300,
                                                      page_size=PAGE_SIZE)
            ]
        except ZendeskError as e:
            expected_error_message = "HTTP-error-code: 524, Error: Unknown Error"
            # Verify the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        # Verify the request retry 10 times
        self.assertEqual(mock_get.call_count, 10)

    @patch(
        "requests.get",
        side_effect=10 * [mocked_get(status_code=520, json={"key1": "val1"})],
    )
    def test_get_cursor_based_handles_520(self, mock_get, mock_sleep):
        """
        Test that the tap can handle 520 error and retry it 10 times
        """
        try:
            responses = [
                response
                for response in http.get_cursor_based(url="some_url",
                                                      access_token="some_token",
                                                      request_timeout=300,
                                                      page_size=PAGE_SIZE)
            ]
        except ZendeskError as e:
            expected_error_message = "HTTP-error-code: 520, Error: Unknown Error"
            # Verify the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        # Verify the request retry 10 times
        self.assertEqual(mock_get.call_count, 10)

    @patch(
        "requests.get",
        side_effect=10 * [mocked_get(status_code=503, json={"key1": "val1"})],
    )
    def test_get_cursor_based_handles_503(self, mock_get, mock_sleep):
        """
        Test that the tap can handle 503 error and retry it 10 times
        """
        try:
            responses = [
                response
                for response in http.get_cursor_based(url="some_url",
                                                      access_token="some_token",
                                                      request_timeout=300,
                                                      page_size=PAGE_SIZE)
            ]
        except ZendeskServiceUnavailableError as e:
            expected_error_message = (
                "HTTP-error-code: 503, Error: API service is currently unavailable."
            )
            # Verify the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        # Verify the request retry 10 times
        self.assertEqual(mock_get.call_count, 10)

    @patch("requests.get")
    def test_call_api_handles_protocol_error(self, mock_get, mock_sleep):
        """Check whether the request backoff properly for call_api method for 5 times in case of
         Protocol error"""
        mock_get.side_effect = ProtocolError

        with self.assertRaises(ProtocolError) as _:
            http.call_api(
                url="some_url", request_timeout=300, params={}, headers={}
            )
        self.assertEqual(mock_get.call_count, 5)

    @patch("requests.get")
    def test_call_api_handles_chunked_encoding_error(self, mock_get, mock_sleep):
        """Check whether the request backoff properly for call_api method for 5 times in case of
        ChunkedEncoding error"""
        mock_get.side_effect = ChunkedEncodingError

        with self.assertRaises(ChunkedEncodingError) as _:
            http.call_api(
                url="some_url", request_timeout=300, params={}, headers={}
            )
        self.assertEqual(mock_get.call_count, 5)

    @patch("requests.get")
    def test_call_api_handles_connection_reset_error(self, mock_get, mock_sleep):
        """Check whether the request backoff properly for call_api method for 5 times in case of
        ConnectionResetError error"""
        mock_get.side_effect = ConnectionResetError

        with self.assertRaises(ConnectionResetError) as _:
            http.call_api(
                url="some_url", request_timeout=300, params={}, headers={}
            )
        self.assertEqual(mock_get.call_count, 5)


class TestAPIAsync(unittest.TestCase):

    @patch("aiohttp.ClientSession.get")
    def test_call_api_async_success(self, mocked):
        """
        Test that call_api_async successfully retrieves data when the response status is 200.
        """
        url = "https://api.example.com/resource"
        response_data = {"key": "value"}
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = response_data
        mocked.return_value.__aenter__.return_value = mock_response

        async def run_test():
            async with ClientSession() as session:
                result = await http.call_api_async(session, url, 10, {}, {})
                self.assertEqual(result, response_data)

        asyncio.run(run_test())

    @patch("tap_zendesk.http.async_sleep")
    @patch("aiohttp.ClientSession.get")
    def test_call_api_async_rate_limit(self, mocked, mock_sleep):
        """
        Test that call_api_async retries the request when the response status is 429 (Too Many Requests) and Retry-After header is present with value 10.
        """
        url = "https://api.example.com/resource"
        retry_after = "10"
        response_data = {"key": "value"}
        mock_error_response = AsyncMock()
        mock_error_response.status = 429
        mock_error_response.headers = {"Retry-After": retry_after}
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = response_data
        mocked.return_value.__aenter__.side_effect = [
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_response,
        ]

        async def run_test():
            async with ClientSession() as session:
                result = await http.call_api_async(session, url, 10, {}, {})
                self.assertEqual(result, response_data)

        asyncio.run(run_test())
        mock_sleep.assert_called_with(10)

    @patch("tap_zendesk.http.async_sleep")
    @patch("aiohttp.ClientSession.get")
    def test_call_api_async_rate_limit_zero_retry_after(self, mocked, mock_sleep):
        """
        Test that call_api_async retries the request when the response status is 429 (Too Many Requests) and Retry-After header is present with value 0
        """
        url = "https://api.example.com/resource"
        retry_after = "0"
        response_data = {"key": "value"}
        mock_error_response = AsyncMock()
        mock_error_response.status = 429
        mock_error_response.headers = {"Retry-After": retry_after}
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = response_data
        mocked.return_value.__aenter__.side_effect = [
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_response,
        ]

        async def run_test():
            async with ClientSession() as session:
                result = await http.call_api_async(session, url, 10, {}, {})
                self.assertEqual(result, response_data)

        asyncio.run(run_test())
        mock_sleep.assert_called_with(60)

    @patch("tap_zendesk.http.async_sleep")
    @patch("aiohttp.ClientSession.get")
    def test_call_api_async_rate_limit_retry_after_missing_header(self, mocked, mock_sleep):
        """
        Test that call_api_async retries the request when the response status is 429 (Too Many Requests) and Retry-After header is missing.
        """
        url = "https://api.example.com/resource"
        response_data = {"key": "value"}
        mock_error_response = AsyncMock()
        mock_error_response.status = 429
        mock_error_response.headers = {}
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = response_data
        mocked.return_value.__aenter__.side_effect = [
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_response,
        ]

        async def run_test():
            async with ClientSession() as session:
                result = await http.call_api_async(session, url, 10, {}, {})
                self.assertEqual(result, response_data)
                self.assertEqual(mock_sleep.call_count, 4)

        asyncio.run(run_test())
        mock_sleep.assert_called_with(60)

    @patch("tap_zendesk.http.async_sleep")
    @patch("aiohttp.ClientSession.get")
    def test_call_api_async_rate_limit_exception_after_5_retries(
        self, mocked, mock_sleep
    ):
        """
        Test that call_api_async raises an exception after 5 retries when the response status is 429 (Too Many Requests).
        """
        url = "https://api.example.com/resource"
        retry_after = "1"
        mock_error_response = AsyncMock()
        mock_error_response.status = 429
        mock_error_response.headers = {"Retry-After": retry_after}
        mock_error_response.json.return_value = {}
        mocked.return_value.__aenter__.side_effect = [
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_error_response,
        ]


        async def run_test():
            async with ClientSession() as session:
                with self.assertRaises(ZendeskRateLimitError) as context:
                    await http.call_api_async(session, url, 10, {}, {})
                self.assertEqual(mock_sleep.call_count, 5)
                self.assertEqual(
                    "HTTP-error-code: 429, Error: The API rate limit for your organisation/application pairing has been exceeded.",
                    str(context.exception),
                )

        asyncio.run(run_test())

    @patch("tap_zendesk.http.async_sleep")
    @patch("aiohttp.ClientSession.get")
    def test_call_api_async_conflict(self, mocked, mock_sleep):
        """
        Test that call_api_async retries the request when the response status is 409 (Conflict).
        """
        url = "https://api.example.com/resource"
        response_data = {"key": "value"}
        mock_error_response = AsyncMock()
        mock_error_response.status = 409
        mock_error_response.json.return_value = {}
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = response_data
        mocked.return_value.__aenter__.side_effect = [
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_response,
        ]

        async def run_test():
            async with ClientSession() as session:
                result = await http.call_api_async(session, url, 10, {}, {})
                self.assertEqual(result, response_data)
                self.assertEqual(mock_sleep.call_count, 4)

        asyncio.run(run_test())

    @patch("tap_zendesk.http.async_sleep")
    @patch("aiohttp.ClientSession.get")
    def test_call_api_async_conflict_after_5_retries(self, mocked, mock_sleep):
        """
        Test that call_api_async retries the request when the response status is 409 (Conflict) with backoff.
        """
        url = "https://api.example.com/resource"
        response_data = {"key": "value"}
        mock_error_response = AsyncMock()
        mock_error_response.status = 409
        mock_error_response.json.return_value = {}
        mocked.return_value.__aenter__.side_effect = [
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_error_response,
        ]

        async def run_test():
            async with ClientSession() as session:
                with self.assertRaises(ZendeskConflictError) as context:
                    await http.call_api_async(session, url, 10, {}, {})
                self.assertEqual(mock_sleep.call_count, 5)
                self.assertEqual(
                    "HTTP-error-code: 409, Error: The API request cannot be completed because the requested operation would conflict with an existing item.",
                    str(context.exception),
                )

        asyncio.run(run_test())

    @patch("tap_zendesk.http.async_sleep")
    @patch("aiohttp.ClientSession.get")
    def test_call_api_async_500_error_backoff(self, mocked, mock_sleep):
        """
        Test that call_api_async raises an exception for 500 (Internal Server Error) after 5 retries.
        """
        url = "https://api.example.com/resource"
        error_message = "Internal Server Error"
        response_data = {"error": error_message}
        mock_error_response = AsyncMock()
        mock_error_response.status = 500
        mock_error_response.headers = {}
        mock_error_response.json.return_value = response_data
        mocked.return_value.__aenter__.side_effect = [
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_error_response,
        ]

        async def run_test():
            async with ClientSession() as session:
                with self.assertRaises(ZendeskInternalServerError) as context:
                    await http.call_api_async(session, url, 10, {}, {})
                self.assertEqual('HTTP-error-code: 500, Error: Internal Server Error', str(context.exception))
                self.assertEqual(mock_sleep.call_count, 5)

        asyncio.run(run_test())

    @patch("tap_zendesk.http.async_sleep")
    @patch("aiohttp.ClientSession.get")
    def test_call_api_async_502_error_backoff(self, mocked, mock_sleep):
        """
        Test that call_api_async raises an exception for 502 (Bad Gateway Error) after 5 retries.
        """
        url = "https://api.example.com/resource"
        error_message = "Bad Gateway Error"
        response_data = {"error": error_message}
        mock_error_response = AsyncMock()
        mock_error_response.status = 502
        mock_error_response.headers = {}
        mock_error_response.json.return_value = response_data
        mocked.return_value.__aenter__.side_effect = [
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_error_response,
        ]

        async def run_test():
            async with ClientSession() as session:
                with self.assertRaises(ZendeskBadGatewayError) as context:
                    await http.call_api_async(session, url, 10, {}, {})
                self.assertEqual('HTTP-error-code: 502, Error: Bad Gateway Error', str(context.exception))
                self.assertEqual(mock_sleep.call_count, 5)

        asyncio.run(run_test())

    @patch("tap_zendesk.http.async_sleep")
    @patch("aiohttp.ClientSession.get")
    def test_call_api_async_524_error_backoff(self, mocked, mock_sleep):
        """
        Test that call_api_async raises an exception for 524 (Unknown Error) after 5 retries.
        """
        url = "https://api.example.com/resource"
        error_message = "Unknown Error"
        response_data = {"error": error_message}
        mock_error_response = AsyncMock()
        mock_error_response.status = 524
        mock_error_response.headers = {}
        mock_error_response.json.return_value = response_data
        mocked.return_value.__aenter__.side_effect = [
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_error_response,
            mock_error_response,
        ]

        async def run_test():
            async with ClientSession() as session:
                with self.assertRaises(ZendeskBackoffError) as context:
                    await http.call_api_async(session, url, 10, {}, {})
                self.assertEqual('HTTP-error-code: 524, Error: Unknown Error', str(context.exception))
                self.assertEqual(mock_sleep.call_count, 5)

        asyncio.run(run_test())

    @patch("tap_zendesk.http.async_sleep")
    @patch("aiohttp.ClientSession.get")
    def test_call_api_async_400(self, mocked, mock_sleep):
        """
        Test that call_api_async raises an exception for 401 (Bad Request) responses without retrying.
        """
        url = "https://api.example.com/resource"
        error_message = "Bad Request"
        response_data = {"error": error_message}
        mock_error_response = AsyncMock()
        mock_error_response.status = 400
        mock_error_response.headers = {}
        mock_error_response.json.return_value = response_data
        mocked.return_value.__aenter__.return_value = mock_error_response

        async def run_test():
            async with ClientSession() as session:
                with self.assertRaises(ZendeskBadRequestError) as context:
                    await http.call_api_async(session, url, 10, {}, {})
                self.assertEqual('HTTP-error-code: 400, Error: Bad Request', str(context.exception))
                self.assertEqual(mock_sleep.call_count, 0)

        asyncio.run(run_test())

    @patch("aiohttp.ClientSession.get")
    def test_paginate_ticket_audits(self, mocked):
        """
        Test that paginate_ticket_audits correctly paginates through multiple pages of results.
        """
        url = "https://api.example.com/resource"
        access_token = "test_token"
        page_size = 2
        first_page = {
            "audits": [{"id": 1}, {"id": 2}],
            "meta": {
                "has_more": True,
                "after_cursor": "page_cursor_after==",
                "before_cursor": "page_cursor_before=="
            },
            "links":{
                "prev":"https://example.zendesk.com/api/v2/tickets/example/audits.json?page[before]=page_cursor_before==&page[size]=100",
                "next":"https://example.zendesk.com/api/v2/tickets/example/audits.json?page[after]=page_cursor_after==&page[size]=100"
            }
        }
        second_page = {
            "audits": [{"id": 3}, {"id": 4}],
            "meta": {
                "has_more": False,
                "after_cursor": "page_cursor_after==",
                "before_cursor": "page_cursor_before=="
            },
            "links":{
                "prev":"https://example.zendesk.com/api/v2/tickets/example/audits.json?page[before]=page_cursor_before==&page[size]=100",
                "next":"https://example.zendesk.com/api/v2/tickets/example/audits.json?page[after]=page_cursor_after==&page[size]=100"
            }
        }
        expected_result = {
            "audits": [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}],
            "meta": {
                "has_more": True,
                "after_cursor": "page_cursor_after==",
                "before_cursor": "page_cursor_before=="
            },
            "links":{
                "prev":"https://example.zendesk.com/api/v2/tickets/example/audits.json?page[before]=page_cursor_before==&page[size]=100",
                "next":"https://example.zendesk.com/api/v2/tickets/example/audits.json?page[after]=page_cursor_after==&page[size]=100"
            }
        }
        mock_first_response = AsyncMock()
        mock_first_response.status = 200
        mock_first_response.json.return_value = first_page
        mock_second_response = AsyncMock()
        mock_second_response.status = 200
        mock_second_response.json.return_value = second_page
        mocked.return_value.__aenter__.side_effect = [
            mock_first_response,
            mock_second_response,
        ]

        async def run_test():
            async with ClientSession() as session:
                result = await http.paginate_ticket_audits(
                    session, url, access_token, 10, page_size
                )
                self.assertEqual(result, expected_result)

        asyncio.run(run_test())

    @patch("aiohttp.ClientSession.get")
    def test_paginate_cursor_async_single_page(self, mocked):
        """
        A response with has_more=False should return immediately after one page.
        """
        url = "https://api.example.com/resource"
        response_data = {
            "identities": [{"id": 1}, {"id": 2}],
            "meta": {"has_more": False},
        }
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = response_data
        mocked.return_value.__aenter__.return_value = mock_response

        async def run_test():
            async with ClientSession() as session:
                result = await http.paginate_cursor_async(
                    session, url, "token", 10, 100, "identities"
                )
                self.assertEqual(result, [{"id": 1}, {"id": 2}])

        asyncio.run(run_test())

    @patch("aiohttp.ClientSession.get")
    def test_paginate_cursor_async_multi_page(self, mocked):
        """
        Multiple pages linked via meta.after_cursor should be aggregated into
        a single list of records, in order.
        """
        url = "https://api.example.com/resource"
        first_page = {
            "identities": [{"id": 1}],
            "meta": {"has_more": True, "after_cursor": "cursor-1"},
        }
        second_page = {
            "identities": [{"id": 2}],
            "meta": {"has_more": False},
        }
        mock_first_response = AsyncMock()
        mock_first_response.status = 200
        mock_first_response.json.return_value = first_page
        mock_second_response = AsyncMock()
        mock_second_response.status = 200
        mock_second_response.json.return_value = second_page
        mocked.return_value.__aenter__.side_effect = [
            mock_first_response,
            mock_second_response,
        ]

        async def run_test():
            async with ClientSession() as session:
                result = await http.paginate_cursor_async(
                    session, url, "token", 10, 100, "identities"
                )
                self.assertEqual(result, [{"id": 1}, {"id": 2}])

        asyncio.run(run_test())
        # Second call's params should carry the after_cursor from the first page.
        _, second_call_kwargs = mocked.call_args_list[1]
        self.assertEqual(second_call_kwargs["params"]["page[after]"], "cursor-1")

    @patch("aiohttp.ClientSession.get")
    def test_paginate_cursor_async_stops_when_cursor_missing(self, mocked):
        """
        If has_more is True but no after_cursor is present, pagination should
        stop rather than loop forever.
        """
        url = "https://api.example.com/resource"
        response_data = {
            "identities": [{"id": 1}],
            "meta": {"has_more": True},
        }
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = response_data
        mocked.return_value.__aenter__.return_value = mock_response

        async def run_test():
            async with ClientSession() as session:
                result = await http.paginate_cursor_async(
                    session, url, "token", 10, 100, "identities"
                )
                self.assertEqual(result, [{"id": 1}])

        asyncio.run(run_test())
        mocked.assert_called_once()


class TestHttpPaginationHelpers(unittest.TestCase):

    def test_build_headers_merges_additional_headers(self):
        headers = http.build_headers("token", additional_headers={"X-Custom": "value"})
        self.assertEqual(headers["Authorization"], "Bearer token")
        self.assertEqual(headers["X-Custom"], "value")

    def test_build_headers_without_additional_headers(self):
        headers = http.build_headers("token")
        self.assertNotIn("X-Custom", headers)

    @patch("tap_zendesk.http.call_api")
    def test_get_cursor_based_uses_provided_cursor(self, mock_call_api):
        mock_call_api.return_value.json.return_value = SINGLE_RESPONSE
        list(http.get_cursor_based(
            "https://example.zendesk.com/resource", "token", REQUEST_TIMEOUT, PAGE_SIZE,
            cursor="existing_cursor"
        ))
        _, kwargs = mock_call_api.call_args
        self.assertEqual(kwargs["params"]["page[after]"], "existing_cursor")

    @patch("aiohttp.ClientSession.get")
    def test_paginate_ticket_audits_missing_cursor_stops_pagination(self, mocked):
        """
        If a subsequent page's meta is missing 'after_cursor', pagination must stop
        gracefully (KeyError handled) instead of raising. The second page still has
        a cursor (covering the successful continuation branch) before the third
        page triggers the missing-cursor KeyError branch.
        """
        first_page = {
            "audits": [{"id": 1}],
            "meta": {"has_more": True, "after_cursor": "cursor1"},
        }
        second_page = {
            "audits": [{"id": 2}],
            "meta": {"has_more": True, "after_cursor": "cursor2"},
        }
        third_page = {
            "audits": [{"id": 3}],
            "meta": {"has_more": True},  # after_cursor missing -> KeyError branch
        }
        mock_first_response = AsyncMock()
        mock_first_response.status = 200
        mock_first_response.json.return_value = first_page
        mock_second_response = AsyncMock()
        mock_second_response.status = 200
        mock_second_response.json.return_value = second_page
        mock_third_response = AsyncMock()
        mock_third_response.status = 200
        mock_third_response.json.return_value = third_page
        mocked.return_value.__aenter__.side_effect = [
            mock_first_response,
            mock_second_response,
            mock_third_response,
        ]

        async def run_test():
            async with ClientSession() as session:
                result = await http.paginate_ticket_audits(
                    session, "https://api.example.com/resource", "token", 10, 2
                )
                self.assertEqual(result["audits"], [{"id": 1}, {"id": 2}, {"id": 3}])

        asyncio.run(run_test())

    def test_get_offset_based_paginates_until_no_next_page(self):
        with patch("tap_zendesk.http.call_api") as mock_call_api:
            first_response = Mock()
            first_response.json.return_value = {
                "tickets": [{"id": 1}],
                "next_page": "https://example.zendesk.com/resource?page=2",
            }
            second_response = Mock()
            second_response.json.return_value = {
                "tickets": [{"id": 2}],
            }
            mock_call_api.side_effect = [first_response, second_response]

            pages = list(http.get_offset_based(
                "https://example.zendesk.com/resource", "token", REQUEST_TIMEOUT, PAGE_SIZE
            ))
            self.assertEqual(len(pages), 2)
            self.assertEqual(mock_call_api.call_count, 2)

    def test_get_offset_based_uses_after_url_when_next_page_absent(self):
        with patch("tap_zendesk.http.call_api") as mock_call_api:
            first_response = Mock()
            first_response.json.return_value = {
                "tickets": [{"id": 1}],
                "after_url": "https://example.zendesk.com/resource?page=2",
            }
            second_response = Mock()
            second_response.json.return_value = {"tickets": [{"id": 2}]}
            mock_call_api.side_effect = [first_response, second_response]

            pages = list(http.get_offset_based(
                "https://example.zendesk.com/resource", "token", REQUEST_TIMEOUT, PAGE_SIZE
            ))
            self.assertEqual(len(pages), 2)

    @patch("aiohttp.ClientSession.get")
    def test_raise_for_error_for_async_handles_invalid_json_response(self, mocked):
        """
        When response.json() raises ContentTypeError/ValueError, raise_for_error_for_async
        must treat the body as an empty dict instead of propagating the JSON error.
        """
        from aiohttp import ContentTypeError as AiohttpContentTypeError

        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.json.side_effect = AiohttpContentTypeError(Mock(), Mock())
        mocked.return_value.__aenter__.return_value = mock_response

        async def run_test():
            async with ClientSession() as session:
                with self.assertRaises(ZendeskNotFoundError) as context:
                    await http.call_api_async(session, "https://api.example.com/resource", 10, {}, {})
                self.assertIn("HTTP-error-code: 404", str(context.exception))

        asyncio.run(run_test())

    def test_get_incremental_export_yields_pages_until_end_of_stream(self):
        with patch("tap_zendesk.http.call_api") as mock_call_api:
            first_response = Mock()
            first_response.json.return_value = {
                "tickets": [{"id": 1}],
                "end_of_stream": False,
                "after_cursor": "cursor1",
            }
            second_response = Mock()
            second_response.json.return_value = {
                "tickets": [{"id": 2}],
                "end_of_stream": True,
            }
            mock_call_api.side_effect = [first_response, second_response]

            pages = list(http.get_incremental_export(
                "https://example.zendesk.com/resource", "token", REQUEST_TIMEOUT, 0, None
            ))
            self.assertEqual(len(pages), 2)
            self.assertEqual(mock_call_api.call_count, 2)

    def test_get_incremental_export_raises_when_cursor_missing(self):
        with patch("tap_zendesk.http.call_api") as mock_call_api:
            response = Mock()
            response.json.return_value = {"tickets": [], "end_of_stream": False}
            mock_call_api.return_value = response

            with self.assertRaises(ValueError):
                list(http.get_incremental_export(
                    "https://example.zendesk.com/resource", "token", REQUEST_TIMEOUT, 0, None
                ))

    def test_get_incremental_export_accepts_datetime_start_time(self):
        import datetime
        with patch("tap_zendesk.http.call_api") as mock_call_api:
            response = Mock()
            response.json.return_value = {"tickets": [], "end_of_stream": True}
            mock_call_api.return_value = response

            list(http.get_incremental_export(
                "https://example.zendesk.com/resource", "token", REQUEST_TIMEOUT,
                datetime.datetime.now(datetime.timezone.utc), None
            ))
            mock_call_api.assert_called_once()

    def test_get_incremental_export_offset_yields_pages_until_no_next_url(self):
        with patch("tap_zendesk.http.call_api") as mock_call_api:
            first_response = Mock()
            first_response.json.return_value = {
                "tickets": [{"id": 1}],
                "end_of_stream": False,
                "next_page": "https://example.zendesk.com/resource?page=2",
            }
            second_response = Mock()
            second_response.json.return_value = {
                "tickets": [{"id": 2}],
                "end_of_stream": True,
            }
            mock_call_api.side_effect = [first_response, second_response]

            pages = list(http.get_incremental_export_offset(
                "https://example.zendesk.com/resource", "token", REQUEST_TIMEOUT, PAGE_SIZE, 0
            ))
            self.assertEqual(len(pages), 2)

    def test_get_incremental_export_offset_stops_when_no_next_page(self):
        with patch("tap_zendesk.http.call_api") as mock_call_api:
            response = Mock()
            response.json.return_value = {"tickets": [{"id": 1}], "end_of_stream": False}
            mock_call_api.return_value = response

            pages = list(http.get_incremental_export_offset(
                "https://example.zendesk.com/resource", "token", REQUEST_TIMEOUT, PAGE_SIZE, 0
            ))
            self.assertEqual(len(pages), 1)
            mock_call_api.assert_called_once()

    def test_get_incremental_export_offset_accepts_datetime_start_time(self):
        import datetime
        with patch("tap_zendesk.http.call_api") as mock_call_api:
            response = Mock()
            response.json.return_value = {"tickets": [], "end_of_stream": True}
            mock_call_api.return_value = response

            list(http.get_incremental_export_offset(
                "https://example.zendesk.com/resource", "token", REQUEST_TIMEOUT, PAGE_SIZE,
                datetime.datetime.now(datetime.timezone.utc)
            ))
            mock_call_api.assert_called_once()
