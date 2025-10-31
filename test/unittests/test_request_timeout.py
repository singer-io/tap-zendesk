import unittest
from unittest.mock import Mock, patch
from tap_zendesk import http
from tap_zendesk.streams import abstracts, TicketAudits
import requests
import datetime
import asyncio
import aiohttp

PAGINATE_RESPONSE = {
    'meta': {'has_more': True,
             'after_cursor': 'some_cursor'},
    'end_of_stream': False,
    'after_cursor': 'some_cursor',
    'next_page': '3'
}
REQUEST_TIMEOUT = 300
REQUEST_TIMEOUT_STR = "300"
REQUEST_TIMEOUT_FLOAT = 300.05
PAGE_SIZE = 100

SINGLE_RESPONSE = {
    'meta': {'has_more': False}
}

START_TIME = datetime.datetime.strptime("2021-10-30T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")
def mocked_get(*args, **kwargs):
    fake_response = requests.models.Response()
    fake_response.headers.update(kwargs.get('headers', {}))
    fake_response.status_code = kwargs['status_code']

    # We can't set the content or text of the Response directly, so we mock a function
    fake_response.json = Mock()
    fake_response.json.side_effect = lambda:kwargs.get('json', {})

    return fake_response

@patch("time.sleep")
class TestRequestTimeoutBackoff(unittest.TestCase):
    """
    A set of unit tests to ensure that requests are retrying properly for Timeout Error.
    """
    @patch('requests.get')
    def test_call_api_handles_timeout_error(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times,
        """
        mock_get.side_effect = requests.exceptions.Timeout

        try:
            responses = http.call_api(url='some_url', request_timeout=300, params={}, headers={})
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get', side_effect=5*[requests.exceptions.Timeout])
    def test_get_cursor_based_handles_timeout_error(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times,
        """

        try:
            responses = [response for response in http.get_cursor_based(url='some_url',
                                                                        access_token='some_token',
                                                                        request_timeout=REQUEST_TIMEOUT,
                                                                        page_size=PAGE_SIZE)]
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get', side_effect=[mocked_get(status_code=200, json={"key1": "val1", **PAGINATE_RESPONSE}),
                                        requests.exceptions.Timeout, requests.exceptions.Timeout,
                                        mocked_get(status_code=200, json={"key1": "val1", **SINGLE_RESPONSE})])
    def test_get_cursor_based_handles_timeout_error_in_pagination_call(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout`. In next page call the tap should retry request timeout error.
        """

        try:
            responses = [response for response in http.get_cursor_based(url='some_url',
                                                                        access_token='some_token',
                                                                        request_timeout=REQUEST_TIMEOUT,
                                                                        page_size=PAGE_SIZE)]
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request call total 4 times(2 time retry call, 2 time 200 call)
        self.assertEqual(mock_get.call_count, 4)

    @patch('requests.get', side_effect=5*[requests.exceptions.Timeout])
    def test_get_offset_based_handles_timeout_error(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times,
        """

        try:
            responses = [response for response in http.get_offset_based(url='some_url',
                                                                        access_token='some_token',
                                                                        request_timeout=REQUEST_TIMEOUT,
                                                                        page_size=PAGE_SIZE)]
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get', side_effect=[mocked_get(status_code=200, json={"key1": "val1", **PAGINATE_RESPONSE}),
                                        requests.exceptions.Timeout, requests.exceptions.Timeout,
                                        mocked_get(status_code=200, json={"key1": "val1", **SINGLE_RESPONSE})])
    def test_get_offset_based_handles_timeout_error_in_pagination_call(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout`. In next page call the tap should retry request timeout error.
        """

        try:
            responses = [response for response in http.get_offset_based(url='some_url',
                                                                        access_token='some_token',
                                                                        request_timeout=REQUEST_TIMEOUT,
                                                                        page_size=PAGE_SIZE)]
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request call total 4 times(2 time retry call, 2 time 200 call)
        self.assertEqual(mock_get.call_count, 4)

    @patch('requests.get', side_effect=5*[requests.exceptions.Timeout])
    def test_get_incremental_export_handles_timeout_error(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times,
        """

        try:
            responses = [response for response in http.get_incremental_export(url='some_url',access_token='some_token', 
                                                                              request_timeout=REQUEST_TIMEOUT, start_time= datetime.datetime.utcnow(), side_load=None)]
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get')
    def test_cursor_based_stream_timeout_error_without_parameter(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when `request_timeout` does not passed,
        """
        mock_get.side_effect = requests.exceptions.Timeout

        paginated_stream = abstracts.PaginatedStream(config={'subdomain': '34', 'access_token': 'df'})
        paginated_stream.endpoint = 'endpoint_path'
        paginated_stream.pagination_type = 'cursor'
        try:
            responses = list(paginated_stream.get_objects())
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get')
    def test_cursor_based_stream_timeout_error_with_zero_str_value(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when string "0" value of `request_timeout` passed,
        """
        mock_get.side_effect = requests.exceptions.Timeout
        paginated_stream = abstracts.PaginatedStream(config={'subdomain': '34', 'access_token': 'df'})
        paginated_stream.endpoint = 'endpoint_path'
        paginated_stream.pagination_type = 'cursor'
        try:
            responses = list(paginated_stream.get_objects())
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get')
    def test_cursor_based_stream_timeout_error_with_zero_int_value(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when int 0 value of `request_timeout` passed,
        """
        mock_get.side_effect = requests.exceptions.Timeout
        paginated_stream = abstracts.PaginatedStream(config={'subdomain': '34', 'access_token': 'df'})
        paginated_stream.endpoint = 'endpoint_path'
        paginated_stream.pagination_type = 'cursor'
        try:
            responses = list(paginated_stream.get_objects())
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get')
    def test_cursor_based_stream_timeout_error_with_str_value(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when string value of `request_timeout` passed,
        """
        mock_get.side_effect = requests.exceptions.Timeout
        paginated_stream = abstracts.PaginatedStream(config={'subdomain': '34', 'access_token': 'df'})
        paginated_stream.endpoint = 'endpoint_path'
        paginated_stream.pagination_type = 'cursor'
        try:
            responses = list(paginated_stream.get_objects())
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get')
    def test_cursor_based_stream_timeout_error_with_int_value(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when int value of `request_timeout` passed,
        """
        mock_get.side_effect = requests.exceptions.Timeout
        paginated_stream = abstracts.PaginatedStream(config={'subdomain': '34', 'access_token': 'df'})
        paginated_stream.endpoint = 'endpoint_path'
        paginated_stream.pagination_type = 'cursor'
        try:
            responses = list(paginated_stream.get_objects())
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get')
    def test_cursor_based_stream_timeout_error_with_float_value(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when float value of `request_timeout` passed,
        """
        mock_get.side_effect = requests.exceptions.Timeout
        paginated_stream = abstracts.PaginatedStream(config={'subdomain': '34', 'access_token': 'df'})
        paginated_stream.endpoint = 'endpoint_path'
        paginated_stream.pagination_type = 'cursor'
        try:
            responses = list(paginated_stream.get_objects())
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)
    @patch('requests.get')
    def test_cursor_based_stream_timeout_error_with_empty_value(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when empty value of `request_timeout` passed,
        """
        mock_get.side_effect = requests.exceptions.Timeout
        paginated_stream = abstracts.PaginatedStream(config={'subdomain': '34', 'access_token': 'df'})
        paginated_stream.endpoint = 'endpoint_path'
        paginated_stream.pagination_type = 'cursor'
        try:
            responses = list(paginated_stream.get_objects())
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)
    @patch('requests.get')
    def test_cursor_based_export_stream_timeout_error_without_parameter(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when `request_timeout` does not passed,
        """
        mock_get.side_effect = requests.exceptions.Timeout
        cursor_based_export_stream = abstracts.CursorBasedExportStream(config={'subdomain': '34', 'access_token': 'df'})
        cursor_based_export_stream.endpoint = 'endpoint_path'
        try:
            responses = list(cursor_based_export_stream.get_objects(datetime.datetime.utcnow()))
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get')
    def test_cursor_based_export_stream_timeout_error_with_zero_str_value(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when stiring "0" value of `request_timeout` passed,
        """
        mock_get.side_effect = requests.exceptions.Timeout
        cursor_based_export_stream = abstracts.CursorBasedExportStream(config={'subdomain': '34', 'access_token': 'df', 'request_timeout': '0'})
        cursor_based_export_stream.endpoint = 'endpoint_path'
        try:
            responses = list(cursor_based_export_stream.get_objects(datetime.datetime.utcnow()))
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get')
    def test_cursor_based_export_stream_timeout_error_with_zero_int_value(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when int 0 value of `request_timeout` passed,
        """
        mock_get.side_effect = requests.exceptions.Timeout
        cursor_based_export_stream = abstracts.CursorBasedExportStream(config={'subdomain': '34', 'access_token': 'df', 'request_timeout': 0})
        cursor_based_export_stream.endpoint = 'endpoint_path'
        try:
            responses = list(cursor_based_export_stream.get_objects(datetime.datetime.utcnow()))
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get')
    def test_cursor_based_export_stream_timeout_error_with_empty_value(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when empty value of `request_timeout` passed,
        """
        mock_get.side_effect = requests.exceptions.Timeout
        cursor_based_export_stream = abstracts.CursorBasedExportStream(config={'subdomain': '34', 'access_token': 'df', 'request_timeout': ''})
        cursor_based_export_stream.endpoint = 'endpoint_path'
        try:
            responses = list(cursor_based_export_stream.get_objects(datetime.datetime.utcnow()))
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get')
    def test_cursor_based_export_stream_timeout_error_with_str_value(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when string value of `request_timeout` passed,
        """
        mock_get.side_effect = requests.exceptions.Timeout
        cursor_based_export_stream = abstracts.CursorBasedExportStream(config={'subdomain': '34', 'access_token': 'df', 'request_timeout': REQUEST_TIMEOUT_STR})
        cursor_based_export_stream.endpoint = 'endpoint_path'
        try:
            responses = list(cursor_based_export_stream.get_objects(datetime.datetime.utcnow()))
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get')
    def test_cursor_based_export_stream_timeout_error_with_int_value(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when int value of `request_timeout` passed,
        """
        mock_get.side_effect = requests.exceptions.Timeout
        cursor_based_export_stream = abstracts.CursorBasedExportStream(config={'subdomain': '34', 'access_token': 'df', 'request_timeout': REQUEST_TIMEOUT})
        cursor_based_export_stream.endpoint = 'endpoint_path'
        try:
            responses = list(cursor_based_export_stream.get_objects(datetime.datetime.utcnow()))
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch('requests.get')
    def test_cursor_based_export_stream_timeout_error_with_float_value(self, mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when float value of `request_timeout` passed,
        """
        mock_get.side_effect = requests.exceptions.Timeout
        cursor_based_export_stream = abstracts.CursorBasedExportStream(config={'subdomain': '34', 'access_token': 'df', 'request_timeout': REQUEST_TIMEOUT_FLOAT})
        cursor_based_export_stream.endpoint = 'endpoint_path'
        try:
            responses = list(cursor_based_export_stream.get_objects(datetime.datetime.utcnow()))
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 5 times on timeout
        self.assertEqual(mock_get.call_count, 5)

    @patch("asyncio.sleep", return_value=None)
    @patch("aiohttp.ClientSession.get")
    def test_ticket_audits_timeout_error_without_parameter(
        self, mock_get, mock_async_sleep, mock_sleep
    ):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when `request_timeout` does not passed,"""
        mock_get.return_value.__aenter__.side_effect = requests.exceptions.Timeout

        ticket_audits = TicketAudits(
            config={"subdomain": "test-zendesk", "access_token": "df"}
        )

        async def run_test():
            async with aiohttp.ClientSession() as session:
                try:
                    await ticket_audits.get_objects(session, 1)
                except requests.exceptions.Timeout as e:
                    pass

            # Verify the request retry 5 times on timeout
            self.assertEqual(mock_async_sleep.call_count, 4)

        asyncio.run(run_test())

    @patch("asyncio.sleep", return_value=None)
    @patch("aiohttp.ClientSession.get")
    def test_ticket_audits_timeout_error_with_zero_str_value(
        self, mock_get, mock_async_sleep, mock_sleep
    ):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when string "0" value of `request_timeout` passed,"""
        mock_get.return_value.__aenter__.side_effect = requests.exceptions.Timeout
        ticket_audits = TicketAudits(
            config={
                "subdomain": "test-zendesk",
                "access_token": "df",
                "request_timeout": "0",
            }
        )

        async def run_test():
            async with aiohttp.ClientSession() as session:
                try:
                    await ticket_audits.get_objects(session, 1)
                except requests.exceptions.Timeout as e:
                    pass

            # Verify the request retry 5 times on timeout
            self.assertEqual(mock_async_sleep.call_count, 4)

        asyncio.run(run_test())

    @patch("asyncio.sleep", return_value=None)
    @patch("aiohttp.ClientSession.get")
    def test_ticket_audits_timeout_error_with_zero_int_value(
        self, mock_get, mock_async_sleep, mock_sleep
    ):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when int 0 value of `request_timeout` passed,"""
        mock_get.return_value.__aenter__.side_effect = requests.exceptions.Timeout

        ticket_audits = TicketAudits(
            config={
                "subdomain": "test-zendesk",
                "access_token": "df",
                "request_timeout": 0,
            }
        )

        async def run_test():
            async with aiohttp.ClientSession() as session:
                try:
                    await ticket_audits.get_objects(session, 1)
                except requests.exceptions.Timeout as e:
                    pass

            # Verify the request retry 5 times on timeout
            self.assertEqual(mock_async_sleep.call_count, 4)

        asyncio.run(run_test())

    @patch("asyncio.sleep", return_value=None)
    @patch("aiohttp.ClientSession.get")
    def test_ticket_audits_timeout_error_with_str_value(
        self, mock_get, mock_async_sleep, mock_sleep
    ):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when string value of `request_timeout` passed,"""
        mock_get.return_value.__aenter__.side_effect = requests.exceptions.Timeout

        ticket_audits = TicketAudits(
            config={
                "subdomain": "test-zendesk",
                "access_token": "df",
                "request_timeout": REQUEST_TIMEOUT_STR,
            }
        )

        async def run_test():
            async with aiohttp.ClientSession() as session:
                try:
                    await ticket_audits.get_objects(session, 1)
                except requests.exceptions.Timeout as e:
                    pass

            # Verify the request retry 5 times on timeout
            self.assertEqual(mock_async_sleep.call_count, 4)

        asyncio.run(run_test())

    @patch("asyncio.sleep", return_value=None)
    @patch("aiohttp.ClientSession.get")
    def test_ticket_audits_timeout_error_with_int_value(
        self, mock_get, mock_async_sleep, mock_sleep
    ):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when int value of `request_timeout` passed,"""
        mock_get.return_value.__aenter__.side_effect = requests.exceptions.Timeout

        ticket_audits = TicketAudits(
            config={
                "subdomain": "test-zendesk",
                "access_token": "df",
                "request_timeout": REQUEST_TIMEOUT,
            }
        )

        async def run_test():
            async with aiohttp.ClientSession() as session:
                try:
                    await ticket_audits.get_objects(session, 1)
                except requests.exceptions.Timeout as e:
                    pass

            # Verify the request retry 5 times on timeout
            self.assertEqual(mock_async_sleep.call_count, 4)

        asyncio.run(run_test())

    @patch("asyncio.sleep", return_value=None)
    @patch("aiohttp.ClientSession.get")
    def test_ticket_audits_timeout_error_with_float_value(
        self, mock_get, mock_async_sleep, mock_sleep
    ):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when float value of `request_timeout` passed,"""
        mock_get.return_value.__aenter__.side_effect = requests.exceptions.Timeout

        ticket_audits = TicketAudits(
            config={
                "subdomain": "test-zendesk",
                "access_token": "df",
                "request_timeout": REQUEST_TIMEOUT_FLOAT,
            }
        )

        async def run_test():
            async with aiohttp.ClientSession() as session:
                try:
                    await ticket_audits.get_objects(session, 1)
                except requests.exceptions.Timeout as e:
                    pass

            # Verify the request retry 5 times on timeout
            self.assertEqual(mock_async_sleep.call_count, 4)

        asyncio.run(run_test())

    @patch("asyncio.sleep", return_value=None)
    @patch("aiohttp.ClientSession.get")
    def test_ticket_audits_timeout_error_with_empty_value(
        self, mock_get, mock_async_sleep, mock_sleep
    ):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 5 times when empty value of `request_timeout` passed,"""
        mock_get.return_value.__aenter__.side_effect = requests.exceptions.Timeout

        ticket_audits = TicketAudits(
            config={
                "subdomain": "test-zendesk",
                "access_token": "df",
                "request_timeout": REQUEST_TIMEOUT_STR,
            }
        )

        async def run_test():
            async with aiohttp.ClientSession() as session:
                try:
                    await ticket_audits.get_objects(session, 1)
                except requests.exceptions.Timeout as e:
                    pass

            # Verify the request retry 5 times on timeout
            self.assertEqual(mock_async_sleep.call_count, 4)

        asyncio.run(run_test())
