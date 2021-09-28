import unittest
from unittest.mock import MagicMock, Mock, patch
from tap_zendesk import http, streams
import requests

import zenpy

SINGLE_RESPONSE = {
    'meta': {'has_more': False}
}

PAGINATE_RESPONSE = {
    'meta': {'has_more': True,
             'after_cursor': 'some_cursor'}
}

def mocked_get(*args, **kwargs):
    fake_response = requests.models.Response()
    fake_response.headers.update(kwargs.get('headers', {}))
    fake_response.status_code = kwargs['status_code']

    # We can't set the content or text of the Response directly, so we mock a function
    fake_response.json = Mock()
    fake_response.json.side_effect = lambda:kwargs.get('json', {})

    return fake_response


class TestBackoff(unittest.TestCase):
    """Test that we can make single requests to the API and handle cursor based pagination.

    Note: Because `get_cursor_based` is a generator, we have to consume it
    in the test before making assertions
    """

    @patch('requests.get',
           side_effect=[mocked_get(status_code=200, json=SINGLE_RESPONSE)])
    def test_get_cursor_based_gets_one_page(self, mock_get):
        responses = [response for response in http.get_cursor_based(url='some_url',
                                                                    access_token='some_token')]
        actual_response = responses[0]
        self.assertDictEqual(SINGLE_RESPONSE,
                             actual_response)

        expected_call_count = 1
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)

    @patch('requests.get',
           side_effect=[
               mocked_get(status_code=200, json={"key1": "val1", **PAGINATE_RESPONSE}),
               mocked_get(status_code=200, json={"key2": "val2", **SINGLE_RESPONSE}),
           ])
    def test_get_cursor_based_can_paginate(self, mock_get):
        responses = [response
                     for response in http.get_cursor_based(url='some_url',
                                                           access_token='some_token')]

        self.assertDictEqual({"key1": "val1", **PAGINATE_RESPONSE},
                              responses[0])
        self.assertDictEqual({"key2": "val2", **SINGLE_RESPONSE},
                              responses[1])

        expected_call_count = 2
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)


    @patch('requests.get',
           side_effect=[
               mocked_get(status_code=429, headers={'Retry-After': '1'}, json={"key3": "val3", **SINGLE_RESPONSE}),
               mocked_get(status_code=429, headers={'retry-after': 1}, json={"key2": "val2", **SINGLE_RESPONSE}),
               mocked_get(status_code=200, json={"key1": "val1", **SINGLE_RESPONSE}),
           ])
    def test_get_cursor_based_handles_429(self, mock_get):
        """Test that the tap:
        - can handle 429s
        - requests uses a case insensitive dict for the `headers`
        - can handle either a string or an integer for the retry header
        """
        responses = [response for response in http.get_cursor_based(url='some_url',
                                                                    access_token='some_token')]
        actual_response = responses[0]
        self.assertDictEqual({"key1": "val1", **SINGLE_RESPONSE},
                             actual_response)

        expected_call_count = 3
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)

    @patch('requests.get',side_effect=[mocked_get(status_code=400, json={"key1": "val1"})])
    def test_get_cursor_based_handles_400(self,mock_get):
        try:
            responses = [response for response in http.get_cursor_based(url='some_url',
                                                                    access_token='some_token')]

        except http.ZendeskBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)
            
    @patch('requests.get',side_effect=[mocked_get(status_code=401, json={"key1": "val1"})])
    def test_get_cursor_based_handles_401(self,mock_get):
        try:
            responses = [response for response in http.get_cursor_based(url='some_url',
                                                                    access_token='some_token')]
        except http.ZendeskUnauthorizedError as e:
            expected_error_message = "HTTP-error-code: 401, Error: The access token provided is expired, revoked,"\
                " malformed or invalid for other reasons."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)
            
    @patch('requests.get',side_effect=[mocked_get(status_code=404, json={"key1": "val1"})])
    def test_get_cursor_based_handles_404(self,mock_get):
        try:
            responses = [response for response in http.get_cursor_based(url='some_url',
                                                                    access_token='some_token')]
        except http.ZendeskNotFoundError as e:
            expected_error_message = "HTTP-error-code: 404, Error: There is no help desk configured at this address."\
                " This means that the address is available and that you can claim it at http://www.zendesk.com/signup"
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)
            
            
    @patch('requests.get',side_effect=[mocked_get(status_code=409, json={"key1": "val1"})])
    def test_get_cursor_based_handles_409(self,mock_get):
        try:
            responses = [response for response in http.get_cursor_based(url='some_url',
                                                                    access_token='some_token')]
        except http.ZendeskConflictError as e:
            expected_error_message = "HTTP-error-code: 409, Error: The request does not match our state in some way."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)
            
    @patch('requests.get',side_effect=[mocked_get(status_code=422, json={"key1": "val1"})])
    def test_get_cursor_based_handles_422(self,mock_get):
        try:
            responses = [response for response in http.get_cursor_based(url='some_url',
                                                                    access_token='some_token')]
        except http.ZendeskUnprocessableEntityError as e:
            expected_error_message = "HTTP-error-code: 422, Error: The request content itself is not processable by the server."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)
            
    @patch('requests.get',side_effect=[mocked_get(status_code=500, json={"key1": "val1"})])
    def test_get_cursor_based_handles_500(self,mock_get):
        try:
            responses = [response for response in http.get_cursor_based(url='some_url',
                                                                    access_token='some_token')]
        except http.ZendeskInternalServerError as e:
            expected_error_message = "HTTP-error-code: 500, Error: The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)
            
    @patch('requests.get',side_effect=[mocked_get(status_code=501, json={"key1": "val1"})])
    def test_get_cursor_based_handles_501(self,mock_get):
        try:
            responses = [response for response in http.get_cursor_based(url='some_url',
                                                                    access_token='some_token')]
        except http.ZendeskNotImplementedError as e:
            expected_error_message = "HTTP-error-code: 501, Error: The server does not support the functionality required to fulfill the request."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)
            
    @patch('requests.get',side_effect=[mocked_get(status_code=502, json={"key1": "val1"})])
    def test_get_cursor_based_handles_502(self,mock_get):
        try:
            responses = [response for response in http.get_cursor_based(url='some_url',
                                                                    access_token='some_token')]
        except http.ZendeskBadGatewayError as e:
            expected_error_message = "HTTP-error-code: 502, Error: Server received an invalid response."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)
            
    @patch('requests.get',
           side_effect=[
               mocked_get(status_code=503, headers={'Retry-After': '1'}, json={"key3": "val3", **SINGLE_RESPONSE}),
               mocked_get(status_code=503, headers={'retry-after': 1}, json={"key2": "val2", **SINGLE_RESPONSE}),
               mocked_get(status_code=200, json={"key1": "val1", **SINGLE_RESPONSE}),
           ])
    def test_get_cursor_based_handles_500(self,mock_get):
        """Test that the tap:
        - can handle 503s
        - requests uses a case insensitive dict for the `headers`
        - can handle either a string or an integer for the retry header
        """
        responses = [response for response in http.get_cursor_based(url='some_url',
                                                                    access_token='some_token')]
        actual_response = responses[0]
        self.assertDictEqual({"key1": "val1", **SINGLE_RESPONSE},
                             actual_response)

        expected_call_count = 3
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)
    
    @patch('requests.get')
    def test_get_cursor_based_handles_204(self,mock_get):
        fake_response = requests.models.Response()
        fake_response.status_code = 204
        
        mock_get.side_effect = [fake_response]
        try:
            responses = [response for response in http.get_cursor_based(url='some_url',
                                                                    access_token='some_token')]
        except http.ZendeskError as e:
            expected_error_message =  'HTTP-error-code: 204, Error: Unknown Error'
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)
                
    @patch("tap_zendesk.streams.LOGGER.warning")    
    def test_raise_or_log_zenpy_apiexception(self, mocked_logger):
        schema = {}
        stream = 'test_stream'
        error_string = '{"error": "Forbidden", "description": "You are missing the following required scopes: read"}'
        e = zenpy.lib.exception.APIException(error_string)
        streams.raise_or_log_zenpy_apiexception(schema, stream, e)
        mocked_logger.assert_called_with(
            "The account credentials supplied do not have access to `%s` custom fields.",
            stream)

