import unittest
from unittest.mock import MagicMock, Mock, patch
from tap_zendesk import http
import requests


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
        #Verify actual response of cursor based gets is equall to SINGLE_RESPONSE
        self.assertDictEqual(SINGLE_RESPONSE,
                             actual_response)

        expected_call_count = 1
        actual_call_count = mock_get.call_count
        #Verify actual_call_count is only 1
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

        #Verify response of 1st call have expected pagination attribute
        self.assertDictEqual({"key1": "val1", **PAGINATE_RESPONSE},
                              responses[0])
        #Verifi response of 2nd call has expected SINGLE_RESPONSE
        self.assertDictEqual({"key2": "val2", **SINGLE_RESPONSE},
                              responses[1])

        expected_call_count = 2
        actual_call_count = mock_get.call_count
        #Verify actual call count of api is 2
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
        #Verify get_cursor_based can retry 429 error and actual_call_count is expected.
        self.assertEqual(expected_call_count, actual_call_count)
