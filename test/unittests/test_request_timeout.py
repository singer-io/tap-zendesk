import unittest
from unittest.mock import patch
from tap_zendesk import http
import requests

@patch("time.sleep")
class TestRequestTimeoutBackoff(unittest.TestCase):
    """A set of unit tests to ensure that requests are retrying properly for Timeout Error"""   

    @patch('requests.get')
    def test_call_api_handles_timeout_error(self,mock_get, mock_sleep):
        """We mock request method to raise a `Timeout` and expect the tap to retry this up to 10 times,
        """
        mock_get.side_effect = requests.exceptions.Timeout

        try:
            responses = http.call_api(url='some_url', params={}, headers={})
        except requests.exceptions.Timeout as e:
            pass

        # Verify the request retry 10 times on timeout
        self.assertEqual(mock_get.call_count, 10)
        