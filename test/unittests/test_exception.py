import unittest
from tap_zendesk import get_session
from unittest import mock
from tap_zendesk.streams import raise_or_log_zenpy_apiexception, zenpy, json


class ValueError(Exception):
    def __init__(self, m):
        self.message = m

    def __str__(self):
        return self.message


class TestException(unittest.TestCase):
    @mock.patch("tap_zendesk.streams.LOGGER.warning")
    def test_exception_logger(self, mocked_logger):
        schema = {}
        stream = 'test_stream'
        error_string = '{"error":{"message": "You do not have access to this page. Please contact the account owner of this help desk for further help."}' + "}"
        json_object = json.dumps(error_string)
        e = zenpy.lib.exception.APIException(error_string)
        raise_or_log_zenpy_apiexception(schema, stream, e)
        mocked_logger.assert_called_with(
            "The account credentials supplied do not have access to `%s` custom fields.",
            stream)
        
