import unittest
from unittest.mock import MagicMock, Mock, patch
from parameterized import parameterized
from tap_zendesk import discover, http
from tap_zendesk.streams import TalkPhoneNumbers, SLAPolicies, TicketForms, SatisfactionRatings, STREAMS
import tap_zendesk
import requests
import zenpy

ACCSESS_TOKEN_ERROR = '{"error": "Forbidden", "description": "Missing the following required scopes: read"}'
API_TOKEN_ERROR = '{"error": {"title": "Forbidden",'\
        '"message": "Access to this resource is restricted. Please contact the account administrator for assistance."}}'
AUTH_ERROR = '{"error": "Could not authenticate you"}'
START_DATE = "2021-10-30T00:00:00Z"

def mocked_get(*args, **kwargs):
    fake_response = requests.models.Response()
    fake_response.headers.update(kwargs.get('headers', {}))
    fake_response.status_code = kwargs['status_code']

    # We can't set the content or text of the Response directly, so we mock a function
    fake_response.json = Mock()
    fake_response.json.side_effect = lambda:kwargs.get('json', {})

    return fake_response

class TestDiscovery(unittest.TestCase):
    '''
    Test that we can call api for each stream in discovey mode and handle forbidden error.
    '''
    @patch("tap_zendesk.discover.LOGGER.warning")
    @patch('tap_zendesk.streams.TalkPhoneNumbers.check_access')
    @patch('tap_zendesk.streams.TicketMetricEvents.check_access')
    @patch('tap_zendesk.streams.Organizations.check_access',side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch('tap_zendesk.streams.Users.check_access',side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch('tap_zendesk.streams.TicketForms.check_access',side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch('tap_zendesk.streams.SLAPolicies.check_access',side_effect=[mocked_get(status_code=200, json={"key1": "val1"})])
    @patch('tap_zendesk.discover.load_shared_schema_refs', return_value={})
    @patch('tap_zendesk.streams.Stream.load_metadata', return_value={})
    @patch('tap_zendesk.streams.Stream.load_schema', return_value={})
    @patch('singer.resolve_schema_references', return_value={})
    @patch('requests.get',
           side_effect=[
                mocked_get(status_code=200, json={"tickets": [{"id": "t1"}]}), # Response of the 1st get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 2nd get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 3rd get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 4th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 5th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 6th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 7th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 8th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 9th get request call
                mocked_get(status_code=403, json={"key1": "val1"}) # Response of the 10th get request call
            ])
    def test_discovery_handles_403__raise_tap_zendesk_forbidden_error(self, mock_get, mock_resolve_schema_references,
                                mock_load_metadata, mock_load_schema,mock_load_shared_schema_refs, mocked_sla_policies,
                                mocked_ticket_forms, mock_users, mock_organizations, mocked_ticket_metric_events, mocked_talk_phone_numbers, mock_logger):
        '''
        Test that we handle forbidden error for child streams. discover_streams calls check_access for each stream to
        check the read perission. discover_streams call many other methods including load_shared_schema_refs, load_metadata,
        load_schema, resolve_schema_references also which we mock to test forbidden error. We mock check_access method of
        some of stream method which call request of zenpy module and also mock get method of requests module with 200, 403 error.

        '''
        discover.discover_streams('dummy_client', {'subdomain': 'arp', 'access_token': 'dummy_token', 'start_date':START_DATE})
        expected_call_count = 8
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)

        # Verifying the logger message
        mock_logger.assert_called_with("The account credentials supplied do not have 'read' access to the following stream(s): "\
            "groups, users, organizations, ticket_audits, ticket_fields, ticket_forms, group_memberships, macros, "\
            "tags. The data for these streams would not be collected due to lack of required "\
            "permission.")

    @patch("tap_zendesk.discover.LOGGER.warning")
    @patch('tap_zendesk.streams.TalkPhoneNumbers.check_access')
    @patch('tap_zendesk.streams.TicketMetricEvents.check_access')
    @patch('tap_zendesk.streams.Organizations.check_access',side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch('tap_zendesk.streams.Users.check_access',side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch('tap_zendesk.streams.TicketForms.check_access',side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch('tap_zendesk.streams.SLAPolicies.check_access',side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch('tap_zendesk.discover.load_shared_schema_refs', return_value={})
    @patch('tap_zendesk.streams.Stream.load_metadata', return_value={})
    @patch('tap_zendesk.streams.Stream.load_schema', return_value={})
    @patch('singer.resolve_schema_references', return_value={})
    @patch('requests.get',
           side_effect=[
                mocked_get(status_code=200, json={"tickets": [{"id": "t1"}]}), # Response of the 1st get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 2nd get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 3rd get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 4th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 5th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 6th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 7th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 8th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 9th get request call
                mocked_get(status_code=403, json={"key1": "val1"}) # Response of the 10th get request call
            ])
    def test_discovery_handles_403_raise_zenpy_forbidden_error_for_access_token(self, mock_get, mock_resolve_schema_references, mock_load_metadata,
                                mock_load_schema,mock_load_shared_schema_refs, mocked_sla_policies, mocked_ticket_forms,
                                mock_users, mock_organizations, mocked_ticket_metric_events, mocked_talk_phone_numbers, mock_logger):
        '''
        Test that we handle forbidden error received from last failed request which we called from zenpy module and
        log proper warning message. discover_streams calls check_access for each stream to check the
        read perission. discover_streams call many other methods including load_shared_schema_refs, load_metadata,
        load_schema, resolve_schema_references also which we mock to test forbidden error. We mock check_access method of
        some of stream method which call request of zenpy module and also mock get method of requests module with 200, 403 error.
        '''
        discover.discover_streams('dummy_client', {'subdomain': 'arp', 'access_token': 'dummy_token', 'start_date':START_DATE})

        expected_call_count = 8
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)

        # Verifying the logger message
        mock_logger.assert_called_with("The account credentials supplied do not have 'read' access to the following stream(s): "\
            "groups, users, organizations, ticket_audits, ticket_fields, ticket_forms, group_memberships, macros, "\
            "tags, sla_policies. The data for these streams would not be collected due to "\
            "lack of required permission.")

    @patch("tap_zendesk.discover.LOGGER.warning")
    @patch('tap_zendesk.streams.TalkPhoneNumbers.check_access')
    @patch('tap_zendesk.streams.TicketMetricEvents.check_access')
    @patch('tap_zendesk.streams.Organizations.check_access',side_effect=zenpy.lib.exception.APIException(API_TOKEN_ERROR))
    @patch('tap_zendesk.streams.Users.check_access',side_effect=zenpy.lib.exception.APIException(API_TOKEN_ERROR))
    @patch('tap_zendesk.streams.TicketForms.check_access',side_effect=zenpy.lib.exception.APIException(API_TOKEN_ERROR))
    @patch('tap_zendesk.streams.SLAPolicies.check_access',side_effect=[mocked_get(status_code=200, json={"key1": "val1"})])
    @patch('tap_zendesk.discover.load_shared_schema_refs', return_value={})
    @patch('tap_zendesk.streams.Stream.load_metadata', return_value={})
    @patch('tap_zendesk.streams.Stream.load_schema', return_value={})
    @patch('singer.resolve_schema_references', return_value={})
    @patch('requests.get',
           side_effect=[
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 1st get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 2nd get request call
                mocked_get(status_code=404, json={"key1": "val1"}), # Response of the 4th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 5th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 6th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 7th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 8th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 9th get request call
                mocked_get(status_code=404, json={"key1": "val1"}) # Response of the 10th get request call
            ])
    def test_discovery_handles_403_raise_zenpy_forbidden_error_for_api_token(self, mock_get, mock_resolve_schema_references, 
                                mock_load_metadata, mock_load_schema,mock_load_shared_schema_refs, mocked_sla_policies, 
                                mocked_ticket_forms, mock_users, mock_organizations, mocked_ticket_metric_events, mocked_talk_phone_numbers, mock_logger):
        '''
        Test that we handle forbidden error received from last failed request which we called from zenpy module and
        log proper warning message. discover_streams calls check_access for each stream to check the 
        read perission. discover_streams call many other methods including load_shared_schema_refs, load_metadata, 
        load_schema, resolve_schema_references also which we mock to test forbidden error. We mock check_access method of 
        some of stream method which call request of zenpy module and also mock get method of requests module with 200, 403 error.
        '''

        responses = discover.discover_streams('dummy_client', {'subdomain': 'arp', 'access_token': 'dummy_token', 'start_date':START_DATE})
        expected_call_count = 8
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)

        # Verifying the logger message
        mock_logger.assert_called_with("The account credentials supplied do not have 'read' access to the following stream(s): "\
            "tickets, groups, users, organizations, ticket_fields, ticket_forms, group_memberships, macros, "\
            "tags. The data for these streams would not be collected due to lack of required permission.")

    @patch('tap_zendesk.streams.TalkPhoneNumbers.check_access')
    @patch('tap_zendesk.streams.TicketMetricEvents.check_access')
    @patch('tap_zendesk.streams.Organizations.check_access',side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch('tap_zendesk.streams.Users.check_access',side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch('tap_zendesk.streams.TicketForms.check_access',side_effect=zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR))
    @patch('tap_zendesk.streams.SLAPolicies.check_access',side_effect=[mocked_get(status_code=200, json={"key1": "val1"})])
    @patch('tap_zendesk.discover.load_shared_schema_refs', return_value={})
    @patch('tap_zendesk.streams.Stream.load_metadata', return_value={})
    @patch('tap_zendesk.streams.Stream.load_schema', return_value={})
    @patch('singer.resolve_schema_references', return_value={})
    @patch('requests.get',
           side_effect=[
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 1st get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 2nd get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 3rd get request call
                mocked_get(status_code=400, json={"key1": "val1"}), # Response of the 4th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 5th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 6th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 7th get request call
            ])
    def test_discovery_handles_except_403_error_requests_module(self, mock_get, mock_resolve_schema_references, 
                                mock_load_metadata, mock_load_schema,mock_load_shared_schema_refs, mocked_sla_policies, 
                                mocked_ticket_forms, mock_users, mock_organizations, mocked_ticket_metric_events, mocked_talk_phone_numbers):
        '''
        Test that function raises error directly if error code is other than 403. discover_streams calls check_access for each 
        stream to check the read perission. discover_streams call many other methods including load_shared_schema_refs, load_metadata, 
        load_schema, resolve_schema_references also which we mock to test forbidden error. We mock check_access method of 
        some of stream method which call request of zenpy module and also mock get method of requests module with 200, 403 error.
        '''
        try:
            responses = discover.discover_streams('dummy_client', {'subdomain': 'arp', 'access_token': 'dummy_token', 'start_date':START_DATE})
        except http.ZendeskBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        expected_call_count = 4
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)


    @patch('tap_zendesk.streams.TalkPhoneNumbers.check_access')
    @patch('tap_zendesk.streams.TicketMetricEvents.check_access')
    @patch('tap_zendesk.streams.Organizations.check_access',side_effect=zenpy.lib.exception.APIException(AUTH_ERROR))
    @patch('tap_zendesk.streams.Users.check_access',side_effect=zenpy.lib.exception.APIException(AUTH_ERROR))
    @patch('tap_zendesk.streams.TicketForms.check_access',side_effect=zenpy.lib.exception.APIException(AUTH_ERROR))
    @patch('tap_zendesk.streams.SLAPolicies.check_access',side_effect=[mocked_get(status_code=200, json={"key1": "val1"})])
    @patch('tap_zendesk.discover.load_shared_schema_refs', return_value={})
    @patch('tap_zendesk.streams.Stream.load_metadata', return_value={})
    @patch('tap_zendesk.streams.Stream.load_schema', return_value={})
    @patch('singer.resolve_schema_references', return_value={})
    @patch('requests.get',
           side_effect=[
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 1st get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 2nd get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 3rd get request call
                mocked_get(status_code=400, json={"key1": "val1"}), # Response of the 4th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 5th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 6th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 7th get request call
            ])
    def test_discovery_handles_except_403_error_zenpy_module(self, mock_get, mock_resolve_schema_references, 
                                mock_load_metadata, mock_load_schema,mock_load_shared_schema_refs, mocked_sla_policies, 
                                mocked_ticket_forms, mock_users, mock_organizations, mocked_ticket_metric_events, mocked_talk_phone_numbers):
        '''
        Test that discovery mode raise error direclty if it is rather than 403 for request zenpy module. discover_streams call 
        many other methods including load_shared_schema_refs, load_metadata, load_schema, resolve_schema_references
        also which we mock to test forbidden error. We mock check_access method of some of stream method which
        call request of zenpy module and also mock get method of requests module with 400, 403 error.
        '''
        try:
            responses = discover.discover_streams('dummy_client', {'subdomain': 'arp', 'access_token': 'dummy_token', 'start_date':START_DATE})
        except zenpy.lib.exception.APIException as e:
            expected_error_message = AUTH_ERROR
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        expected_call_count = 2
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)

    @patch('tap_zendesk.streams.TalkPhoneNumbers.check_access')
    @patch('tap_zendesk.streams.TicketMetricEvents.check_access')
    @patch('tap_zendesk.streams.Organizations.check_access',side_effect=[mocked_get(status_code=200, json={"key1": "val1"})])
    @patch('tap_zendesk.streams.Users.check_access',side_effect=[mocked_get(status_code=200, json={"key1": "val1"})])
    @patch('tap_zendesk.streams.TicketForms.check_access',side_effect=[mocked_get(status_code=200, json={"key1": "val1"})])
    @patch('tap_zendesk.streams.SLAPolicies.check_access',side_effect=[mocked_get(status_code=200, json={"key1": "val1"})])
    @patch('tap_zendesk.discover.load_shared_schema_refs', return_value={})
    @patch('tap_zendesk.streams.Stream.load_metadata', return_value={})
    @patch('tap_zendesk.streams.Stream.load_schema', return_value={})
    @patch('singer.resolve_schema_references', return_value={})
    @patch('requests.get',
           side_effect=[
                mocked_get(status_code=200, json={"tickets": [{"id": "t1"}]}), # Response of the 1st get request call
                mocked_get(status_code=200, json={"key1": "val1"}), # Response of the 1st get request call
                mocked_get(status_code=200, json={"key1": "val1"}), # Response of the 1st get request call
                mocked_get(status_code=200, json={"key1": "val1"}), # Response of the 1st get request call
                mocked_get(status_code=200, json={"key1": "val1"}), # Response of the 1st get request call
                mocked_get(status_code=200, json={"key1": "val1"}), # Response of the 1st get request call
                mocked_get(status_code=200, json={"key1": "val1"}), # Response of the 1st get request call
                mocked_get(status_code=200, json={"key1": "val1"}), # Response of the 1st get request call
                mocked_get(status_code=200, json={"key1": "val1"}), # Response of the 1st get request call
                mocked_get(status_code=200, json={"key1": "val1"}) # Response of the 1st get request call
            ])
    def test_discovery_handles_200_response(self, mock_get, mock_resolve_schema_references, 
                                mock_load_metadata, mock_load_schema,mock_load_shared_schema_refs, mocked_sla_policies, 
                                mocked_ticket_forms, mock_users, mock_organizations, mocked_ticket_metric_events, mocked_talk_phone_numbers):
        '''
        Test that discovery mode does not raise any error in case of all streams have read permission
        '''
        discover.discover_streams('dummy_client', {'subdomain': 'arp', 'access_token': 'dummy_token', 'start_date':START_DATE})

        expected_call_count = 8
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)

    @patch("tap_zendesk.discover.LOGGER.warning")
    @patch('tap_zendesk.streams.TalkPhoneNumbers.check_access')
    @patch('tap_zendesk.streams.TicketMetricEvents.check_access')
    @patch('tap_zendesk.streams.Organizations.check_access',side_effect=zenpy.lib.exception.APIException(API_TOKEN_ERROR))
    @patch('tap_zendesk.streams.Users.check_access',side_effect=zenpy.lib.exception.APIException(API_TOKEN_ERROR))
    @patch('tap_zendesk.streams.TicketForms.check_access',side_effect=zenpy.lib.exception.APIException(API_TOKEN_ERROR))
    @patch('tap_zendesk.streams.SLAPolicies.check_access',side_effect=zenpy.lib.exception.APIException(API_TOKEN_ERROR))
    @patch('tap_zendesk.discover.load_shared_schema_refs', return_value={})
    @patch('tap_zendesk.streams.Stream.load_metadata', return_value={})
    @patch('tap_zendesk.streams.Stream.load_schema', return_value={})
    @patch('singer.resolve_schema_references', return_value={})
    @patch('requests.get',
           side_effect=[
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 1st get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 2nd get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 3rd get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 4th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 5th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 6th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 7th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 8th get request call
                mocked_get(status_code=403, json={"key1": "val1"}), # Response of the 9th get request call
                mocked_get(status_code=403, json={"key1": "val1"}) # Response of the 10th get request call
            ])
    def test_discovery_handles_403_for_all_streams_api_token(self, mock_get, mock_resolve_schema_references, 
                                mock_load_metadata, mock_load_schema,mock_load_shared_schema_refs, mocked_sla_policies, 
                                mocked_ticket_forms, mock_users, mock_organizations, mocked_ticket_metric_events, mocked_talk_phone_numbers, mock_logger):
        '''
        Test that we handle forbidden error received from all streams and raise the ZendeskForbiddenError
        with proper error message. discover_streams calls check_access for each stream to check the 
        read perission. discover_streams call many other methods including load_shared_schema_refs, load_metadata, 
        load_schema, resolve_schema_references also which we mock to test forbidden error. We mock check_access method of 
        some of stream method which call request of zenpy module and also mock get method of requests module with 200, 403 error.
        '''
        try:
            responses = discover.discover_streams('dummy_client', {'subdomain': 'arp', 'access_token': 'dummy_token', 'start_date':START_DATE})
        except http.ZendeskForbiddenError as e:
            expected_message = "HTTP-error-code: 403, Error: The account credentials supplied do not have 'read' access to any "\
            "of streams supported by the tap. Data collection cannot be initiated due to lack of permissions."
            # # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_message)


class TestOptionalStreamDiscovery(unittest.TestCase):
    '''
    Tests for the optional/essential stream distinction introduced to fix the customer-reported
    issue: a 403 on a plan-tier or add-on stream (e.g. talk_phone_numbers, sla_policies,
    ticket_forms, satisfaction_ratings) must NOT block connection creation.
    '''

    # -------------------------------------------------------------------------
    # Test 1: Exact customer-reported scenario
    #   OAuth client has no Zendesk Talk access → phone_numbers endpoint returns 403.
    #   After our fix, TalkPhoneNumbers.check_access converts the HTTPError to
    #   ZendeskForbiddenError. discover.py should exclude the stream silently and
    #   continue – the connection must succeed.
    # -------------------------------------------------------------------------
    @patch('tap_zendesk.discover.LOGGER.warning')
    @patch('tap_zendesk.streams.TalkPhoneNumbers.check_access',
           side_effect=http.ZendeskForbiddenError(
               '403 Client Error: Forbidden for url: '
               'https://testaccount.zendesk.com/api/v2/channels/voice/phone_numbers.json'))
    @patch('tap_zendesk.streams.SLAPolicies.check_access')         # succeeds
    @patch('tap_zendesk.streams.TicketForms.check_access')          # succeeds
    @patch('tap_zendesk.streams.TicketMetricEvents.check_access')   # succeeds
    @patch('tap_zendesk.streams.Organizations.check_access')        # succeeds
    @patch('tap_zendesk.streams.Users.check_access')                # succeeds
    @patch('tap_zendesk.discover.load_shared_schema_refs', return_value={})
    @patch('tap_zendesk.streams.Stream.load_metadata', return_value={})
    @patch('tap_zendesk.streams.Stream.load_schema', return_value={})
    @patch('singer.resolve_schema_references', return_value={})
    @patch('requests.get', side_effect=[
        mocked_get(status_code=200, json={'tickets': [{'id': 't1'}]}),  # tickets
        mocked_get(status_code=200, json={'groups': []}),               # groups
        mocked_get(status_code=200, json={}),                           # ticket_audits
        mocked_get(status_code=200, json={}),                           # ticket_fields
        mocked_get(status_code=200, json={}),                           # group_memberships
        mocked_get(status_code=200, json={}),                           # macros
        mocked_get(status_code=200, json={}),                           # satisfaction_ratings
        mocked_get(status_code=200, json={}),                           # tags
    ])
    def test_talk_phone_numbers_403_excluded_connection_succeeds(
            self, mock_get, mock_resolve_schema_references, mock_load_schema,
            mock_load_metadata, mock_load_shared_schema_refs,
            mock_users, mock_organizations, mock_ticket_metric_events,
            mock_ticket_forms, mock_sla_policies, mock_talk_phone_numbers,
            mock_logger):
        '''
        Replicates the exact customer-reported failure:
          "Failed to create connection
           403 Client Error: Forbidden for url: .../api/v2/channels/voice/phone_numbers.json"

        The OAuth client has no Zendesk Talk product access. After the fix,
        TalkPhoneNumbers.check_access raises ZendeskForbiddenError (converted from
        requests.HTTPError). discover.py must:
          1. NOT raise an exception (connection succeeds).
          2. Exclude talk_phone_numbers from the returned catalog.
          3. Log a warning identifying the excluded stream.
        '''
        result = discover.discover_streams(
            'dummy_client',
            {'subdomain': 'arp', 'access_token': 'dummy_token', 'start_date': START_DATE}
        )

        # Connection must succeed – all streams except talk_phone_numbers are returned.
        # Derive the expected count from STREAMS so this test isn't broken by unrelated stream additions.
        excluded_streams = {'talk_phone_numbers'}
        stream_names = [s['stream'] for s in result]
        self.assertNotIn('talk_phone_numbers', stream_names)
        self.assertEqual(len(result), len(STREAMS) - len(excluded_streams))

        # Warning must be logged for the excluded optional stream
        mock_logger.assert_any_call(
            "Stream '%s' is not available for this account (plan tier or add-on not "
            "provisioned). It will be excluded from the available streams.",
            'talk_phone_numbers'
        )

    # -------------------------------------------------------------------------
    # Test 2: All four optional streams return 403
    #   talk_phone_numbers (Talk add-on), sla_policies (plan-tier),
    #   ticket_forms (plan-tier), satisfaction_ratings (plan-tier) all unavailable.
    #   Connection must still succeed and all four must be absent from the catalog.
    # -------------------------------------------------------------------------
    @patch('tap_zendesk.discover.LOGGER.warning')
    @patch('tap_zendesk.streams.TalkPhoneNumbers.check_access',
           side_effect=http.ZendeskForbiddenError('403 Forbidden: talk not provisioned'))
    @patch('tap_zendesk.streams.SLAPolicies.check_access',
           side_effect=http.ZendeskForbiddenError('403 Forbidden: sla not on plan'))
    @patch('tap_zendesk.streams.TicketForms.check_access',
           side_effect=http.ZendeskForbiddenError('403 Forbidden: ticket_forms not on plan'))
    @patch('tap_zendesk.streams.TicketMetricEvents.check_access')   # succeeds
    @patch('tap_zendesk.streams.Organizations.check_access')        # succeeds
    @patch('tap_zendesk.streams.Users.check_access')                # succeeds
    @patch('tap_zendesk.discover.load_shared_schema_refs', return_value={})
    @patch('tap_zendesk.streams.Stream.load_metadata', return_value={})
    @patch('tap_zendesk.streams.Stream.load_schema', return_value={})
    @patch('singer.resolve_schema_references', return_value={})
    @patch('requests.get', side_effect=[
        mocked_get(status_code=200, json={'tickets': [{'id': 't1'}]}),  # tickets
        mocked_get(status_code=200, json={}),                           # groups
        mocked_get(status_code=200, json={}),                           # ticket_audits
        mocked_get(status_code=200, json={}),                           # ticket_fields
        mocked_get(status_code=200, json={}),                           # group_memberships
        mocked_get(status_code=200, json={}),                           # macros
        mocked_get(status_code=403, json={}),                           # satisfaction_ratings (optional, 403 → excluded)
        mocked_get(status_code=200, json={}),                           # tags
    ])
    def test_all_optional_streams_403_connection_still_succeeds(
            self, mock_get, mock_resolve_schema_references, mock_load_schema,
            mock_load_metadata, mock_load_shared_schema_refs,
            mock_users, mock_organizations, mock_ticket_metric_events,
            mock_ticket_forms, mock_sla_policies, mock_talk_phone_numbers,
            mock_logger):
        '''
        When all four optional/plan-tier streams (talk_phone_numbers, sla_policies,
        ticket_forms, satisfaction_ratings) return 403, the connection must still
        succeed and all four must be excluded from the catalog. Essential streams
        (tickets, groups, users, organizations, etc.) remain fully accessible.
        '''
        result = discover.discover_streams(
            'dummy_client',
            {'subdomain': 'arp', 'access_token': 'dummy_token', 'start_date': START_DATE}
        )

        # Connection succeeds: all optional streams excluded, essential streams remain.
        # Derive the expected count from STREAMS so this test isn't broken by unrelated stream additions.
        optional_streams = {'talk_phone_numbers', 'sla_policies', 'ticket_forms', 'satisfaction_ratings'}
        stream_names = [s['stream'] for s in result]
        self.assertEqual(len(result), len(STREAMS) - len(optional_streams))

        # All four optional streams must be absent
        for stream in optional_streams:
            self.assertNotIn(stream, stream_names)

        # A warning must have been logged for each excluded optional stream
        for stream in optional_streams:
            mock_logger.assert_any_call(
                "Stream '%s' is not available for this account (plan tier or add-on not "
                "provisioned). It will be excluded from the available streams.",
                stream
            )


class TestCheckAccessOptionalStreams(unittest.TestCase):
    '''
    Unit tests for the check_access() fixes on the three streams affected by the
    customer-reported bug:
      - TalkPhoneNumbers: Zenpy Talk raises requests.HTTPError directly (not ZendeskForbiddenError)
      - SLAPolicies:      Zenpy native client raises APIException
      - TicketForms:      Zenpy native client raises APIException
    All three must convert their native exceptions to ZendeskForbiddenError so
    discover.py can handle them uniformly via the is_optional flag.
    '''

    CONFIG = {
        'subdomain': 'testaccount',
        'access_token': 'test_token',
        'start_date': START_DATE,
    }

    # -----------------------------------------------------------------------
    # Parameterized: Zenpy native-client APIException → ZendeskForbiddenError
    # Covers SLAPolicies and TicketForms which both use self.client.<method>()
    # and must convert APIException to ZendeskForbiddenError.
    # -----------------------------------------------------------------------
    @parameterized.expand([
        ('sla_policies',  SLAPolicies,  'sla_policies'),
        ('ticket_forms',  TicketForms,  'ticket_forms'),
    ])
    def test_zenpy_api_exception_raises_zendesk_forbidden(self, _name, stream_cls, client_attr):
        '''
        Plan-tier streams (SLAPolicies, TicketForms) use the Zenpy native client.
        A 403 surfaces as APIException; check_access() must re-raise it as
        ZendeskForbiddenError for uniform handling in discover.py.
        '''
        client = MagicMock()
        getattr(client, client_attr).side_effect = zenpy.lib.exception.APIException(ACCSESS_TOKEN_ERROR)
        stream = stream_cls(client, self.CONFIG)

        with self.assertRaises(http.ZendeskForbiddenError):
            stream.check_access()

    # -----------------------------------------------------------------------
    # Parameterized: TalkPhoneNumbers check_access HTTP error handling
    # The Zenpy Talk client calls response.raise_for_status() internally,
    # so it surfaces errors as requests.HTTPError rather than ZendeskForbiddenError.
    # -----------------------------------------------------------------------
    @parameterized.expand([
        (
            'http_403_converted_to_zendesk_forbidden',
            403,
            '403 Client Error: Forbidden for url: '
            'https://testaccount.zendesk.com/api/v2/channels/voice/phone_numbers.json',
            http.ZendeskForbiddenError,
        ),
        (
            'http_non_403_reraises_unchanged',
            500,
            '500 Server Error: Internal Server Error',
            requests.exceptions.HTTPError,
        ),
    ])
    def test_talk_phone_numbers_http_error(self, _name, status_code, error_msg, expected_exc):
        '''
        TalkPhoneNumbers.check_access() calls the Zenpy Talk client which raises
        requests.HTTPError for HTTP errors:
          - 403 must be converted to ZendeskForbiddenError (customer-reported scenario).
          - Non-403 errors (e.g. 500) must propagate unchanged.
        '''
        client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = status_code
        client.talk.phone_numbers.side_effect = requests.exceptions.HTTPError(
            error_msg, response=mock_response
        )
        stream = TalkPhoneNumbers(client, self.CONFIG)

        with self.assertRaises(expected_exc):
            stream.check_access()

    def test_talk_phone_numbers_404_silently_passes(self):
        '''
        ZendeskNotFoundError (404) should be silently ignored if the account simply
        has no phone numbers configured, which is not a permissions problem.
        '''
        client = MagicMock()
        client.talk.phone_numbers.side_effect = http.ZendeskNotFoundError('Not Found')
        stream = TalkPhoneNumbers(client, self.CONFIG)

        stream.check_access()

    # -----------------------------------------------------------------------
    # SatisfactionRatings: base class check_access() via http.call_api
    # Confirms the implicit contract: http.call_api → raise_for_error maps
    # HTTP 403 to ZendeskForbiddenError, so no dedicated override is needed.
    # -----------------------------------------------------------------------
    def test_satisfaction_ratings_403_raises_zendesk_forbidden(self):
        '''
        SatisfactionRatings.is_optional=True relies on the base Stream.check_access()
        which calls http.call_api(). raise_for_error() maps HTTP 403 directly to
        ZendeskForbiddenError via ERROR_CODE_EXCEPTION_MAPPING — no override is
        needed. This test explicitly guards that contract so the implicit dependency
        on http.call_api behaviour is visible and regression-protected.
        '''
        with patch('requests.get', return_value=mocked_get(
                status_code=403, json={'error': 'Forbidden'})):
            stream = SatisfactionRatings(MagicMock(), self.CONFIG)
            with self.assertRaises(http.ZendeskForbiddenError):
                stream.check_access()

    def test_satisfaction_ratings_non_403_reraises_unchanged(self):
        '''
        A non-403 error raised by http.call_api must propagate unchanged.
        We patch http.call_api directly to avoid triggering the backoff decorator
        (which retries 5xx errors up to 10 times, causing the test to hang).
        '''
        with patch('tap_zendesk.streams.http.call_api',
                   side_effect=http.ZendeskInternalServerError('500 Server Error')):
            stream = SatisfactionRatings(MagicMock(), self.CONFIG)
            with self.assertRaises(http.ZendeskInternalServerError):
                stream.check_access()
