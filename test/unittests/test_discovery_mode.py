import unittest
from unittest.mock import MagicMock, Mock, patch
from tap_zendesk import discover, http
import tap_zendesk
import requests
import zenpy

ACCSESS_TOKEN_ERROR = '{"error": "Forbidden", "description": "You are missing the following required scopes: read"}'
API_TOKEN_ERROR = '{"error": {"title": "Forbidden",'\
        '"message": "You do not have access to this page. Please contact the account owner of this help desk for further help."}}'
AUTH_ERROR = '{"error": "Could not authenticate you"}'

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
                mocked_get(status_code=200, json={"tickets": [{"id": "t1"}]}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"})
            ])
    def test_discovery_handles_403__raise_tap_zendesk_forbidden_error(self, mock_get, mock_resolve_schema_references, 
                                mock_load_metadata, mock_load_schema,mock_load_shared_schema_refs, mocked_sla_policies, 
                                mocked_ticket_forms, mock_users, mock_organizations):
        '''
        Test that we handle forbidden error for child streams.
        '''
        try:
            responses = discover.discover_streams('dummy_client', {'subdomain': 'arp', 'access_token': 'dummy_token'})
        except tap_zendesk.http.ZendeskForbiddenError as e:
            expected_error_message =  "HTTP-error-code: 403, Error: You are missing the following required scopes: read. "\
                "The account credentials supplied do not have read access for the following stream(s):  groups, users, "\
                "organizations, ticket_audits, ticket_comments, ticket_fields, ticket_forms, group_memberships, macros, "\
                    "satisfaction_ratings, tags, ticket_metrics"

            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        expected_call_count = 10
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)

        
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
                mocked_get(status_code=200, json={"tickets": [{"id": "t1"}]}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"})
            ])
    def test_discovery_handles_403_raise_zenpy_forbidden_error_for_access_token(self, mock_get, mock_resolve_schema_references, mock_load_metadata, 
                                mock_load_schema,mock_load_shared_schema_refs, mocked_sla_policies, mocked_ticket_forms, 
                                mock_users, mock_organizations):
        '''
        Test that we handle forbidden error received from last failed request which we called from zenpy module and
        raised zenpy.lib.exception.APIException
        '''
        try:
            responses = discover.discover_streams('dummy_client', {'subdomain': 'arp', 'access_token': 'dummy_token'})
        except tap_zendesk.http.ZendeskForbiddenError as e:
            expected_error_message = "HTTP-error-code: 403, Error: You are missing the following required scopes: read. "\
                "The account credentials supplied do not have read access for the following stream(s):  groups, users, "\
                "organizations, ticket_audits, ticket_comments, ticket_fields, ticket_forms, group_memberships, macros, "\
                "satisfaction_ratings, tags, ticket_metrics, sla_policies"

            self.assertEqual(str(e), expected_error_message)

        expected_call_count = 10
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)


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
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=404, json={"key1": "val1"}),
                mocked_get(status_code=404, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=404, json={"key1": "val1"})
            ])
    def test_discovery_handles_403_raise_zenpy_forbidden_error_for_api_token(self, mock_get, mock_resolve_schema_references, 
                                mock_load_metadata, mock_load_schema,mock_load_shared_schema_refs, mocked_sla_policies, 
                                mocked_ticket_forms, mock_users, mock_organizations):
        '''
        Test that we handle forbidden error received from last failed request which we called from zenpy module and
        raised zenpy.lib.exception.APIException
        '''
        try:
            responses = discover.discover_streams('dummy_client', {'subdomain': 'arp', 'access_token': 'dummy_token'})
        except tap_zendesk.http.ZendeskForbiddenError as e:
            expected_error_message = "HTTP-error-code: 403, Error: You are missing the following required scopes: read. "\
                "The account credentials supplied do not have read access for the following stream(s):  tickets, groups, users, "\
                "organizations, ticket_fields, ticket_forms, group_memberships, macros, satisfaction_ratings, tags"

            self.assertEqual(str(e), expected_error_message)

        expected_call_count = 10
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)
        
        
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
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=400, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
            ])
    def test_discovery_handles_except_403_error_requests_module(self, mock_get, mock_resolve_schema_references, 
                                mock_load_metadata, mock_load_schema,mock_load_shared_schema_refs, mocked_sla_policies, 
                                mocked_ticket_forms, mock_users, mock_organizations):
        '''
        Test that it raise error direclty if it is rather than 403
        '''
        try:
            responses = discover.discover_streams('dummy_client', {'subdomain': 'arp', 'access_token': 'dummy_token'})
        except http.ZendeskBadRequestError as e:
            expected_error_message = "HTTP-error-code: 400, Error: A validation exception has occurred."
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)
            
        expected_call_count = 4
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)


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
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=400, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
                mocked_get(status_code=403, json={"key1": "val1"}),
            ])
    def test_discovery_handles_except_403_error_zenpy_module(self, mock_get, mock_resolve_schema_references, 
                                mock_load_metadata, mock_load_schema,mock_load_shared_schema_refs, mocked_sla_policies, 
                                mocked_ticket_forms, mock_users, mock_organizations):
        '''
        Test that discovery mode raise error direclty if it is rather than 403 for zenpy module
        '''
        try:
            responses = discover.discover_streams('dummy_client', {'subdomain': 'arp', 'access_token': 'dummy_token'})
        except zenpy.lib.exception.APIException as e:
            expected_error_message = AUTH_ERROR
            # Verifying the message formed for the custom exception
            self.assertEqual(str(e), expected_error_message)

        expected_call_count = 2
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)
        
        
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
                mocked_get(status_code=200, json={"tickets": [{"id": "t1"}]}),
                mocked_get(status_code=200, json={"key1": "val1"}),
                mocked_get(status_code=200, json={"key1": "val1"}),
                mocked_get(status_code=200, json={"key1": "val1"}),
                mocked_get(status_code=200, json={"key1": "val1"}),
                mocked_get(status_code=200, json={"key1": "val1"}),
                mocked_get(status_code=200, json={"key1": "val1"}),
                mocked_get(status_code=200, json={"key1": "val1"}),
                mocked_get(status_code=200, json={"key1": "val1"}),
                mocked_get(status_code=200, json={"key1": "val1"})
            ])
    def test_discovery_handles_200_response(self, mock_get, mock_resolve_schema_references, 
                                mock_load_metadata, mock_load_schema,mock_load_shared_schema_refs, mocked_sla_policies, 
                                mocked_ticket_forms, mock_users, mock_organizations):
        '''
        Test that discovery mode does not raise any error in case of all streams have read permission
        '''
        discover.discover_streams('dummy_client', {'subdomain': 'arp', 'access_token': 'dummy_token'})

        expected_call_count = 10
        actual_call_count = mock_get.call_count
        self.assertEqual(expected_call_count, actual_call_count)