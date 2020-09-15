import unittest
import os
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

class ZendeskTest(unittest.TestCase):
    def name(self):
        return "test_name"

    """ Handle default authorization info for tests to subclass. """
    def tap_name(self):
        return "tap-zendesk"

    def get_type(self):
        return "platform.zendesk"


    def required_environment_variables(self):
        return set(['TAP_ZENDESK_CLIENT_ID',
                    'TAP_ZENDESK_CLIENT_SECRET',
                    'TAP_ZENDESK_ACCESS_TOKEN',
                    ])

    def setUp(self):
        missing_envs = [x for x in self.required_environment_variables() if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Missing environment variables, please set {}." .format(missing_envs))

    def get_properties(self):
       return {
           'start_date' : '2017-01-01T00:00:00Z',
           'subdomain': 'rjmdev',
           'marketplace_app_id': int(os.getenv('TAP_ZENDESK_MARKETPLACE_APP_ID')) or 0,
           'marketplace_name': os.getenv('TAP_ZENDESK_MARKETPLACE_NAME') or "",
           'marketplace_organization_id': int(os.getenv('TAP_ZENDESK_MARKETPLACE_ORGANIZATION_ID')) or 0,
           'search_window_size':  '2592000'# seconds in a month
        }

    def get_credentials(self):
        return {'access_token': os.getenv('TAP_ZENDESK_ACCESS_TOKEN'),
                'client_id': os.getenv('TAP_ZENDESK_CLIENT_ID'),
                'client_secret': os.getenv('TAP_ZENDESK_CLIENT_SECRET')}

    def expected_check_streams(self):
        return {
            'groups',
            'group_memberships',
            'macros',
            'organizations',
            'satisfaction_ratings',
            'sla_policies',
            'tags',
            'ticket_comments',
            'ticket_fields',
            'ticket_forms',
            'ticket_metrics',
            'tickets',
            'users',
            'ticket_audits'
        }

    def test_run(self):
        # Default test setup
        # Create the connection for Zendesk
        conn_id = connections.ensure_connection(self)

        # Run a check job using orchestrator
        check_job_name = runner.run_check_mode(self, conn_id)

        # Assert that the check job succeeded
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify schemas discovered were discovered
        self.found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertEqual(len(self.found_catalogs), len(self.expected_check_streams()), msg="unable to locate schemas for connection {}".format(conn_id))

        # Verify the schemas discovered were exactly what we expect
        found_catalog_names = set(map(lambda c: c['tap_stream_id'], self.found_catalogs))
        subset = self.expected_check_streams().issubset( found_catalog_names )
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")

        self.do_test(conn_id)

    def do_test(self, conn_id):
        pass
