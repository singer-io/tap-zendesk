import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
from base import ZendeskTest
import unittest
from functools import reduce
from singer import metadata
from zenpy import Zenpy
from zenpy.lib.api_objects import Group, Organization

class ZendeskMinimalSelection(ZendeskTest):
    def name(self):
        return "tap_tester_zendesk_minimal_selection"

    def expected_sync_streams(self):
        return {
            'users'
        }

    def expected_pks(self):
        return {
            'users': {'id'}
        }

    def do_test(self, conn_id):
        # Select our catalogs
        our_catalogs = [c for c in self.found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]
        for c in our_catalogs:
            c_annotated = menagerie.get_annotated_schema(conn_id, c['stream_id'])
            c_metadata = metadata.to_map(c_annotated['metadata'])
            # Tags table only has name and count columns; don't select count
            connections.select_catalog_and_fields_via_metadata(conn_id, c, c_annotated, [], ['name'])

        # Clear state before our run
        menagerie.set_state(conn_id, {})

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        # Ensure all records are retrieving the sub set of fields
        records = runner.get_records_from_target_output()
        for stream in self.expected_sync_streams():
            messages = records.get(stream).get('messages')
            for m in messages:
                pk_set = self.expected_pks()[stream]
                for pk in pk_set:
                    self.assertIsNotNone(m.get('data', {}).get(pk), msg="Missing primary-key for message {}".format(m))

