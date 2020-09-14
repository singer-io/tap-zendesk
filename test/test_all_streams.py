import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from functools import reduce
from singer import metadata
from base import ZendeskTest

class ZendeskAllStreams(ZendeskTest):
    def name(self):
        return "tap_tester_zendesk_all_streams"

    def expected_sync_streams(self):
        return {
            "tickets",
            "groups",
            "users",
            "organizations",
            "ticket_audits",
            "ticket_fields",
            "group_memberships",
            "macros",
            #"tags",
            "ticket_metrics",
        }

    def expected_pks(self):
        return {
            "tickets": {"id"},
            "groups": {"id"},
            "users": {"id"},
            "organizations": {"id"},
            "ticket_audits": {"id"},
            "ticket_fields": {"id"},
            "group_memberships": {"id"},
            "macros": {"id"},
            #"tags": {"name"},
            "ticket_metrics": {"id"},
        }

    def do_test(self, conn_id):
        # Select our catalogs
        our_catalogs = [c for c in self.found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]
        for c in our_catalogs:
            c_annotated = menagerie.get_annotated_schema(conn_id, c['stream_id'])
            c_metadata = metadata.to_map(c_annotated['metadata'])
            connections.select_catalog_and_fields_via_metadata(conn_id, c, c_annotated, [], [])

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

        # Ensure all records have a value for PK(s)
        records = runner.get_records_from_target_output()
        for stream in self.expected_sync_streams():
            messages = records.get(stream,{}).get('messages',[])
            if stream in  ['tickets', 'groups', 'users']:
                self.assertGreater(len(messages), 100, msg="Stream {} has fewer than 100 records synced".format(stream))
            for m in messages:
                pk_set = self.expected_pks()[stream]
                for pk in pk_set:
                    self.assertIsNotNone(m.get('data', {}).get(pk), msg="Missing primary-key for message {}".format(m))

