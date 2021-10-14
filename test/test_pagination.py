import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
from base import ZendeskTest


class ZendeskPagination(ZendeskTest):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """
    API_LIMIT = 100
    def name(self):
        return "zendesk_pagination_test"

    def test_run(self):
        """
        • Verify that for each stream you can get multiple pages of data.  
        This requires we ensure more than 1 page of data exists at all times for any given stream.
        • Verify by pks that the data replicated matches the data we expect.

        """
        
        # Streams to verify all fields tests
        expected_streams = self.expected_check_streams()
        #Skip satisfaction_ratings streams as only end user of tickets can create satisfaction_ratings
        expected_streams = expected_streams - {"satisfaction_ratings"}
        
        expected_automatic_fields = self.expected_automatic_fields()
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields)

        # grab metadata after performing table-and-field selection to set expectations
        # used for asserting all fields are replicated
        stream_to_all_catalog_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(
                fields_from_field_level_md)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        actual_fields_by_stream = runner.examine_target_output_for_fields()

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                
                # verify that we can paginate with all fields selected
                record_count_sync = record_count_by_stream.get(stream, 0)
                self.assertGreater(record_count_sync, self.API_LIMIT,
                                    msg="The number of records is not over the stream max limit")

                primary_keys_list = [tuple([message.get('data').get(expected_pk) for expected_pk in expected_primary_keys])
                                        for message in synced_records.get(stream).get('messages')
                                        if message.get('action') == 'upsert']

                if record_count_sync > self.API_LIMIT:

                    primary_keys_list_1 = primary_keys_list[:self.API_LIMIT]
                    primary_keys_list_2 = primary_keys_list[self.API_LIMIT:2*self.API_LIMIT]

                    primary_keys_page_1 = set(primary_keys_list_1)
                    primary_keys_page_2 = set(primary_keys_list_2)

                    # Verify by primary keys that data is unique for page
                    self.assertTrue(
                        primary_keys_page_1.isdisjoint(primary_keys_page_2))
