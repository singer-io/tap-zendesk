import unittest
from unittest.mock import MagicMock, patch, call

from zenpy.lib.api_objects import BaseObject
from zenpy.lib.proxy import ProxyList

from tap_zendesk.sync import (
    process_record,
    update_currently_syncing,
    sync_stream,
    ZendeskEncoder,
)


class TestProcessRecord(unittest.TestCase):
    """ Tests for `process_record`, which serializes zenpy objects into plain dicts. """

    def test_plain_dict_passthrough(self):
        record = {"id": 1, "name": "test"}
        self.assertEqual(process_record(record), record)

    def test_base_object_is_serialized_to_dict(self):
        obj = BaseObject()
        obj.id = 1
        obj.name = "test"
        result = process_record(obj)
        self.assertEqual(result, {"id": 1, "name": "test"})

    def test_base_object_callable_values_are_dropped(self):
        class FakeBaseObject(BaseObject):
            def to_dict(self, serialize=False):
                return {"id": 1, "some_callable": lambda: "nope"}

        encoder = ZendeskEncoder()
        result = encoder.default(FakeBaseObject())
        self.assertEqual(result, {"id": 1})

    def test_proxy_list_is_serialized_to_list(self):
        record = {"tags": ProxyList(["a", "b", "c"])}
        result = process_record(record)
        self.assertEqual(result, {"tags": ["a", "b", "c"]})

    def test_nested_base_object(self):
        child = BaseObject()
        child.id = 2
        parent = {"id": 1, "child": child}
        result = process_record(parent)
        self.assertEqual(result, {"id": 1, "child": {"id": 2}})


class TestZendeskEncoderDefault(unittest.TestCase):
    """ Tests for `ZendeskEncoder.default` fallback behavior. """

    def test_default_raises_typeerror_for_unsupported_type(self):
        encoder = ZendeskEncoder()
        with self.assertRaises(TypeError):
            encoder.default(object())

    def test_default_copies_proxy_list_directly(self):
        # ProxyList subclasses `list`, so json.dumps serializes it natively without
        # ever calling `default()`. Exercise the `default()` branch directly to cover
        # the explicit ProxyList handling in ZendeskEncoder.
        encoder = ZendeskEncoder()
        proxy_list = ProxyList(["a", "b"])
        result = encoder.default(proxy_list)
        self.assertEqual(result, ["a", "b"])


class TestUpdateCurrentlySyncing(unittest.TestCase):
    """ Tests for `update_currently_syncing`. """

    @patch('tap_zendesk.sync.singer.write_state')
    @patch('tap_zendesk.sync.singer.set_currently_syncing')
    @patch('tap_zendesk.sync.singer.get_currently_syncing')
    def test_stream_name_provided_sets_currently_syncing(
            self, mock_get_currently_syncing, mock_set_currently_syncing, mock_write_state):
        state = {}
        update_currently_syncing(state, "tickets")

        mock_set_currently_syncing.assert_called_once_with(state, "tickets")
        mock_write_state.assert_called_once_with(state)

    @patch('tap_zendesk.sync.singer.write_state')
    @patch('tap_zendesk.sync.singer.set_currently_syncing')
    @patch('tap_zendesk.sync.singer.get_currently_syncing')
    def test_falsy_stream_name_with_existing_currently_syncing_deletes_it(
            self, mock_get_currently_syncing, mock_set_currently_syncing, mock_write_state):
        mock_get_currently_syncing.return_value = "tickets"
        state = {"currently_syncing": "tickets"}

        update_currently_syncing(state, None)

        self.assertNotIn("currently_syncing", state)
        mock_set_currently_syncing.assert_not_called()
        mock_write_state.assert_called_once_with(state)

    @patch('tap_zendesk.sync.singer.write_state')
    @patch('tap_zendesk.sync.singer.set_currently_syncing')
    @patch('tap_zendesk.sync.singer.get_currently_syncing')
    def test_falsy_stream_name_without_existing_currently_syncing_sets_none(
            self, mock_get_currently_syncing, mock_set_currently_syncing, mock_write_state):
        mock_get_currently_syncing.return_value = None
        state = {}

        update_currently_syncing(state, None)

        mock_set_currently_syncing.assert_called_once_with(state, None)
        mock_write_state.assert_called_once_with(state)


class TestSyncStream(unittest.TestCase):
    """ Tests for `sync_stream`. """

    def _make_instance(self, replication_method, tap_stream_id="tickets", replication_key="updated_at"):
        stream = MagicMock()
        stream.tap_stream_id = tap_stream_id
        stream.schema.to_dict.return_value = {"type": "object", "properties": {}}
        stream.metadata = []

        instance = MagicMock()
        instance.stream = stream
        instance.replication_method = replication_method
        instance.replication_key = replication_key
        return instance, stream

    @patch('tap_zendesk.sync.metrics.record_counter')
    @patch('tap_zendesk.sync.singer.write_record')
    @patch('tap_zendesk.sync.singer.write_state')
    @patch('tap_zendesk.sync.singer.write_bookmark')
    @patch('tap_zendesk.sync.update_currently_syncing')
    @patch('tap_zendesk.sync.singer.get_currently_syncing', return_value=None)
    def test_incremental_stream_without_bookmark_writes_initial_bookmark(
            self, mock_get_currently_syncing, mock_update_currently_syncing,
            mock_write_bookmark, mock_write_state, mock_write_record, mock_record_counter):
        instance, stream = self._make_instance("INCREMENTAL")
        instance.sync.return_value = iter([(stream, {"id": 1})])

        counter_cm = MagicMock()
        counter_cm.value = 1
        mock_record_counter.return_value.__enter__.return_value = counter_cm

        state = {}
        result = sync_stream(state, "2020-01-01T00:00:00Z", instance)

        mock_write_bookmark.assert_called_once_with(
            state, "tickets", "updated_at", "2020-01-01T00:00:00Z")
        counter_cm.increment.assert_called_once()
        mock_write_record.assert_called_once_with("tickets", {"id": 1})
        mock_write_state.assert_called_once_with(state)
        self.assertEqual(mock_update_currently_syncing.call_args_list,
                          [call(state, "tickets"), call(state, None)])
        self.assertEqual(result, 1)

    @patch('tap_zendesk.sync.metrics.record_counter')
    @patch('tap_zendesk.sync.singer.write_record')
    @patch('tap_zendesk.sync.singer.write_state')
    @patch('tap_zendesk.sync.singer.write_bookmark')
    @patch('tap_zendesk.sync.update_currently_syncing')
    def test_incremental_stream_with_existing_bookmark_skips_initial_write(
            self, mock_update_currently_syncing, mock_write_bookmark,
            mock_write_state, mock_write_record, mock_record_counter):
        instance, stream = self._make_instance("INCREMENTAL")
        instance.sync.return_value = iter([])

        counter_cm = MagicMock()
        counter_cm.value = 0
        mock_record_counter.return_value.__enter__.return_value = counter_cm

        state = {"bookmarks": {"tickets": {"updated_at": "2020-06-01T00:00:00Z"}}}
        sync_stream(state, "2020-01-01T00:00:00Z", instance)

        mock_write_bookmark.assert_not_called()
        mock_write_state.assert_called_once_with(state)

    @patch('tap_zendesk.sync.metrics.record_counter')
    @patch('tap_zendesk.sync.singer.write_record')
    @patch('tap_zendesk.sync.singer.write_state')
    @patch('tap_zendesk.sync.update_currently_syncing')
    def test_full_table_stream_does_not_write_state_at_end(
            self, mock_update_currently_syncing, mock_write_state,
            mock_write_record, mock_record_counter):
        instance, stream = self._make_instance("FULL_TABLE")
        instance.sync.return_value = iter([(stream, {"id": 1})])

        counter_cm = MagicMock()
        counter_cm.value = 1
        mock_record_counter.return_value.__enter__.return_value = counter_cm

        state = {}
        sync_stream(state, "2020-01-01T00:00:00Z", instance)

        mock_write_state.assert_not_called()

    @patch('tap_zendesk.sync.metrics.record_counter')
    @patch('tap_zendesk.sync.singer.write_record')
    @patch('tap_zendesk.sync.singer.write_state')
    @patch('tap_zendesk.sync.update_currently_syncing')
    def test_substream_records_are_not_counted(
            self, mock_update_currently_syncing, mock_write_state,
            mock_write_record, mock_record_counter):
        instance, stream = self._make_instance("FULL_TABLE", tap_stream_id="tickets")
        child_stream = MagicMock()
        child_stream.tap_stream_id = "ticket_comments"
        child_stream.schema.to_dict.return_value = {"type": "object", "properties": {}}
        child_stream.metadata = []

        instance.sync.return_value = iter([
            (stream, {"id": 1}),
            (child_stream, {"id": 2}),
        ])

        counter_cm = MagicMock()
        counter_cm.value = 1
        mock_record_counter.return_value.__enter__.return_value = counter_cm

        sync_stream({}, "2020-01-01T00:00:00Z", instance)

        counter_cm.increment.assert_called_once()
        self.assertEqual(mock_write_record.call_args_list,
                          [call("tickets", {"id": 1}), call("ticket_comments", {"id": 2})])
