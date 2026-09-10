import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from zenpy.lib.exception import APIException

from tap_zendesk.exceptions import ZendeskForbiddenError
from tap_zendesk.streams.abstracts import (
    Stream,
    PaginatedStream,
    CursorBasedExportStream,
    ParentChildBookmarkMixin,
    ChildBookmarkMixin,
    process_custom_field,
    raise_or_log_zenpy_apiexception,
    raise_forbidden_if_access_denied,
    get_abs_path,
)


def make_config(**overrides):
    config = {"start_date": "2020-01-01T00:00:00Z", "subdomain": "foo", "access_token": "abc"}
    config.update(overrides)
    return config


class TestProcessCustomField(unittest.TestCase):

    def test_returns_schema_for_known_type(self):
        field = MagicMock(type="integer")
        schema = process_custom_field(field)
        self.assertEqual(schema, {"type": ["integer", "null"]})

    def test_date_type_adds_datetime_format(self):
        field = MagicMock(type="date")
        schema = process_custom_field(field)
        self.assertEqual(schema["format"], "datetime")

    def test_dropdown_type_adds_enum(self):
        option_a = MagicMock(value="a")
        option_b = MagicMock(value="b")
        field = MagicMock(type="dropdown", custom_field_options=[option_a, option_b])
        schema = process_custom_field(field)
        self.assertEqual(schema["enum"], ["a", "b"])

    @patch('tap_zendesk.streams.abstracts.LOGGER')
    def test_unsupported_type_logs_critical_and_defaults_to_string(self, mock_logger):
        field = MagicMock(type="unknown-type", title="Title", key="key")
        schema = process_custom_field(field)
        mock_logger.critical.assert_called_once()
        self.assertEqual(schema["type"], ["string", "null"])


class TestStreamInit(unittest.TestCase):

    def test_default_request_timeout_and_page_size(self):
        stream = Stream(client=MagicMock(), config=make_config())
        self.assertEqual(stream.request_timeout, 300)
        self.assertEqual(stream.page_size, 100)

    def test_custom_request_timeout_and_page_size(self):
        stream = Stream(client=MagicMock(), config=make_config(request_timeout="120", page_size="50"))
        self.assertEqual(stream.request_timeout, 120.0)
        self.assertEqual(stream.page_size, 50)

    def test_invalid_page_size_falls_back_to_default(self):
        stream = Stream(client=MagicMock(), config=make_config(page_size="5000"))
        self.assertEqual(stream.page_size, 100)

    def test_search_window_size_too_small_raises(self):
        with self.assertRaises(ValueError):
            Stream(client=MagicMock(), config=make_config(search_window_size="1"))

    def test_search_window_size_valid_does_not_raise(self):
        Stream(client=MagicMock(), config=make_config(search_window_size="5"))


class TestGetAbsPath(unittest.TestCase):

    def test_returns_path_relative_to_module(self):
        path = get_abs_path("../schemas/bookmarks.json")
        self.assertTrue(path.endswith("bookmarks.json"))


class TestGetBookmark(unittest.TestCase):

    def test_returns_start_date_when_no_bookmark_present(self):
        stream = Stream(client=MagicMock(), config=make_config())
        stream.replication_key = "updated_at"
        result = stream.get_bookmark({}, "bookmarks")
        self.assertEqual(result, datetime(2020, 1, 1, tzinfo=timezone.utc))

    def test_returns_existing_bookmark(self):
        stream = Stream(client=MagicMock(), config=make_config())
        stream.replication_key = "updated_at"
        state = {"bookmarks": {"bookmarks": {"updated_at": "2021-06-01T00:00:00Z"}}}
        result = stream.get_bookmark(state, "bookmarks")
        self.assertEqual(result, datetime(2021, 6, 1, tzinfo=timezone.utc))


class TestUpdateBookmark(unittest.TestCase):

    def test_writes_new_bookmark_when_greater_than_current(self):
        stream = Stream(client=MagicMock(), config=make_config())
        stream.replication_key = "updated_at"
        state = {}
        stream.update_bookmark(state, "bookmarks", "2022-01-01T00:00:00Z")
        self.assertEqual(state["bookmarks"]["bookmarks"]["updated_at"], "2022-01-01T00:00:00Z")

    def test_keeps_current_bookmark_when_new_value_is_older(self):
        stream = Stream(client=MagicMock(), config=make_config())
        stream.replication_key = "updated_at"
        state = {"bookmarks": {"bookmarks": {"updated_at": "2023-01-01T00:00:00Z"}}}
        stream.update_bookmark(state, "bookmarks", "2022-01-01T00:00:00Z")
        self.assertEqual(state["bookmarks"]["bookmarks"]["updated_at"], "2023-01-01T00:00:00Z")

    def test_accepts_datetime_value_directly(self):
        stream = Stream(client=MagicMock(), config=make_config())
        stream.replication_key = "updated_at"
        state = {}
        stream.update_bookmark(state, "bookmarks", datetime(2022, 5, 1, tzinfo=timezone.utc))
        self.assertEqual(state["bookmarks"]["bookmarks"]["updated_at"], "2022-05-01T00:00:00Z")


class TestLoadSchemaAndMetadata(unittest.TestCase):

    def test_load_schema_reads_real_schema_file(self):
        stream = Stream(client=MagicMock(), config=make_config())
        stream.name = "bookmarks"
        schema = stream.load_schema()
        self.assertIn("properties", schema)
        self.assertIn("id", schema["properties"])

    def test_add_custom_fields_default_passthrough(self):
        stream = Stream(client=MagicMock(), config=make_config())
        schema = {"type": "object", "properties": {}}
        self.assertIs(stream._add_custom_fields(schema), schema)

    def test_load_metadata_marks_key_properties_automatic(self):
        stream = Stream(client=MagicMock(), config=make_config())
        stream.name = "bookmarks"
        stream.replication_method = "FULL_TABLE"
        stream.replication_key = None
        stream.parent = ""
        mdata_list = stream.load_metadata()
        mdata_map = {tuple(entry["breadcrumb"]): entry["metadata"] for entry in mdata_list}
        self.assertEqual(mdata_map[("properties", "id")]["inclusion"], "automatic")
        self.assertEqual(mdata_map[("properties", "url")]["inclusion"], "available")
        self.assertNotIn("parent-tap-stream-id", mdata_map[()])

    def test_load_metadata_includes_replication_key_and_parent(self):
        stream = Stream(client=MagicMock(), config=make_config())
        stream.name = "bookmarks"
        stream.replication_method = "INCREMENTAL"
        stream.replication_key = "created_at"
        stream.parent = "tickets"
        mdata_list = stream.load_metadata()
        mdata_map = {tuple(entry["breadcrumb"]): entry["metadata"] for entry in mdata_list}
        self.assertEqual(mdata_map[()]["parent-tap-stream-id"], "tickets")
        self.assertEqual(mdata_map[()]["valid-replication-keys"], ["created_at"])
        self.assertEqual(mdata_map[("properties", "created_at")]["inclusion"], "automatic")


class TestIsSelected(unittest.TestCase):

    def test_returns_true_when_selected(self):
        stream = Stream(client=MagicMock(), config=make_config())
        stream.stream = MagicMock(metadata=[
            {"breadcrumb": (), "metadata": {"selected": True}}
        ])
        self.assertTrue(stream.is_selected())

    def test_returns_false_when_not_selected(self):
        stream = Stream(client=MagicMock(), config=make_config())
        stream.stream = MagicMock(metadata=[
            {"breadcrumb": (), "metadata": {"selected": False}}
        ])
        self.assertFalse(stream.is_selected())


class TestCheckAccess(unittest.TestCase):

    @patch('tap_zendesk.streams.abstracts.http.call_api')
    def test_calls_api_with_stream_endpoint(self, mock_call_api):
        stream = Stream(client=MagicMock(), config=make_config())
        stream.endpoint = "bookmarks"
        stream.check_access()
        mock_call_api.assert_called_once()


class TestUpdateParams(unittest.TestCase):

    def test_resets_params(self):
        stream = Stream(client=MagicMock(), config=make_config())
        stream.params = {"foo": "bar"}
        stream.update_params()
        self.assertEqual(stream.params, {})


class TestGetStreamEndpoint(unittest.TestCase):

    def test_builds_full_url(self):
        stream = Stream(client=MagicMock(), config=make_config())
        stream.endpoint = "bookmarks"
        url = stream.get_stream_endpoint()
        self.assertEqual(url, "https://foo.zendesk.com/api/v2/bookmarks")

    def test_missing_placeholder_raises_value_error(self):
        stream = Stream(client=MagicMock(), config=make_config())
        stream.endpoint = "tickets/{ticket_id}/comments"
        with self.assertRaises(ValueError):
            stream.get_stream_endpoint()


class TestModifyObject(unittest.TestCase):

    def test_returns_record_unchanged(self):
        stream = Stream(client=MagicMock(), config=make_config())
        record = {"id": 1}
        self.assertIs(stream.modify_object(record), record)


class TestGetNestedValue(unittest.TestCase):

    def test_returns_nested_value(self):
        stream = Stream(client=MagicMock(), config=make_config())
        data = {"a": {"b": {"c": 42}}}
        self.assertEqual(stream.get_nested_value(data, "a.b.c"), 42)

    def test_returns_default_when_path_not_dict(self):
        stream = Stream(client=MagicMock(), config=make_config())
        data = {"a": "not-a-dict"}
        self.assertEqual(stream.get_nested_value(data, "a.b.c", default="missing"), "missing")


class TestEmitSubStreamMetrics(unittest.TestCase):

    @patch('tap_zendesk.streams.abstracts.singer.metrics.log')
    def test_emits_metrics_when_selected(self, mock_log):
        stream = Stream(client=MagicMock(), config=make_config())
        sub_stream = MagicMock()
        sub_stream.is_selected.return_value = True
        sub_stream.count = 5
        sub_stream.stream.tap_stream_id = "ticket_comments"

        stream.emit_sub_stream_metrics(sub_stream)

        mock_log.assert_called_once()
        self.assertEqual(sub_stream.count, 0)

    @patch('tap_zendesk.streams.abstracts.singer.metrics.log')
    def test_does_not_emit_metrics_when_not_selected(self, mock_log):
        stream = Stream(client=MagicMock(), config=make_config())
        sub_stream = MagicMock()
        sub_stream.is_selected.return_value = False

        stream.emit_sub_stream_metrics(sub_stream)

        mock_log.assert_not_called()


class TestPaginatedStreamGetObjects(unittest.TestCase):

    def _make_stream(self, pagination_type="cursor", item_key="bookmarks"):
        stream = PaginatedStream(client=MagicMock(), config=make_config())
        stream.endpoint = "bookmarks"
        stream.pagination_type = pagination_type
        stream.item_key = item_key
        return stream

    @patch('tap_zendesk.streams.abstracts.http.get_cursor_based')
    def test_cursor_pagination_yields_list_items(self, mock_get_cursor_based):
        stream = self._make_stream(pagination_type="cursor")
        mock_get_cursor_based.return_value = [{"bookmarks": [{"id": 1}, {"id": 2}]}]
        result = list(stream.get_objects())
        self.assertEqual(result, [{"id": 1}, {"id": 2}])

    @patch('tap_zendesk.streams.abstracts.http.get_offset_based')
    def test_offset_pagination_yields_dict_item(self, mock_get_offset_based):
        stream = self._make_stream(pagination_type="offset", item_key="bookmark")
        mock_get_offset_based.return_value = [{"bookmark": {"id": 1}}]
        result = list(stream.get_objects())
        self.assertEqual(result, [{"id": 1}])

    @patch('tap_zendesk.streams.abstracts.http.get_cursor_based')
    def test_unrecognized_item_key_yields_nothing(self, mock_get_cursor_based):
        stream = self._make_stream(pagination_type="cursor", item_key="missing_key")
        mock_get_cursor_based.return_value = [{"bookmarks": [{"id": 1}]}]
        result = list(stream.get_objects())
        self.assertEqual(result, [])

    def test_unsupported_pagination_type_raises(self):
        stream = self._make_stream(pagination_type="bogus")
        with self.assertRaises(ValueError):
            list(stream.get_objects())

    @patch('tap_zendesk.streams.abstracts.http.get_cursor_based')
    def test_non_dict_non_list_item_yields_nothing(self, mock_get_cursor_based):
        stream = self._make_stream(pagination_type="cursor", item_key="bookmarks")
        mock_get_cursor_based.return_value = [{"bookmarks": "not-a-dict-or-list"}]
        result = list(stream.get_objects())
        self.assertEqual(result, [])


class TestPaginatedStreamSync(unittest.TestCase):

    def _make_stream(self, replication_method):
        stream = PaginatedStream(client=MagicMock(), config=make_config())
        stream.name = "bookmarks"
        stream.endpoint = "bookmarks"
        stream.pagination_type = "cursor"
        stream.item_key = "bookmarks"
        stream.replication_method = replication_method
        stream.replication_key = "updated_at" if replication_method == "INCREMENTAL" else None
        stream.stream = MagicMock(tap_stream_id="bookmarks")
        return stream

    @patch.object(PaginatedStream, 'is_selected', return_value=True)
    @patch.object(PaginatedStream, 'get_objects')
    def test_incremental_sync_yields_selected_records_and_updates_bookmark(
            self, mock_get_objects, mock_is_selected):
        stream = self._make_stream("INCREMENTAL")
        mock_get_objects.return_value = iter([
            {"id": 1, "updated_at": "2021-01-01T00:00:00Z"},
        ])

        state = {}
        results = list(stream.sync(state))

        self.assertEqual(len(results), 1)
        self.assertEqual(state["bookmarks"]["bookmarks"]["updated_at"], "2021-01-01T00:00:00Z")

    @patch.object(PaginatedStream, 'get_objects')
    def test_incremental_sync_missing_replication_key_raises(self, mock_get_objects):
        stream = self._make_stream("INCREMENTAL")
        mock_get_objects.return_value = iter([{"id": 1}])

        with self.assertRaises(ValueError):
            list(stream.sync({}))

    @patch.object(PaginatedStream, 'is_selected', return_value=False)
    @patch.object(PaginatedStream, 'get_objects')
    def test_incremental_sync_skips_unselected_records(self, mock_get_objects, mock_is_selected):
        stream = self._make_stream("INCREMENTAL")
        mock_get_objects.return_value = iter([
            {"id": 1, "updated_at": "2021-01-01T00:00:00Z"},
        ])
        results = list(stream.sync({}))
        self.assertEqual(results, [])

    @patch.object(PaginatedStream, 'is_selected', return_value=True)
    @patch.object(PaginatedStream, 'get_objects')
    def test_incremental_sync_skips_records_older_than_bookmark(self, mock_get_objects, mock_is_selected):
        stream = self._make_stream("INCREMENTAL")
        mock_get_objects.return_value = iter([
            {"id": 1, "updated_at": "2019-01-01T00:00:00Z"},
        ])
        state = {"bookmarks": {"bookmarks": {"updated_at": "2020-06-01T00:00:00Z"}}}
        results = list(stream.sync(state))
        self.assertEqual(results, [])

    @patch.object(PaginatedStream, 'is_selected', return_value=True)
    @patch.object(PaginatedStream, 'get_objects')
    def test_full_table_sync_yields_all_selected_records(self, mock_get_objects, mock_is_selected):
        stream = self._make_stream("FULL_TABLE")
        mock_get_objects.return_value = iter([{"id": 1}, {"id": 2}])
        results = list(stream.sync({}))
        self.assertEqual(len(results), 2)

    @patch.object(PaginatedStream, 'get_objects')
    def test_unknown_replication_method_raises(self, mock_get_objects):
        stream = self._make_stream("FULL_TABLE")
        stream.replication_method = "BOGUS"
        mock_get_objects.return_value = iter([{"id": 1}])
        with self.assertRaises(ValueError):
            list(stream.sync({}))

    @patch.object(PaginatedStream, 'is_selected', return_value=True)
    @patch.object(PaginatedStream, 'get_objects')
    def test_incremental_sync_syncs_children(self, mock_get_objects, mock_is_selected):
        stream = self._make_stream("INCREMENTAL")
        child = MagicMock()
        child.is_selected.return_value = False
        child.sync.return_value = iter([])
        stream.child_to_sync = [child]

        mock_get_objects.return_value = iter([
            {"id": 1, "updated_at": "2021-01-01T00:00:00Z"},
        ])
        list(stream.sync({}))

        child.sync.assert_called_once()

    @patch.object(PaginatedStream, 'is_selected', return_value=True)
    @patch.object(PaginatedStream, 'get_objects')
    def test_full_table_sync_syncs_children(self, mock_get_objects, mock_is_selected):
        stream = self._make_stream("FULL_TABLE")
        child = MagicMock()
        child.is_selected.return_value = False
        child.sync.return_value = iter([(MagicMock(), {"id": 99})])
        stream.child_to_sync = [child]

        mock_get_objects.return_value = iter([{"id": 1}])
        results = list(stream.sync({}))

        child.sync.assert_called_once()
        self.assertTrue(any(r[1] == {"id": 1} for r in results))


class TestCursorBasedExportStream(unittest.TestCase):

    def _make_stream(self, replication_method="INCREMENTAL"):
        stream = CursorBasedExportStream(client=MagicMock(), config=make_config())
        stream.name = "tickets"
        stream.endpoint = "incremental/tickets"
        stream.item_key = "tickets"
        stream.replication_method = replication_method
        stream.replication_key = "updated_at" if replication_method == "INCREMENTAL" else None
        stream.stream = MagicMock(tap_stream_id="tickets")
        return stream

    @patch('tap_zendesk.streams.abstracts.http.get_incremental_export')
    def test_get_objects_yields_items_from_pages(self, mock_get_incremental_export):
        stream = self._make_stream()
        mock_get_incremental_export.return_value = [{"tickets": [{"id": 1}, {"id": 2}]}]
        results = list(stream.get_objects(0))
        self.assertEqual(results, [{"id": 1}, {"id": 2}])

    @patch.object(CursorBasedExportStream, 'is_selected', return_value=True)
    @patch.object(CursorBasedExportStream, 'get_objects')
    def test_incremental_sync_updates_bookmark(self, mock_get_objects, mock_is_selected):
        stream = self._make_stream("INCREMENTAL")
        mock_get_objects.return_value = iter([
            {"id": 1, "updated_at": "2021-01-01T00:00:00Z"},
        ])
        state = {}
        results = list(stream.sync(state))
        self.assertEqual(len(results), 1)
        self.assertEqual(state["bookmarks"]["tickets"]["updated_at"], "2021-01-01T00:00:00Z")

    @patch.object(CursorBasedExportStream, 'get_objects')
    def test_incremental_sync_missing_replication_key_raises(self, mock_get_objects):
        stream = self._make_stream("INCREMENTAL")
        mock_get_objects.return_value = iter([{"id": 1}])
        with self.assertRaises(ValueError):
            list(stream.sync({}))

    @patch.object(CursorBasedExportStream, 'is_selected', return_value=True)
    @patch.object(CursorBasedExportStream, 'get_objects')
    def test_full_table_sync(self, mock_get_objects, mock_is_selected):
        stream = self._make_stream("FULL_TABLE")
        mock_get_objects.return_value = iter([{"id": 1}])
        results = list(stream.sync({}))
        self.assertEqual(len(results), 1)

    @patch.object(CursorBasedExportStream, 'get_objects')
    def test_unknown_replication_method_raises(self, mock_get_objects):
        stream = self._make_stream("FULL_TABLE")
        stream.replication_method = "BOGUS"
        mock_get_objects.return_value = iter([{"id": 1}])
        with self.assertRaises(ValueError):
            list(stream.sync({}))

    @patch.object(CursorBasedExportStream, 'is_selected', return_value=True)
    @patch.object(CursorBasedExportStream, 'get_objects')
    def test_incremental_sync_syncs_children(self, mock_get_objects, mock_is_selected):
        stream = self._make_stream("INCREMENTAL")
        child = MagicMock()
        child.is_selected.return_value = False
        child.sync.return_value = iter([])
        stream.child_to_sync = [child]

        mock_get_objects.return_value = iter([
            {"id": 1, "updated_at": "2021-01-01T00:00:00Z"},
        ])
        list(stream.sync({}))

        child.sync.assert_called_once()

    @patch.object(CursorBasedExportStream, 'is_selected', return_value=True)
    @patch.object(CursorBasedExportStream, 'get_objects')
    def test_full_table_sync_syncs_children(self, mock_get_objects, mock_is_selected):
        stream = self._make_stream("FULL_TABLE")
        child = MagicMock()
        child.is_selected.return_value = False
        child.sync.return_value = iter([])
        stream.child_to_sync = [child]

        mock_get_objects.return_value = iter([{"id": 1}])
        list(stream.sync({}))

        child.sync.assert_called_once()


class TestRaiseOrLogZenpyApiException(unittest.TestCase):

    def test_non_api_exception_raises_value_error(self):
        with self.assertRaises(ValueError):
            raise_or_log_zenpy_apiexception({}, "tickets", Exception("boom"))

    @patch('tap_zendesk.streams.abstracts.LOGGER')
    def test_missing_read_scope_returns_schema(self, mock_logger):
        schema = {"type": "object"}
        exc = APIException(json.dumps({"description": "Missing the following required scopes: read"}))
        result = raise_or_log_zenpy_apiexception(schema, "tickets", exc)
        self.assertIs(result, schema)
        mock_logger.warning.assert_called_once()

    @patch('tap_zendesk.streams.abstracts.LOGGER')
    def test_restricted_access_error_returns_schema(self, mock_logger):
        schema = {"type": "object"}
        exc = APIException(json.dumps({
            "error": {"message": "Access to this resource is restricted. Please contact the account administrator for assistance."}
        }))
        result = raise_or_log_zenpy_apiexception(schema, "tickets", exc)
        self.assertIs(result, schema)

    def test_other_error_reraises(self):
        exc = APIException(json.dumps({"error": "some other error"}))
        with self.assertRaises(APIException):
            raise_or_log_zenpy_apiexception({}, "tickets", exc)


class TestRaiseForbiddenIfAccessDenied(unittest.TestCase):

    def test_restricted_access_message_raises_forbidden(self):
        exc = APIException(json.dumps({
            "error": {"message": "Access to this resource is restricted. Please contact the account administrator for assistance."}
        }))
        with self.assertRaises(ZendeskForbiddenError):
            raise_forbidden_if_access_denied(exc)

    def test_missing_read_scope_raises_forbidden(self):
        exc = APIException(json.dumps({"description": "Missing the following required scopes: read"}))
        with self.assertRaises(ZendeskForbiddenError):
            raise_forbidden_if_access_denied(exc)

    def test_other_error_reraises_original(self):
        exc = APIException(json.dumps({"error": "some other error"}))
        with self.assertRaises(APIException):
            raise_forbidden_if_access_denied(exc)

    def test_non_json_body_reraises_original(self):
        exc = APIException("not json")
        with self.assertRaises(APIException):
            raise_forbidden_if_access_denied(exc)


class TestParentChildBookmarkMixin(unittest.TestCase):

    class ParentStream(ParentChildBookmarkMixin, Stream):
        pass

    def test_updates_own_bookmark_when_selected(self):
        parent = self.ParentStream(client=MagicMock(), config=make_config())
        parent.name = "tickets"
        parent.replication_key = "updated_at"
        parent.is_selected = MagicMock(return_value=True)
        parent.child_to_sync = []

        state = {}
        parent.update_bookmark(state, "tickets", "2022-01-01T00:00:00Z")

        self.assertEqual(state["bookmarks"]["tickets"]["updated_at"], "2022-01-01T00:00:00Z")

    def test_skips_own_bookmark_when_not_selected(self):
        parent = self.ParentStream(client=MagicMock(), config=make_config())
        parent.name = "tickets"
        parent.replication_key = "updated_at"
        parent.is_selected = MagicMock(return_value=False)
        parent.child_to_sync = []

        state = {}
        parent.update_bookmark(state, "tickets", "2022-01-01T00:00:00Z")

        self.assertNotIn("bookmarks", state)

    def test_persists_traversal_bookmark_when_unselected_parent_has_children_to_sync(self):
        """
        Even if the parent stream itself is not selected, its own bookmark
        must still be persisted whenever it is being traversed to reach a
        selected child stream (i.e. `child_to_sync` is non-empty) - otherwise
        the parent would be re-scanned from `start_date` on every sync purely
        to reach its children.
        """
        parent = self.ParentStream(client=MagicMock(), config=make_config())
        parent.name = "tickets"
        parent.replication_key = "updated_at"
        parent.is_selected = MagicMock(return_value=False)

        child = MagicMock()
        child.is_selected.return_value = True
        child.replication_method = "INCREMENTAL"
        child.name = "ticket_comments"
        child.replication_key = "created_at"
        parent.child_to_sync = [child]

        state = {}
        parent.update_bookmark(state, "tickets", "2022-01-01T00:00:00Z")

        self.assertEqual(state["bookmarks"]["tickets"]["updated_at"], "2022-01-01T00:00:00Z")

    def test_writes_initial_bookmark_for_incremental_child(self):
        parent = self.ParentStream(client=MagicMock(), config=make_config())
        parent.name = "tickets"
        parent.replication_key = "updated_at"
        parent.is_selected = MagicMock(return_value=True)

        child = MagicMock()
        child.is_selected.return_value = True
        child.replication_method = "INCREMENTAL"
        child.name = "ticket_comments"
        child.replication_key = "created_at"
        parent.child_to_sync = [child]

        state = {}
        parent.update_bookmark(state, "tickets", "2022-01-01T00:00:00Z")

        self.assertEqual(state["bookmarks"]["ticket_comments"]["created_at"],
                         parent.config["start_date"])

    def test_skips_full_table_child(self):
        parent = self.ParentStream(client=MagicMock(), config=make_config())
        parent.name = "tickets"
        parent.replication_key = "updated_at"
        parent.is_selected = MagicMock(return_value=True)

        child = MagicMock()
        child.is_selected.return_value = True
        child.replication_method = "FULL_TABLE"
        child.name = "ticket_metrics"
        parent.child_to_sync = [child]

        state = {}
        parent.update_bookmark(state, "tickets", "2022-01-01T00:00:00Z")

        self.assertNotIn("ticket_metrics", state.get("bookmarks", {}))

    def test_skips_unselected_child(self):
        parent = self.ParentStream(client=MagicMock(), config=make_config())
        parent.name = "tickets"
        parent.replication_key = "updated_at"
        parent.is_selected = MagicMock(return_value=True)

        child = MagicMock()
        child.is_selected.return_value = False
        parent.child_to_sync = [child]

        state = {}
        parent.update_bookmark(state, "tickets", "2022-01-01T00:00:00Z")
        # No exception, child bookmark untouched.

    def test_does_not_overwrite_existing_child_bookmark(self):
        parent = self.ParentStream(client=MagicMock(), config=make_config())
        parent.name = "tickets"
        parent.replication_key = "updated_at"
        parent.is_selected = MagicMock(return_value=True)

        child = MagicMock()
        child.is_selected.return_value = True
        child.replication_method = "INCREMENTAL"
        child.name = "ticket_comments"
        child.replication_key = "created_at"
        parent.child_to_sync = [child]

        state = {"bookmarks": {"ticket_comments": {"created_at": "2019-01-01T00:00:00Z"}}}
        parent.update_bookmark(state, "tickets", "2022-01-01T00:00:00Z")

        self.assertEqual(state["bookmarks"]["ticket_comments"]["created_at"], "2019-01-01T00:00:00Z")


class TestChildBookmarkMixin(unittest.TestCase):

    class ChildStream(ChildBookmarkMixin, Stream):
        bookmark_value = None

    def test_caches_bookmark_value_after_first_call(self):
        child = self.ChildStream(client=MagicMock(), config=make_config())
        child.replication_key = "created_at"

        state = {}
        first = child.get_bookmark(state, "ticket_comments")
        # Mutate state; cached value should still be returned unchanged.
        state["bookmarks"] = {"ticket_comments": {"created_at": "2099-01-01T00:00:00Z"}}
        second = child.get_bookmark(state, "ticket_comments")

        self.assertEqual(first, second)
