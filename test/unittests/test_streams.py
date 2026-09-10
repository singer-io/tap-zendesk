import asyncio
import datetime
import unittest
from unittest.mock import MagicMock, patch

import requests
from aiohttp import ClientSession
from zenpy.lib.exception import APIException

from tap_zendesk.exceptions import ZendeskNotFoundError, ZendeskForbiddenError
from tap_zendesk.streams.abstracts import CursorBasedExportStream
from tap_zendesk.streams.users import Users, UserSubStreamMixin
from tap_zendesk.streams.user_identities import UserIdentities
from tap_zendesk.streams.organizations import Organizations
from tap_zendesk.streams.ticket_metric_events import TicketMetricEvents
from tap_zendesk.streams.group_memberships import GroupMemberships
from tap_zendesk.streams.trigger_revisions import TriggerRevisions
from tap_zendesk.streams.schedule_holidays import ScheduleHolidays
from tap_zendesk.streams.macro_attachments import MacroAttachments
from tap_zendesk.streams.ticket_forms import TicketForms
from tap_zendesk.streams.incremental_ticket_events import IncrementalTicketEvents
from tap_zendesk.streams.side_conversations import SideConversations
from tap_zendesk.streams.macro_categories import MacroCategories
from tap_zendesk.streams.satisfaction_ratings import SatisfactionRatings
from tap_zendesk.streams.sla_policies import SLAPolicies
from tap_zendesk.streams.talk_phone_numbers import TalkPhoneNumbers
from tap_zendesk.streams.user_attribute_values import UserAttributeValues


def make_config(**overrides):
    config = {"start_date": "2020-01-01T00:00:00Z", "subdomain": "foo", "access_token": "abc"}
    config.update(overrides)
    return config


class TestUsers(unittest.TestCase):

    def test_add_custom_fields_adds_schemas(self):
        client = MagicMock()
        field = MagicMock(type="integer", key="my_field")
        client.user_fields.return_value = iter([field])
        stream = Users(client=client, config=make_config())

        schema = {"properties": {"user_fields": {"properties": {}}}}
        result = stream._add_custom_fields(schema)

        self.assertEqual(result["properties"]["user_fields"]["properties"]["my_field"],
                          {"type": ["integer", "null"]})

    @patch('tap_zendesk.streams.users.raise_or_log_zenpy_apiexception')
    def test_add_custom_fields_api_exception_delegates_to_helper(self, mock_raise_or_log):
        client = MagicMock()
        client.user_fields.side_effect = APIException('{"error": "boom"}')
        stream = Users(client=client, config=make_config())
        mock_raise_or_log.return_value = {"fallback": True}

        schema = {"properties": {"user_fields": {"properties": {}}}}
        result = stream._add_custom_fields(schema)

        self.assertEqual(result, {"fallback": True})
        mock_raise_or_log.assert_called_once()

    def test_check_access_calls_client_search(self):
        client = MagicMock()
        stream = Users(client=client, config=make_config())
        stream.check_access()
        client.search.assert_called_once()
        _, kwargs = client.search.call_args
        self.assertEqual(kwargs["type"], "user")
        self.assertEqual(kwargs["updated_before"], "2000-01-02T00:00:00Z")

    def _make_child_mock(self, name, selected=True):
        child = MagicMock()
        child.name = name
        child.is_selected.return_value = selected
        return child

    @patch.object(Users, 'get_objects')
    @patch('tap_zendesk.streams.users.CONCURRENCY_LIMIT', 1)
    def test_sync_flushes_identities_batch_immediately_when_limit_reached(self, mock_get_objects):
        """
        With CONCURRENCY_LIMIT=1, each record should trigger its own
        identities_stream.sync_batch call as soon as the batch fills up.
        """
        mock_get_objects.return_value = iter([
            {"id": 1, "updated_at": "2021-06-01T00:00:00Z"},
            {"id": 2, "updated_at": "2021-06-02T00:00:00Z"},
        ])
        stream = Users(client=MagicMock(), config=make_config())
        stream.stream = MagicMock()
        stream.is_selected = MagicMock(return_value=True)
        stream.emit_sub_stream_metrics = MagicMock()

        identities_stream = self._make_child_mock("user_identities", selected=True)
        identities_stream.sync_batch.return_value = iter([])
        other_child = self._make_child_mock("user_attribute_values", selected=True)
        other_child.sync.side_effect = lambda **kwargs: iter([("user_attribute_values", {"id": "a"})])
        stream.child_to_sync = [identities_stream, other_child]

        state = {}
        results = list(stream.sync(state))

        self.assertEqual(identities_stream.sync_batch.call_count, 2)
        identities_stream.sync_batch.assert_any_call(state, [{"id": 1, "updated_at": "2021-06-01T00:00:00Z"}])
        identities_stream.sync_batch.assert_any_call(state, [{"id": 2, "updated_at": "2021-06-02T00:00:00Z"}])
        self.assertEqual(other_child.sync.call_count, 2)
        self.assertEqual(state["bookmarks"]["users"]["updated_at"], "2021-06-02T00:00:00Z")
        # Two user records + two records yielded from other_child.sync
        self.assertEqual(len(results), 4)

    @patch.object(Users, 'get_objects')
    @patch('tap_zendesk.streams.users.CONCURRENCY_LIMIT', 10)
    def test_sync_flushes_remaining_identities_batch_at_end(self, mock_get_objects):
        """
        With a CONCURRENCY_LIMIT higher than the number of records, the
        identities batch should only be flushed once, after the loop ends.
        """
        mock_get_objects.return_value = iter([
            {"id": 1, "updated_at": "2021-06-01T00:00:00Z"},
            {"id": 2, "updated_at": "2021-06-02T00:00:00Z"},
        ])
        stream = Users(client=MagicMock(), config=make_config())
        stream.stream = MagicMock()
        stream.is_selected = MagicMock(return_value=True)
        stream.emit_sub_stream_metrics = MagicMock()

        identities_stream = self._make_child_mock("user_identities", selected=True)
        identities_stream.sync_batch.return_value = iter([])
        stream.child_to_sync = [identities_stream]

        state = {}
        list(stream.sync(state))

        identities_stream.sync_batch.assert_called_once_with(
            state,
            [
                {"id": 1, "updated_at": "2021-06-01T00:00:00Z"},
                {"id": 2, "updated_at": "2021-06-02T00:00:00Z"},
            ],
        )

    @patch.object(Users, 'get_objects')
    def test_sync_skips_identities_batch_when_not_selected(self, mock_get_objects):
        mock_get_objects.return_value = iter([
            {"id": 1, "updated_at": "2021-06-01T00:00:00Z"},
        ])
        stream = Users(client=MagicMock(), config=make_config())
        stream.stream = MagicMock()
        stream.is_selected = MagicMock(return_value=True)
        stream.emit_sub_stream_metrics = MagicMock()

        identities_stream = self._make_child_mock("user_identities", selected=False)
        stream.child_to_sync = [identities_stream]

        state = {}
        list(stream.sync(state))

        identities_stream.sync_batch.assert_not_called()

    @patch.object(Users, 'get_objects')
    def test_sync_no_identities_stream_present(self, mock_get_objects):
        """
        If user_identities isn't in child_to_sync at all (e.g. deselected
        upstream), sync should complete without attempting to batch it.
        """
        mock_get_objects.return_value = iter([
            {"id": 1, "updated_at": "2021-06-01T00:00:00Z"},
        ])
        stream = Users(client=MagicMock(), config=make_config())
        stream.stream = MagicMock()
        stream.is_selected = MagicMock(return_value=True)
        stream.emit_sub_stream_metrics = MagicMock()
        stream.child_to_sync = []

        state = {}
        results = list(stream.sync(state))

        self.assertEqual(len(results), 1)

    @patch.object(Users, 'get_objects')
    def test_sync_skips_records_older_than_bookmark(self, mock_get_objects):
        mock_get_objects.return_value = iter([
            {"id": 1, "updated_at": "2019-01-01T00:00:00Z"},
        ])
        stream = Users(client=MagicMock(), config=make_config())
        stream.stream = MagicMock()
        stream.emit_sub_stream_metrics = MagicMock()

        identities_stream = self._make_child_mock("user_identities", selected=True)
        stream.child_to_sync = [identities_stream]

        state = {"bookmarks": {"users": {"updated_at": "2020-01-01T00:00:00Z"}}}
        results = list(stream.sync(state))

        self.assertEqual(results, [])
        identities_stream.sync_batch.assert_not_called()
        self.assertEqual(state["bookmarks"]["users"]["updated_at"], "2020-01-01T00:00:00Z")

    @patch.object(Users, 'get_objects')
    def test_sync_raises_when_replication_key_missing(self, mock_get_objects):
        mock_get_objects.return_value = iter([{"id": 1}])
        stream = Users(client=MagicMock(), config=make_config())
        stream.stream = MagicMock()
        stream.child_to_sync = []

        with self.assertRaises(ValueError):
            list(stream.sync({}))


class UserSubStream(UserSubStreamMixin, CursorBasedExportStream):
    name = "user_identities"
    endpoint = "users/{user_id}/identities.json"
    item_key = "identities"


class TestUserSubStreamMixin(unittest.TestCase):

    def test_check_access_is_noop(self):
        stream = UserSubStream(client=MagicMock(), config=make_config())
        self.assertIsNone(stream.check_access())

    def test_get_stream_endpoint_uses_parent_user_id(self):
        stream = UserSubStream(client=MagicMock(), config=make_config())
        url = stream.get_stream_endpoint(parent_obj={"id": 42})
        self.assertEqual(url, "https://foo.zendesk.com/api/v2/users/42/identities.json")

    def test_get_stream_endpoint_without_parent_id_raises(self):
        stream = UserSubStream(client=MagicMock(), config=make_config())
        with self.assertRaises(ValueError):
            stream.get_stream_endpoint(parent_obj={})

    @patch.object(CursorBasedExportStream, 'get_objects')
    def test_get_objects_yields_from_super(self, mock_super_get_objects):
        stream = UserSubStream(client=MagicMock(), config=make_config())
        mock_super_get_objects.return_value = iter([{"id": 1}])
        result = list(stream.get_objects(parent_obj={"id": 1}))
        self.assertEqual(result, [{"id": 1}])

    @patch.object(CursorBasedExportStream, 'get_objects')
    def test_get_objects_swallows_not_found_error(self, mock_super_get_objects):
        stream = UserSubStream(client=MagicMock(), config=make_config())

        def raise_not_found(**kwargs):
            raise ZendeskNotFoundError("not found")
            yield  # pragma: no cover

        mock_super_get_objects.side_effect = raise_not_found
        result = list(stream.get_objects(parent_obj={"id": 1}))
        self.assertEqual(result, [])


class TestUserIdentities(unittest.TestCase):

    def _make_stream(self):
        stream = UserIdentities(client=MagicMock(), config=make_config())
        stream.stream = MagicMock()
        return stream

    def test_fetch_raw_records_returns_user_id_and_records(self):
        stream = self._make_stream()

        async def fake_paginate_cursor_async(*args, **kwargs):
            return [{"id": "identity-1"}]

        async def run_test():
            with patch("tap_zendesk.streams.user_identities.http.paginate_cursor_async",
                       side_effect=fake_paginate_cursor_async):
                async with ClientSession() as session:
                    user_id, records = await stream._fetch_raw_records(session, 1)
                    self.assertEqual(user_id, 1)
                    self.assertEqual(records, [{"id": "identity-1"}])

        asyncio.run(run_test())

    def test_fetch_raw_records_returns_empty_list_on_404(self):
        stream = self._make_stream()

        async def raise_not_found(*args, **kwargs):
            raise ZendeskNotFoundError("not found")

        async def run_test():
            with patch("tap_zendesk.streams.user_identities.http.paginate_cursor_async",
                       side_effect=raise_not_found):
                async with ClientSession() as session:
                    user_id, records = await stream._fetch_raw_records(session, 1)
                    self.assertEqual(user_id, 1)
                    self.assertEqual(records, [])

        asyncio.run(run_test())

    def test_fetch_batch_gathers_records_for_all_user_ids(self):
        stream = self._make_stream()

        async def fake_fetch_raw_records(session, user_id):
            return user_id, [{"id": f"identity-{user_id}"}]

        with patch.object(UserIdentities, "_fetch_raw_records", side_effect=fake_fetch_raw_records):
            result = asyncio.run(stream._fetch_batch([1, 2]))

        self.assertEqual(result, {
            1: [{"id": "identity-1"}],
            2: [{"id": "identity-2"}],
        })

    def test_sync_batch_processes_records_per_user_in_order(self):
        stream = self._make_stream()

        with patch.object(UserIdentities, "_fetch_batch") as mock_fetch_batch, \
             patch.object(UserIdentities, "process_records") as mock_process_records:
            mock_fetch_batch.return_value = {
                1: [{"id": "identity-1"}],
                2: [{"id": "identity-2"}],
            }
            mock_process_records.side_effect = lambda state, raw_records, parent_obj=None: iter(
                [(stream.stream, record) for record in raw_records]
            )

            state = {}
            user_records = [{"id": 1}, {"id": 2}]
            results = list(stream.sync_batch(state, user_records))

        self.assertEqual(mock_process_records.call_count, 2)
        mock_process_records.assert_any_call(state, [{"id": "identity-1"}], parent_obj={"id": 1})
        mock_process_records.assert_any_call(state, [{"id": "identity-2"}], parent_obj={"id": 2})
        self.assertEqual(len(results), 2)

    def test_sync_batch_handles_user_missing_from_results(self):
        """
        If a user_id is absent from the fetched results dict (shouldn't normally
        happen, but defensively handled via .get(..., [])), process_records
        should still be called with an empty list.
        """
        stream = self._make_stream()

        with patch.object(UserIdentities, "_fetch_batch") as mock_fetch_batch, \
             patch.object(UserIdentities, "process_records") as mock_process_records:
            mock_fetch_batch.return_value = {}
            mock_process_records.return_value = iter([])

            list(stream.sync_batch({}, [{"id": 1}]))

        mock_process_records.assert_called_once_with({}, [], parent_obj={"id": 1})


class TestOrganizations(unittest.TestCase):

    def test_add_custom_fields_adds_schemas(self):
        client = MagicMock()
        field = MagicMock(type="integer", key="my_field")
        client.organizations._query_zendesk.return_value = iter([field])
        stream = Organizations(client=client, config=make_config())

        schema = {"properties": {"organization_fields": {"properties": {}}}}
        result = stream._add_custom_fields(schema)

        self.assertEqual(result["properties"]["organization_fields"]["properties"]["my_field"],
                          {"type": ["integer", "null"]})

    @patch('tap_zendesk.streams.organizations.raise_or_log_zenpy_apiexception')
    def test_add_custom_fields_api_exception_delegates_to_helper(self, mock_raise_or_log):
        client = MagicMock()
        client.organizations._query_zendesk.side_effect = APIException('{"error": "boom"}')
        stream = Organizations(client=client, config=make_config())
        mock_raise_or_log.return_value = {"fallback": True}

        schema = {"properties": {"organization_fields": {"properties": {}}}}
        result = stream._add_custom_fields(schema)

        self.assertEqual(result, {"fallback": True})
        mock_raise_or_log.assert_called_once()

    def test_sync_yields_records_and_updates_bookmark(self):
        client = MagicMock()
        org = MagicMock(updated_at="2021-06-01T00:00:00Z")
        client.organizations.incremental.return_value = iter([org])
        stream = Organizations(client=client, config=make_config())
        stream.stream = MagicMock()

        state = {}
        results = list(stream.sync(state))

        self.assertEqual(len(results), 1)
        self.assertEqual(state["bookmarks"]["organizations"]["updated_at"], "2021-06-01T00:00:00Z")

    def test_check_access_calls_incremental(self):
        client = MagicMock()
        stream = Organizations(client=client, config=make_config())
        stream.check_access()
        client.organizations.incremental.assert_called_once()


class TestTicketMetricEvents(unittest.TestCase):

    def test_sync_yields_new_records_and_updates_bookmark(self):
        client = MagicMock()
        future_time = (datetime.datetime.now(datetime.timezone.utc) +
                       datetime.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        event = MagicMock(time=future_time)
        client.tickets.metrics_incremental.return_value = iter([event])

        stream = TicketMetricEvents(client=client, config=make_config())
        stream.stream = MagicMock()
        stream.count = 0

        state = {"bookmarks": {"ticket_metric_events": {"time": "2020-01-01T00:00:00Z"}}}
        results = list(stream.sync(state))

        self.assertEqual(len(results), 1)
        self.assertEqual(state["bookmarks"]["ticket_metric_events"]["time"], future_time)
        self.assertEqual(stream.count, 1)

    def test_sync_skips_records_older_than_parsed_start(self):
        client = MagicMock()
        old_time = "2019-01-01T00:00:00Z"
        event = MagicMock(time=old_time)
        client.tickets.metrics_incremental.return_value = iter([event])

        stream = TicketMetricEvents(client=client, config=make_config())
        stream.stream = MagicMock()
        stream.count = 0

        state = {"bookmarks": {"ticket_metric_events": {"time": "2020-01-01T00:00:00Z"}}}
        results = list(stream.sync(state))

        self.assertEqual(results, [])
        self.assertEqual(stream.count, 1)

    def test_check_access_calls_metrics_incremental(self):
        client = MagicMock()
        stream = TicketMetricEvents(client=client, config=make_config())
        stream.check_access()
        client.tickets.metrics_incremental.assert_called_once()

    def test_check_access_swallows_not_found_error(self):
        client = MagicMock()
        client.tickets.metrics_incremental.side_effect = ZendeskNotFoundError("not found")
        stream = TicketMetricEvents(client=client, config=make_config())
        stream.check_access()  # should not raise


class TestGroupMemberships(unittest.TestCase):

    def _make_stream(self):
        stream = GroupMemberships(client=MagicMock(), config=make_config())
        stream.stream = MagicMock()
        return stream

    @patch.object(GroupMemberships, 'get_objects')
    def test_sync_yields_record_with_new_updated_at(self, mock_get_objects):
        stream = self._make_stream()
        mock_get_objects.return_value = iter([
            {"id": 1, "updated_at": "2021-01-01T00:00:00Z"},
        ])
        state = {}
        results = list(stream.sync(state))
        self.assertEqual(len(results), 1)
        self.assertEqual(state["bookmarks"]["group_memberships"]["updated_at"], "2021-01-01T00:00:00Z")

    @patch.object(GroupMemberships, 'get_objects')
    def test_sync_skips_record_older_than_bookmark(self, mock_get_objects):
        stream = self._make_stream()
        mock_get_objects.return_value = iter([
            {"id": 1, "updated_at": "2019-01-01T00:00:00Z"},
        ])
        state = {"bookmarks": {"group_memberships": {"updated_at": "2020-06-01T00:00:00Z"}}}
        results = list(stream.sync(state))
        self.assertEqual(results, [])

    @patch.object(GroupMemberships, 'get_objects')
    def test_sync_yields_record_missing_updated_at_but_with_id(self, mock_get_objects):
        stream = self._make_stream()
        mock_get_objects.return_value = iter([
            {"id": 5, "updated_at": None},
        ])
        results = list(stream.sync({}))
        self.assertEqual(len(results), 1)

    @patch.object(GroupMemberships, 'get_objects')
    def test_sync_skips_record_with_no_id_or_updated_at(self, mock_get_objects):
        stream = self._make_stream()
        mock_get_objects.return_value = iter([
            {"id": None, "updated_at": None},
        ])
        results = list(stream.sync({}))
        self.assertEqual(results, [])


class TestTriggerRevisions(unittest.TestCase):

    def test_check_access_is_noop(self):
        stream = TriggerRevisions(client=MagicMock(), config=make_config())
        self.assertIsNone(stream.check_access())

    def test_get_stream_endpoint_uses_parent_trigger_id(self):
        stream = TriggerRevisions(client=MagicMock(), config=make_config())
        url = stream.get_stream_endpoint(parent_obj={"id": 7})
        self.assertEqual(url, "https://foo.zendesk.com/api/v2/triggers/7/revisions")

    def test_get_stream_endpoint_without_parent_id_raises(self):
        stream = TriggerRevisions(client=MagicMock(), config=make_config())
        with self.assertRaises(ValueError):
            stream.get_stream_endpoint(parent_obj={})

    def test_modify_object_adds_trigger_id_from_parent(self):
        stream = TriggerRevisions(client=MagicMock(), config=make_config())
        record = {"id": 1}
        result = stream.modify_object(record, parent_record={"id": 99})
        self.assertEqual(result["trigger_id"], 99)


class TestScheduleHolidays(unittest.TestCase):

    def test_check_access_is_noop(self):
        stream = ScheduleHolidays(client=MagicMock(), config=make_config())
        self.assertIsNone(stream.check_access())

    def test_get_stream_endpoint_uses_parent_schedule_id(self):
        stream = ScheduleHolidays(client=MagicMock(), config=make_config())
        url = stream.get_stream_endpoint(parent_obj={"id": 7})
        self.assertEqual(url, "https://foo.zendesk.com/api/v2/business_hours/schedules/7/holidays")

    def test_get_stream_endpoint_without_parent_id_raises(self):
        stream = ScheduleHolidays(client=MagicMock(), config=make_config())
        with self.assertRaises(ValueError):
            stream.get_stream_endpoint(parent_obj={})

    def test_modify_object_adds_schedule_id_from_parent(self):
        stream = ScheduleHolidays(client=MagicMock(), config=make_config())
        record = {"id": 1}
        result = stream.modify_object(record, parent_record={"id": 99})
        self.assertEqual(result["schedule_id"], 99)


class TestMacroAttachments(unittest.TestCase):

    def test_check_access_is_noop(self):
        stream = MacroAttachments(client=MagicMock(), config=make_config())
        self.assertIsNone(stream.check_access())

    def test_get_stream_endpoint_uses_parent_macro_id(self):
        stream = MacroAttachments(client=MagicMock(), config=make_config())
        url = stream.get_stream_endpoint(parent_obj={"id": 7})
        self.assertEqual(url, "https://foo.zendesk.com/api/v2/macros/7/attachments")

    def test_get_stream_endpoint_without_parent_id_raises(self):
        stream = MacroAttachments(client=MagicMock(), config=make_config())
        with self.assertRaises(ValueError):
            stream.get_stream_endpoint(parent_obj={})

    def test_modify_object_adds_macro_id_from_parent(self):
        stream = MacroAttachments(client=MagicMock(), config=make_config())
        record = {"id": 1}
        result = stream.modify_object(record, parent_record={"id": 99})
        self.assertEqual(result["macro_id"], 99)


class TestTicketForms(unittest.TestCase):

    def test_sync_yields_record_with_new_updated_at(self):
        client = MagicMock()
        form = MagicMock(updated_at="2021-01-01T00:00:00Z")
        client.ticket_forms.return_value = iter([form])
        stream = TicketForms(client=client, config=make_config())
        stream.stream = MagicMock()

        state = {}
        results = list(stream.sync(state))

        self.assertEqual(len(results), 1)
        self.assertEqual(state["bookmarks"]["ticket_forms"]["updated_at"], "2021-01-01T00:00:00Z")

    def test_sync_skips_record_older_than_bookmark(self):
        client = MagicMock()
        form = MagicMock(updated_at="2019-01-01T00:00:00Z")
        client.ticket_forms.return_value = iter([form])
        stream = TicketForms(client=client, config=make_config())
        stream.stream = MagicMock()

        state = {"bookmarks": {"ticket_forms": {"updated_at": "2020-06-01T00:00:00Z"}}}
        results = list(stream.sync(state))
        self.assertEqual(results, [])

    def test_check_access_calls_client(self):
        client = MagicMock()
        stream = TicketForms(client=client, config=make_config())
        stream.check_access()
        client.ticket_forms.assert_called_once()

    @patch('tap_zendesk.streams.ticket_forms.raise_forbidden_if_access_denied')
    def test_check_access_api_exception_delegates_to_helper(self, mock_raise_forbidden):
        client = MagicMock()
        client.ticket_forms.side_effect = APIException('{"error": "boom"}')
        stream = TicketForms(client=client, config=make_config())

        stream.check_access()

        mock_raise_forbidden.assert_called_once()


class TestIncrementalTicketEvents(unittest.TestCase):

    @patch('tap_zendesk.streams.incremental_ticket_events.http.call_api')
    def test_check_access_calls_api(self, mock_call_api):
        stream = IncrementalTicketEvents(client=MagicMock(), config=make_config())
        stream.check_access()
        mock_call_api.assert_called_once()
        _, kwargs = mock_call_api.call_args
        self.assertIn('start_time', kwargs['params'])

    @patch('tap_zendesk.streams.incremental_ticket_events.http.get_incremental_export_offset')
    def test_get_objects_deduplicates_records_by_id(self, mock_get_incremental_export_offset):
        stream = IncrementalTicketEvents(client=MagicMock(), config=make_config())
        mock_get_incremental_export_offset.return_value = [
            {"ticket_events": [{"id": 1}, {"id": 2}, {"id": 1}]},
        ]
        results = list(stream.get_objects(0))
        self.assertEqual(results, [{"id": 1}, {"id": 2}])

    @patch('tap_zendesk.streams.incremental_ticket_events.http.get_incremental_export_offset')
    def test_get_objects_skips_records_without_id(self, mock_get_incremental_export_offset):
        stream = IncrementalTicketEvents(client=MagicMock(), config=make_config())
        mock_get_incremental_export_offset.return_value = [
            {"ticket_events": [{"foo": "bar"}]},
        ]
        results = list(stream.get_objects(0))
        self.assertEqual(results, [])


class TestSideConversations(unittest.TestCase):

    def test_check_access_is_noop(self):
        stream = SideConversations(client=MagicMock(), config=make_config())
        self.assertIsNone(stream.check_access())

    def test_get_stream_endpoint_uses_parent_ticket_id(self):
        stream = SideConversations(client=MagicMock(), config=make_config())
        url = stream.get_stream_endpoint(parent_obj={"id": 7})
        self.assertEqual(url, "https://foo.zendesk.com/api/v2/tickets/7/side_conversations")

    def test_get_stream_endpoint_without_parent_id_raises(self):
        stream = SideConversations(client=MagicMock(), config=make_config())
        with self.assertRaises(ValueError):
            stream.get_stream_endpoint(parent_obj={})


class TestMacroCategories(unittest.TestCase):

    def test_modify_object_wraps_record_in_name_key(self):
        stream = MacroCategories(client=MagicMock(), config=make_config())
        result = stream.modify_object("Support")
        self.assertEqual(result, {"name": "Support"})


class TestSatisfactionRatings(unittest.TestCase):

    def test_update_params_sets_start_time_from_bookmark(self):
        stream = SatisfactionRatings(client=MagicMock(), config=make_config())
        stream.replication_key = "updated_at"
        state = {}
        stream.params = {}
        stream.update_params(state=state)
        self.assertIn("start_time", stream.params)
        self.assertIsInstance(stream.params["start_time"], int)


class TestSLAPolicies(unittest.TestCase):

    def test_sync_yields_all_policies(self):
        client = MagicMock()
        policy = MagicMock()
        client.sla_policies.return_value = iter([policy])
        stream = SLAPolicies(client=client, config=make_config())
        stream.stream = MagicMock()

        results = list(stream.sync({}))
        self.assertEqual(len(results), 1)

    def test_check_access_calls_client(self):
        client = MagicMock()
        stream = SLAPolicies(client=client, config=make_config())
        stream.check_access()
        client.sla_policies.assert_called_once()

    @patch('tap_zendesk.streams.sla_policies.raise_forbidden_if_access_denied')
    def test_check_access_api_exception_delegates_to_helper(self, mock_raise_forbidden):
        client = MagicMock()
        client.sla_policies.side_effect = APIException('{"error": "boom"}')
        stream = SLAPolicies(client=client, config=make_config())

        stream.check_access()

        mock_raise_forbidden.assert_called_once()


class TestTalkPhoneNumbers(unittest.TestCase):

    def test_sync_yields_all_phone_numbers(self):
        client = MagicMock()
        phone_number = MagicMock()
        client.talk.phone_numbers.return_value = iter([phone_number])
        stream = TalkPhoneNumbers(client=client, config=make_config())
        stream.stream = MagicMock()

        results = list(stream.sync({}))
        self.assertEqual(len(results), 1)

    def test_check_access_swallows_not_found_error(self):
        client = MagicMock()
        client.talk.phone_numbers.side_effect = ZendeskNotFoundError("not found")
        stream = TalkPhoneNumbers(client=client, config=make_config())
        stream.check_access()  # should not raise

    def test_check_access_converts_403_http_error_to_forbidden(self):
        client = MagicMock()
        response = MagicMock(status_code=403)
        client.talk.phone_numbers.side_effect = requests.exceptions.HTTPError(response=response)
        stream = TalkPhoneNumbers(client=client, config=make_config())
        with self.assertRaises(ZendeskForbiddenError):
            stream.check_access()

    def test_check_access_reraises_non_403_http_error(self):
        client = MagicMock()
        response = MagicMock(status_code=500)
        client.talk.phone_numbers.side_effect = requests.exceptions.HTTPError(response=response)
        stream = TalkPhoneNumbers(client=client, config=make_config())
        with self.assertRaises(requests.exceptions.HTTPError):
            stream.check_access()

    @patch('tap_zendesk.streams.talk_phone_numbers.raise_forbidden_if_access_denied')
    def test_check_access_api_exception_delegates_to_helper(self, mock_raise_forbidden):
        client = MagicMock()
        client.talk.phone_numbers.side_effect = APIException('{"error": "boom"}')
        stream = TalkPhoneNumbers(client=client, config=make_config())

        stream.check_access()

        mock_raise_forbidden.assert_called_once()


class TestUserAttributeValues(unittest.TestCase):

    def test_modify_object_adds_user_id_from_parent(self):
        stream = UserAttributeValues(client=MagicMock(), config=make_config())
        record = {"id": 1}
        result = stream.modify_object(record, parent_record={"id": 99})
        self.assertEqual(result["user_id"], 99)
