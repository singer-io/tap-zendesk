import copy
import itertools
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from singer import Catalog, CatalogEntry

from tap_zendesk import do_sync, populate_class_schemas, validate_dependencies
from tap_zendesk import http, streams


CONFIG = {
    "start_date": "2023-01-01T00:00:00Z",
    "subdomain": "example",
    "access_token": "token",
}

_event_ids = itertools.count(1)


def catalog_entry(name, selected=True):
    return CatalogEntry(
        tap_stream_id=name,
        stream=name,
        schema={},
        metadata=[{"breadcrumb": [], "metadata": {"selected": selected}}],
    )


def epoch(timestamp):
    return int(datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())


def make_ticket(ticket_id, generated, event_times=(), **extra):
    ticket = {"id": ticket_id, "status": "open", "generated_timestamp": epoch(generated), **extra}
    if event_times:
        ticket["metric_events"] = {"reply_time": [
            {"id": next(_event_ids), "ticket_id": ticket_id, "time": event_time}
            for event_time in event_times
        ]}
    return ticket


def make_page(tickets, cursor=None):
    meta = {"has_more": bool(cursor)}
    if cursor:
        meta["after_cursor"] = cursor
    return Mock(json=Mock(return_value={"tickets": tickets, "meta": meta}))


def sync_tickets(state, responses, selected=("tickets", "ticket_metric_events")):
    with ExitStack() as stack:
        for name in ["tickets", *streams.Tickets.children]:
            stack.enter_context(patch.object(
                streams.STREAMS[name], "stream", catalog_entry(name, name in selected)))
        stack.enter_context(patch("tap_zendesk.streams.tickets.singer.write_state"))
        call_api = stack.enter_context(patch("tap_zendesk.http.call_api", side_effect=responses))
        result = list(streams.Tickets(config=CONFIG).sync(state))
    return result, call_api


def emitted(result, stream_name):
    return [record for entry, record in result if entry.tap_stream_id == stream_name]


class TestMetricEventBookmarks(unittest.TestCase):
    """The event bookmark must be honoured independently of the tickets bookmark."""

    def test_event_bookmark_behind_ticket_bookmark_does_not_skip_events(self):
        state = {"bookmarks": {
            "tickets": {"generated_timestamp": "2023-01-10T00:00:00Z"},
            "ticket_metric_events": {"time": "2023-01-03T00:00:00Z"},
        }}
        tickets = [
            make_ticket(1, "2023-01-05T00:00:00Z", ["2023-01-02T00:00:00Z", "2023-01-04T00:00:00Z"]),
            make_ticket(2, "2023-01-12T00:00:00Z", ["2023-01-11T00:00:00Z"]),
        ]
        result, _ = sync_tickets(state, [make_page(tickets)])

        self.assertEqual([t["id"] for t in emitted(result, "tickets")], [2])
        self.assertEqual([e["time"] for e in emitted(result, "ticket_metric_events")],
                         ["2023-01-04T00:00:00Z", "2023-01-11T00:00:00Z"])
        self.assertEqual(state["bookmarks"]["tickets"]["generated_timestamp"], "2023-01-12T00:00:00Z")
        self.assertEqual(state["bookmarks"]["ticket_metric_events"]["time"], "2023-01-11T00:00:00Z")

    def test_event_bookmark_ahead_of_ticket_bookmark_skips_synced_events(self):
        state = {"bookmarks": {
            "tickets": {"generated_timestamp": "2023-01-03T00:00:00Z"},
            "ticket_metric_events": {"time": "2023-01-10T00:00:00Z"},
        }}
        tickets = [
            make_ticket(1, "2023-01-05T00:00:00Z", ["2023-01-04T00:00:00Z"]),
            make_ticket(2, "2023-01-12T00:00:00Z", ["2023-01-11T00:00:00Z"]),
        ]
        result, _ = sync_tickets(state, [make_page(tickets)])

        self.assertEqual([t["id"] for t in emitted(result, "tickets")], [1, 2])
        self.assertEqual([e["time"] for e in emitted(result, "ticket_metric_events")],
                         ["2023-01-11T00:00:00Z"])
        self.assertEqual(state["bookmarks"]["ticket_metric_events"]["time"], "2023-01-11T00:00:00Z")

    def test_missing_event_bookmark_falls_back_to_start_date(self):
        state = {"bookmarks": {"tickets": {"generated_timestamp": "2023-01-10T00:00:00Z"}}}
        tickets = [make_ticket(1, "2023-01-05T00:00:00Z", ["2023-01-04T00:00:00Z"])]
        result, _ = sync_tickets(state, [make_page(tickets)])

        self.assertEqual(emitted(result, "tickets"), [])
        self.assertEqual(len(emitted(result, "ticket_metric_events")), 1)
        self.assertEqual(state["bookmarks"]["ticket_metric_events"]["time"], "2023-01-04T00:00:00Z")

    def test_event_bookmark_is_not_advanced_when_sync_fails(self):
        state = {"bookmarks": {
            "tickets": {"generated_timestamp": "2023-01-03T00:00:00Z"},
            "ticket_metric_events": {"time": "2023-01-03T00:00:00Z"},
        }}
        tickets = [make_ticket(1, "2023-01-12T00:00:00Z", ["2023-01-11T00:00:00Z"])]
        with self.assertRaisesRegex(RuntimeError, "boom"):
            sync_tickets(state, [make_page(tickets, cursor="c1"), RuntimeError("boom")])

        self.assertEqual(state["bookmarks"]["ticket_metric_events"], {"time": "2023-01-03T00:00:00Z"})

    def test_unselected_events_leave_their_bookmark_and_tickets_floor_alone(self):
        state = {"bookmarks": {
            "tickets": {"generated_timestamp": "2023-01-10T00:00:00Z"},
            "ticket_metric_events": {"time": "2023-01-03T00:00:00Z"},
        }}
        original = copy.deepcopy(state["bookmarks"]["ticket_metric_events"])
        tickets = [
            make_ticket(1, "2023-01-05T00:00:00Z", ["2023-01-04T00:00:00Z"]),
            make_ticket(2, "2023-01-12T00:00:00Z", ["2023-01-11T00:00:00Z"]),
        ]
        result, _ = sync_tickets(state, [make_page(tickets)], selected=("tickets",))

        self.assertEqual([t["id"] for t in emitted(result, "tickets")], [2])
        self.assertEqual(emitted(result, "ticket_metric_events"), [])
        self.assertEqual(state["bookmarks"]["ticket_metric_events"], original)

    def test_tickets_without_metric_events_are_handled(self):
        state = {}
        result, _ = sync_tickets(state, [make_page([make_ticket(1, "2023-01-05T00:00:00Z")])])

        self.assertEqual(len(emitted(result, "tickets")), 1)
        self.assertEqual(emitted(result, "ticket_metric_events"), [])

    def test_selecting_events_selects_tickets(self):
        selected = ["ticket_metric_events"]
        validate_dependencies(selected)
        self.assertEqual(selected, ["ticket_metric_events", "tickets"])


class TestTicketsRequest(unittest.TestCase):
    def test_uses_list_tickets_with_metric_event_sideloads(self):
        _, call_api = sync_tickets({}, [make_page([make_ticket(1, "2023-01-05T00:00:00Z")])])

        self.assertEqual(call_api.call_args.args[0], "https://example.zendesk.com/api/v2/tickets")
        self.assertEqual(
            call_api.call_args.kwargs["params"],
            {"page[size]": 100, "sort": "updated_at", "include": "metric_sets,metric_events"})


class TestRetiredStreams(unittest.TestCase):
    def make_catalog(self, retired_selected):
        return Catalog(streams=[
            catalog_entry("tickets", False),
            catalog_entry("incremental_ticket_events", retired_selected),
        ])

    def test_populate_class_schemas_ignores_unknown_streams(self):
        for selected, level in ((True, "warning"), (False, "info")):
            with self.subTest(selected=selected), \
                 patch("tap_zendesk.LOGGER") as logger, \
                 patch.object(streams.Tickets, "stream", None):
                catalog = self.make_catalog(selected)
                populate_class_schemas(catalog)

                self.assertIs(streams.Tickets.stream, catalog.streams[0])
                getattr(logger, level).assert_called_once()
                self.assertIn("incremental_ticket_events", getattr(logger, level).call_args.args)

    @patch("tap_zendesk.zendesk_metrics.log_aggregate_rates")
    @patch("tap_zendesk.singer.write_state")
    @patch("tap_zendesk.singer.write_schema")
    def test_do_sync_skips_unknown_streams(self, write_schema, write_state, _log_rates):
        for selected in (True, False):
            with self.subTest(selected=selected):
                write_schema.reset_mock()
                do_sync(None, self.make_catalog(selected), {}, CONFIG)
                write_schema.assert_not_called()


class TestPaginationProgress(unittest.TestCase):
    @patch("tap_zendesk.http.call_api")
    def test_missing_repeated_and_cyclic_cursors_fail(self, call_api):
        for cursors in ([None], [""], ["a", "a"], ["a", "b", "a"]):
            with self.subTest(cursors=cursors):
                call_api.reset_mock()
                call_api.side_effect = [
                    Mock(json=Mock(return_value={
                        "meta": {"has_more": True, "after_cursor": cursor},
                    }))
                    for cursor in cursors
                ]
                with self.assertRaisesRegex(ValueError, "(Missing|Repeated) 'after_cursor'"):
                    list(http.get_cursor_based("url", "token", 300, 100))
                self.assertEqual(call_api.call_count, len(cursors))

    @patch("tap_zendesk.http.call_api")
    def test_resumed_cursor_must_advance(self, call_api):
        call_api.return_value.json.return_value = {
            "meta": {"has_more": True, "after_cursor": "initial"},
        }
        with self.assertRaisesRegex(ValueError, "Repeated 'after_cursor'"):
            list(http.get_cursor_based("url", "token", 300, 100, cursor="initial"))
        self.assertEqual(call_api.call_count, 1)

    @patch("tap_zendesk.http.call_api")
    def test_export_repeated_cursor_fails(self, call_api):
        call_api.return_value.json.return_value = {
            "end_of_stream": False, "after_cursor": "same",
        }
        with self.assertRaisesRegex(ValueError, "Repeated 'after_cursor'"):
            list(http.get_incremental_export("url", "token", 300, 1672531200, "metric_sets"))
        self.assertEqual(call_api.call_count, 2)

    @patch("tap_zendesk.http.call_api")
    def test_final_page_does_not_need_cursor(self, call_api):
        call_api.return_value.json.return_value = {"meta": {"has_more": False}}
        self.assertEqual(len(list(http.get_cursor_based("url", "token", 300, 100))), 1)
