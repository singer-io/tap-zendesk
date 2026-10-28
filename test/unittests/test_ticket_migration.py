import copy
import unittest
from contextlib import ExitStack
from unittest.mock import Mock, patch

from singer import Catalog, CatalogEntry

from tap_zendesk import do_sync, populate_class_schemas, validate_dependencies
from tap_zendesk import http, streams


CONFIG = {
    "start_date": "2023-01-01T00:00:00Z",
    "subdomain": "example",
    "access_token": "token",
}


def catalog_entry(name, selected=True):
    return CatalogEntry(
        tap_stream_id=name,
        stream=name,
        schema={},
        metadata=[{"breadcrumb": [], "metadata": {"selected": selected}}],
    )


class TestMetricEventMigration(unittest.TestCase):
    def make_stream(self):
        stream = streams.TicketMetricEvents(config=CONFIG)
        stream.stream = catalog_entry(stream.name)
        return stream

    @patch("tap_zendesk.http.call_api")
    def test_divergent_bookmarks_resume_events_independently(self, call_api):
        for ticket_bookmark in ("2023-01-10T00:00:00Z", "2023-01-01T00:00:00Z"):
            with self.subTest(ticket_bookmark=ticket_bookmark):
                state = {"bookmarks": {
                    "tickets": {"generated_timestamp": ticket_bookmark},
                    "ticket_metric_events": {"time": "2023-01-03T00:00:00Z"},
                }}
                pages = [
                    {
                        "ticket_metric_events": [
                            {"id": 1, "time": "2023-01-02T00:00:00Z"},
                            {"id": 2, "time": "2023-01-03T00:00:00Z"},
                            {"id": 3, "time": "2023-01-04T00:00:00Z"},
                        ],
                        "meta": {"has_more": True, "after_cursor": "next"},
                    },
                    {
                        "ticket_metric_events": [
                            {"id": 4, "time": "2023-01-05T00:00:00Z"},
                        ],
                        "meta": {"has_more": False},
                    },
                ]
                requests = []

                def respond(*args, **kwargs):
                    requests.append(copy.deepcopy(kwargs["params"]))
                    return Mock(json=Mock(return_value=pages[len(requests) - 1]))

                call_api.side_effect = respond
                result = list(self.make_stream().sync(state))
                self.assertEqual([record["id"] for _, record in result], [2, 3, 4])
                self.assertEqual(requests[0]["start_time"], 1672703999)
                self.assertEqual(requests[1]["page[after]"], "next")
                self.assertEqual(state["bookmarks"]["ticket_metric_events"]["time"],
                                 "2023-01-05T00:00:00Z")
                self.assertEqual(state["bookmarks"]["tickets"]["generated_timestamp"],
                                 ticket_bookmark)

    @patch("tap_zendesk.http.get_cursor_based")
    def test_missing_event_bookmark_uses_start_date(self, paginate):
        paginate.return_value = [{
            "ticket_metric_events": [{"id": 1, "time": CONFIG["start_date"]}],
        }]
        state = {"bookmarks": {"tickets": {"generated_timestamp": "2023-01-10T00:00:00Z"}}}
        result = list(self.make_stream().sync(state))
        self.assertEqual(len(result), 1)
        self.assertEqual(paginate.call_args.kwargs["params"]["start_time"], 1672531199)
        self.assertEqual(state["bookmarks"]["ticket_metric_events"]["time"], CONFIG["start_date"])

    @patch("tap_zendesk.http.call_api")
    def test_pagination_failure_does_not_advance_event_bookmark(self, call_api):
        state = {"bookmarks": {
            "tickets": {"generated_timestamp": "2023-01-10T00:00:00Z"},
            "ticket_metric_events": {"time": CONFIG["start_date"]},
        }}
        original_state = copy.deepcopy(state)
        call_api.return_value.json.return_value = {
            "ticket_metric_events": [{"id": 1, "time": "2023-01-05T00:00:00Z"}],
            "meta": {"has_more": True},
        }
        with self.assertRaisesRegex(ValueError, "Missing 'after_cursor'"):
            list(self.make_stream().sync(state))
        self.assertEqual(state, original_state)

    def test_selecting_events_does_not_require_tickets(self):
        selected = ["ticket_metric_events"]
        validate_dependencies(selected)
        self.assertEqual(selected, ["ticket_metric_events"])


class TestArchivedTicketCoverage(unittest.TestCase):
    @patch("tap_zendesk.streams.tickets.singer.write_state")
    @patch("tap_zendesk.http.call_api")
    def test_ticket_sync_uses_list_tickets_endpoint(self, call_api, write_state):
        ticket = {
            "id": 1,
            "status": "open",
            "generated_timestamp": 1672617600,
            "metric_set": {"id": 2},
        }
        call_api.return_value.json.return_value = {
            "tickets": [ticket],
            "meta": {"has_more": False},
        }
        state = {}
        with ExitStack() as stack:
            for name in ["tickets", *streams.Tickets.children]:
                stack.enter_context(patch.object(
                    streams.STREAMS[name], "stream",
                    catalog_entry(name, name in {"tickets", "ticket_metrics"}),
                ))
            result = list(streams.Tickets(config=CONFIG).sync(state))
        self.assertEqual([entry.tap_stream_id for entry, _ in result],
                         ["tickets", "ticket_metrics"])
        self.assertEqual(result[0][1]["id"], 1)
        self.assertEqual(call_api.call_args.args[0],
                         "https://example.zendesk.com/api/v2/tickets")
        self.assertEqual(call_api.call_args.kwargs["params"],
                         {"page[size]": 100, "sort": "updated_at", "include": "metric_sets"})
        self.assertEqual(state["bookmarks"]["tickets"]["generated_timestamp"],
                         "2023-01-02T00:00:00Z")


class TestRetiredCatalogMigration(unittest.TestCase):
    def test_old_catalog_requires_rediscovery_even_when_unselected(self):
        for selected in (True, False):
            with self.subTest(selected=selected):
                catalog = Catalog(streams=[
                    catalog_entry("tickets"),
                    catalog_entry("incremental_ticket_events", selected),
                ])
                with patch.object(streams.Tickets, "stream", None):
                    with self.assertRaisesRegex(ValueError, "Rediscover the catalog"):
                        populate_class_schemas(catalog)
                    self.assertIsNone(streams.Tickets.stream)
                with patch("tap_zendesk.singer.write_schema") as write_schema:
                    with self.assertRaisesRegex(ValueError, "Rediscover the catalog"):
                        do_sync(None, catalog, {}, CONFIG)
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
