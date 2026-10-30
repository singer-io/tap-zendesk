import base64
import copy
import itertools
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from singer import Catalog, CatalogEntry

from tap_zendesk import do_sync, populate_class_schemas, validate_dependencies
from tap_zendesk import http, streams
from tap_zendesk.exceptions import ZendeskNotFoundError
from tap_zendesk.streams.ticket_metric_events import extract_metric_events


CONFIG = {
    "start_date": "2023-01-01T00:00:00Z",
    "subdomain": "example",
    "access_token": "token",
}
API_TOKEN_CONFIG = {
    "start_date": "2023-01-01T00:00:00Z",
    "subdomain": "example",
    "email": "user@example.com",
    "api_token": "secret",
}
API_TOKEN_HEADER = "Basic " + base64.b64encode(b"user@example.com/token:secret").decode()

_ids = itertools.count(1)


def catalog_entry(name, selected=True):
    return CatalogEntry(
        tap_stream_id=name,
        stream=name,
        schema={},
        metadata=[{"breadcrumb": [], "metadata": {"selected": selected}}],
    )


def epoch(timestamp):
    return int(datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())


def make_ticket(ticket_id, generated, **extra):
    return {"id": ticket_id, "status": "open", "generated_timestamp": epoch(generated), **extra}


def make_event(event_time, ticket_id=None):
    event = {"id": next(_ids), "metric": "reply_time", "type": "measure", "time": event_time}
    if ticket_id:
        event["ticket_id"] = ticket_id
    return event


def events_response(*event_times):
    """A Show Ticket response with `include=metric_events`, events grouped by metric name."""
    return {"ticket": {"id": 0}, "metric_events": {"reply_time": [make_event(t) for t in event_times]}}


def make_page(tickets, cursor=None, **sideloads):
    meta = {"has_more": bool(cursor)}
    if cursor:
        meta["after_cursor"] = cursor
    return Mock(json=Mock(return_value={"tickets": tickets, "meta": meta, **sideloads}))


class Run:
    """Everything observable after syncing the tickets stream against mocked Zendesk responses."""
    def __init__(self):
        self.records = []
        self.list_calls = None
        self.event_requests = []
        self.state_writes = []

    def emitted(self, stream_name):
        return [record for entry, record in self.records if entry.tap_stream_id == stream_name]


def sync_tickets(state, pages, events=None, selected=("tickets", "ticket_metric_events"), batch_size=None):
    """
    `pages`: responses of List Tickets (an exception in the list is raised as the next request).
    `events`: ticket id -> Show Ticket response (or exception) returned by the per-ticket request.
    """
    run = Run()
    events = events or {}

    async def show_ticket(session, url, request_timeout, params=None, headers=None):
        ticket_id = int(url.rsplit("/", 1)[1])
        run.event_requests.append((url, params))
        result = events.get(ticket_id, {})
        if isinstance(result, Exception):
            raise result
        return copy.deepcopy(result)

    with ExitStack() as stack:
        for name in ["tickets", *streams.Tickets.children]:
            stack.enter_context(patch.object(
                streams.STREAMS[name], "stream", catalog_entry(name, name in selected)))
        stack.enter_context(patch(
            "tap_zendesk.streams.tickets.singer.write_state",
            side_effect=lambda written: run.state_writes.append(copy.deepcopy(written))))
        stack.enter_context(patch("tap_zendesk.http.call_api_async", new=show_ticket))
        # Other tests leave these module settings changed, so pin them and never really sleep
        stack.enter_context(patch("tap_zendesk.streams.tickets.CONCURRENCY_LIMIT", batch_size or 20))
        stack.enter_context(patch("tap_zendesk.streams.tickets.AUDITS_REQUEST_PER_MINUTE", 450))
        stack.enter_context(patch("tap_zendesk.streams.tickets.time.sleep"))
        run.list_calls = stack.enter_context(patch("tap_zendesk.http.call_api", side_effect=pages))
        try:
            for record in streams.Tickets(config=CONFIG).sync(state):
                run.records.append(record)
        except Exception as error:  # pylint: disable=broad-except
            run.error = error
        else:
            run.error = None
    return run


class TestMetricEventBookmarks(unittest.TestCase):
    """The event bookmark must be honoured independently of the tickets bookmark."""

    def test_event_bookmark_behind_ticket_bookmark_does_not_skip_events(self):
        state = {"bookmarks": {
            "tickets": {"generated_timestamp": "2023-01-10T00:00:00Z"},
            "ticket_metric_events": {"time": "2023-01-03T00:00:00Z"},
        }}
        run = sync_tickets(
            state,
            [make_page([make_ticket(1, "2023-01-05T00:00:00Z"), make_ticket(2, "2023-01-12T00:00:00Z")])],
            events={
                1: events_response("2023-01-02T00:00:00Z", "2023-01-04T00:00:00Z"),
                2: events_response("2023-01-11T00:00:00Z"),
            })

        self.assertIsNone(run.error)
        self.assertEqual([t["id"] for t in run.emitted("tickets")], [2])
        self.assertEqual([e["time"] for e in run.emitted("ticket_metric_events")],
                         ["2023-01-04T00:00:00Z", "2023-01-11T00:00:00Z"])
        self.assertEqual(state["bookmarks"]["tickets"]["generated_timestamp"], "2023-01-12T00:00:00Z")
        self.assertEqual(state["bookmarks"]["ticket_metric_events"]["time"], "2023-01-11T00:00:00Z")

    def test_event_bookmark_ahead_of_ticket_bookmark_skips_synced_events(self):
        state = {"bookmarks": {
            "tickets": {"generated_timestamp": "2023-01-03T00:00:00Z"},
            "ticket_metric_events": {"time": "2023-01-10T00:00:00Z"},
        }}
        run = sync_tickets(
            state,
            [make_page([make_ticket(1, "2023-01-05T00:00:00Z"), make_ticket(2, "2023-01-12T00:00:00Z")])],
            events={1: events_response("2023-01-04T00:00:00Z"), 2: events_response("2023-01-11T00:00:00Z")})

        self.assertEqual([t["id"] for t in run.emitted("tickets")], [1, 2])
        self.assertEqual([e["time"] for e in run.emitted("ticket_metric_events")], ["2023-01-11T00:00:00Z"])
        # Ticket 1 cannot hold events newer than the event bookmark, so it is not requested
        self.assertEqual([url for url, _ in run.event_requests],
                         ["https://example.zendesk.com/api/v2/tickets/2"])
        self.assertEqual(state["bookmarks"]["ticket_metric_events"]["time"], "2023-01-11T00:00:00Z")

    def test_missing_event_bookmark_falls_back_to_start_date(self):
        state = {"bookmarks": {"tickets": {"generated_timestamp": "2023-01-10T00:00:00Z"}}}
        run = sync_tickets(
            state, [make_page([make_ticket(1, "2023-01-05T00:00:00Z")])],
            events={1: events_response("2023-01-04T00:00:00Z")})

        self.assertEqual(run.emitted("tickets"), [])
        self.assertEqual(len(run.emitted("ticket_metric_events")), 1)
        self.assertEqual(state["bookmarks"]["ticket_metric_events"]["time"], "2023-01-04T00:00:00Z")

    def test_event_bookmark_is_not_advanced_when_sync_fails(self):
        state = {"bookmarks": {
            "tickets": {"generated_timestamp": "2023-01-03T00:00:00Z"},
            "ticket_metric_events": {"time": "2023-01-03T00:00:00Z"},
        }}
        original = copy.deepcopy(state)
        run = sync_tickets(
            state,
            [make_page([make_ticket(1, "2023-01-12T00:00:00Z")], cursor="c1"), RuntimeError("boom")],
            events={1: events_response("2023-01-11T00:00:00Z")}, batch_size=1)

        self.assertIsInstance(run.error, RuntimeError)
        self.assertEqual(state, original)
        self.assertTrue(all(written == original for written in run.state_writes))

    def test_unselected_events_are_not_fetched_and_keep_their_bookmark(self):
        state = {"bookmarks": {
            "tickets": {"generated_timestamp": "2023-01-10T00:00:00Z"},
            "ticket_metric_events": {"time": "2023-01-03T00:00:00Z"},
        }}
        run = sync_tickets(
            state,
            [make_page([make_ticket(1, "2023-01-05T00:00:00Z"), make_ticket(2, "2023-01-12T00:00:00Z")])],
            selected=("tickets",))

        self.assertEqual([t["id"] for t in run.emitted("tickets")], [2])
        self.assertEqual(run.event_requests, [])
        self.assertEqual(state["bookmarks"]["ticket_metric_events"], {"time": "2023-01-03T00:00:00Z"})

    def test_deleted_tickets_and_tickets_without_events_are_handled(self):
        run = sync_tickets(
            {},
            [make_page([make_ticket(1, "2023-01-05T00:00:00Z", status="deleted"),
                        make_ticket(2, "2023-01-06T00:00:00Z")])],
            events={2: {"ticket": {"id": 2}}})

        self.assertEqual(len(run.emitted("tickets")), 2)
        self.assertEqual(run.emitted("ticket_metric_events"), [])
        self.assertEqual([url for url, _ in run.event_requests],
                         ["https://example.zendesk.com/api/v2/tickets/2"])

    def test_ticket_gone_before_its_events_are_fetched_is_skipped(self):
        run = sync_tickets(
            {}, [make_page([make_ticket(1, "2023-01-05T00:00:00Z")])],
            events={1: ZendeskNotFoundError("gone")})

        self.assertIsNone(run.error)
        self.assertEqual(len(run.emitted("tickets")), 1)
        self.assertEqual(run.emitted("ticket_metric_events"), [])

    def test_events_are_requested_with_the_single_ticket_sideload(self):
        run = sync_tickets({}, [make_page([make_ticket(7, "2023-01-05T00:00:00Z")])],
                           events={7: events_response("2023-01-05T00:00:00Z")})

        self.assertEqual(run.event_requests,
                         [("https://example.zendesk.com/api/v2/tickets/7", {"include": "metric_events"})])
        self.assertEqual(run.emitted("ticket_metric_events")[0]["ticket_id"], 7)

    def test_events_are_fetched_in_batches(self):
        tickets = [make_ticket(i, "2023-01-05T00:00:00Z") for i in range(1, 6)]
        run = sync_tickets({}, [make_page(tickets)],
                           events={i: events_response("2023-01-05T00:00:00Z") for i in range(1, 6)},
                           batch_size=2)

        self.assertEqual(len(run.emitted("ticket_metric_events")), 5)
        self.assertEqual(sorted(url for url, _ in run.event_requests),
                         [f"https://example.zendesk.com/api/v2/tickets/{i}" for i in range(1, 6)])

    def test_selecting_events_selects_tickets(self):
        selected = ["ticket_metric_events"]
        validate_dependencies(selected)
        self.assertEqual(selected, ["ticket_metric_events", "tickets"])


class TestExtractMetricEvents(unittest.TestCase):
    def test_supported_response_shapes(self):
        events = [{"id": 1}, {"id": 2}]
        cases = {
            "top-level dict of lists": {"metric_events": {"reply_time": events[:1], "resolution_time": events[1:]}},
            "top-level list": {"metric_events": events},
            "nested dict of lists": {"ticket": {"metric_events": {"reply_time": events}}},
            "nested list": {"ticket": {"metric_events": events}},
        }
        for name, response in cases.items():
            with self.subTest(name):
                self.assertEqual(extract_metric_events(response), events)

    def test_missing_or_unexpected_events_yield_nothing(self):
        for response in ({}, {"ticket": None}, {"ticket": {}}, {"metric_events": None}, {"metric_events": "x"}):
            with self.subTest(response=response):
                self.assertEqual(extract_metric_events(response), [])


class TestTicketMetricSets(unittest.TestCase):
    def sync_with_list(self, page):
        return sync_tickets({}, [page], selected=("tickets", "ticket_metrics"))

    def test_top_level_metric_sets_are_matched_to_their_ticket(self):
        run = self.sync_with_list(make_page(
            [make_ticket(1, "2023-01-05T00:00:00Z"), make_ticket(2, "2023-01-06T00:00:00Z")],
            metric_sets=[{"id": 20, "ticket_id": 2}, {"id": 10, "ticket_id": 1}]))

        self.assertEqual(run.emitted("ticket_metrics"), [{"id": 10, "ticket_id": 1}, {"id": 20, "ticket_id": 2}])

    def test_nested_metric_set_is_emitted(self):
        run = self.sync_with_list(make_page(
            [make_ticket(1, "2023-01-05T00:00:00Z", metric_set={"id": 10, "ticket_id": 1})]))

        self.assertEqual(run.emitted("ticket_metrics"), [{"id": 10, "ticket_id": 1}])

    def test_tickets_without_metric_sets_emit_none(self):
        run = self.sync_with_list(make_page(
            [make_ticket(1, "2023-01-05T00:00:00Z")], metric_sets=[{"id": 99, "ticket_id": 5}]))

        self.assertEqual(len(run.emitted("tickets")), 1)
        self.assertEqual(run.emitted("ticket_metrics"), [])

    def test_list_request_does_not_ask_for_metric_events(self):
        run = self.sync_with_list(make_page([make_ticket(1, "2023-01-05T00:00:00Z")]))

        self.assertEqual(run.list_calls.call_args.args[0], "https://example.zendesk.com/api/v2/tickets")
        self.assertEqual(run.list_calls.call_args.kwargs["params"],
                         {"page[size]": 100, "sort": "updated_at", "include": "metric_sets"})


class TestInterruptedTicketSync(unittest.TestCase):
    """Results are ordered by updated_at, so the tickets bookmark is only safe once every page was read."""

    def interrupted_run(self, state):
        # Page 1 holds a late ticket; the unseen January 5 ticket is on the page that fails
        return sync_tickets(
            state,
            [make_page([make_ticket(1, "2023-01-20T00:00:00Z")], cursor="c1"), RuntimeError("interrupted")],
            selected=("tickets",), batch_size=1)

    def test_checkpoints_never_advance_the_bookmark_part_way(self):
        state = {}
        run = self.interrupted_run(state)

        self.assertIsInstance(run.error, RuntimeError)
        self.assertEqual([t["id"] for t in run.emitted("tickets")], [1])
        self.assertTrue(run.state_writes)
        for written in run.state_writes:
            self.assertNotEqual(written.get("bookmarks", {}).get("tickets", {}).get("generated_timestamp"),
                                "2023-01-20T00:00:00Z")

    def test_resumed_sync_still_reaches_the_ticket_missed_by_the_interruption(self):
        state = {"bookmarks": {"tickets": {"generated_timestamp": "2023-01-01T00:00:00Z"}}}
        run = self.interrupted_run(state)
        persisted = run.state_writes[-1]

        resumed = sync_tickets(
            persisted,
            [make_page([make_ticket(1, "2023-01-20T00:00:00Z")], cursor="c1"),
             make_page([make_ticket(2, "2023-01-05T00:00:00Z")])],
            selected=("tickets",))

        self.assertIsNone(resumed.error)
        self.assertEqual([t["id"] for t in resumed.emitted("tickets")], [1, 2])
        self.assertEqual(persisted["bookmarks"]["tickets"]["generated_timestamp"], "2023-01-20T00:00:00Z")

    def test_bookmark_is_committed_after_a_complete_walk(self):
        state = {}
        run = sync_tickets(
            state,
            [make_page([make_ticket(1, "2023-01-20T00:00:00Z")], cursor="c1"),
             make_page([make_ticket(2, "2023-01-05T00:00:00Z")])],
            selected=("tickets",))

        self.assertIsNone(run.error)
        self.assertEqual(state["bookmarks"]["tickets"]["generated_timestamp"], "2023-01-20T00:00:00Z")
        self.assertEqual(run.state_writes[-1], state)


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
    def test_do_sync_skips_unknown_streams(self, write_schema, _write_state, _log_rates):
        for selected in (True, False):
            with self.subTest(selected=selected):
                write_schema.reset_mock()
                do_sync(None, self.make_catalog(selected), {}, CONFIG)
                write_schema.assert_not_called()


class TestAuthentication(unittest.TestCase):
    """Both documented configurations (OAuth access_token, or email and api_token) must work."""

    def test_get_credential(self):
        self.assertEqual(http.get_credential(CONFIG), "token")
        self.assertEqual(http.get_credential(API_TOKEN_CONFIG), API_TOKEN_HEADER)
        # OAuth has precedence, as when creating the Zenpy client
        self.assertEqual(http.get_credential({**API_TOKEN_CONFIG, "access_token": "token"}), "token")
        for config in ({}, {"email": "user@example.com"}, {"api_token": "secret"}):
            with self.subTest(config=config), self.assertRaisesRegex(ValueError, "authentication"):
                http.get_credential(config)

    def test_authorization_header(self):
        self.assertEqual(http.build_headers(http.get_credential(CONFIG))["Authorization"], "Bearer token")
        self.assertEqual(http.build_headers(http.get_credential(API_TOKEN_CONFIG))["Authorization"], API_TOKEN_HEADER)

    @patch("tap_zendesk.http.requests.get")
    def test_organizations_authenticate_with_either_configuration(self, mock_get):
        mock_get.return_value = Mock(status_code=200, json=Mock(return_value={
            "organizations": [{"id": 1, "updated_at": "2023-02-01T00:00:00Z"}], "meta": {"has_more": False}}))
        expected = {"oauth": "Bearer token", "api token": API_TOKEN_HEADER}

        for name, config in (("oauth", CONFIG), ("api token", API_TOKEN_CONFIG)):
            with self.subTest(name):
                mock_get.reset_mock()
                stream = streams.Organizations(client=Mock(), config=config)
                stream.stream = catalog_entry("organizations")

                stream.check_access()
                records = list(stream.sync({}))

                self.assertEqual(len(records), 1)
                self.assertEqual(mock_get.call_count, 2)
                for call in mock_get.call_args_list:
                    self.assertEqual(call.kwargs["headers"]["Authorization"], expected[name])

    @patch("tap_zendesk.http.requests.get")
    def test_other_streams_authenticate_with_api_token(self, mock_get):
        mock_get.return_value = Mock(status_code=200, json=Mock(return_value={
            "tickets": [], "audits": [], "meta": {"has_more": False}}))

        for stream_class in (streams.Tickets, streams.TicketAudits, streams.Groups):
            with self.subTest(stream_class.__name__):
                mock_get.reset_mock()
                stream_class(client=Mock(), config=API_TOKEN_CONFIG).check_access()
                self.assertEqual(mock_get.call_args.kwargs["headers"]["Authorization"], API_TOKEN_HEADER)


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
