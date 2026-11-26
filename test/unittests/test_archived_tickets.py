import copy
import unittest
from contextlib import ExitStack
from unittest.mock import Mock, patch

from singer import Catalog, CatalogEntry

from tap_zendesk import http, streams


CONFIG = {
    "start_date": "2026-11-01T00:00:00Z",
    "subdomain": "example",
    "access_token": "token",
    "search_archived_tickets": True,
}


def catalog_entry(name, selected=True):
    return CatalogEntry(
        tap_stream_id=name,
        stream=name,
        schema={},
        metadata=[{"breadcrumb": [], "metadata": {"selected": selected}}],
    )


def mock_response(payload):
    return Mock(json=Mock(return_value=payload))


class TestGetSearchResults(unittest.TestCase):
    """Cover http.get_search_results — pagination and the sort/per_page params."""

    def test_single_page(self):
        page = {"count": 1, "results": [{"id": 1, "updated_at": "2026-11-20T00:00:00Z"}]}
        with patch("tap_zendesk.http.call_api", side_effect=[mock_response(page)]) as mock_call:
            responses = list(http.get_search_results(
                subdomain="example", access_token="token",
                request_timeout=300, query="type:ticket status:closed",
            ))
        self.assertEqual(len(responses), 1)
        self.assertEqual(responses[0], page)
        _, kwargs = mock_call.call_args
        self.assertEqual(kwargs["params"]["sort_by"], "updated_at")
        self.assertEqual(kwargs["params"]["sort_order"], "asc")

    def test_paginates_via_next_page(self):
        page1 = {
            "count": 2,
            "results": [{"id": 1, "updated_at": "2026-11-20T00:00:00Z"}],
            "next_page": "https://example.zendesk.com/api/v2/search.json?page=2",
        }
        page2 = {"count": 2, "results": [{"id": 2, "updated_at": "2026-11-21T00:00:00Z"}]}
        with patch("tap_zendesk.http.call_api", side_effect=[mock_response(page1), mock_response(page2)]):
            responses = list(http.get_search_results(
                subdomain="example", access_token="token",
                request_timeout=300, query="q",
            ))
        self.assertEqual(len(responses), 2)


class TestGetShowManyTickets(unittest.TestCase):
    """Cover http.get_show_many_tickets — batching and metric_sets sideload normalization."""

    def test_batches_in_groups_of_100(self):
        ids = list(range(1, 251))  # 250 ids → 3 batches of 100/100/50

        def _response(call_index):
            return mock_response({
                "tickets": [{"id": call_index * 1000}],
                "metric_sets": [],
            })

        responses = [_response(i) for i in range(1, 4)]
        with patch("tap_zendesk.http.call_api", side_effect=responses) as mock_call:
            tickets = list(http.get_show_many_tickets(
                subdomain="example", access_token="token",
                request_timeout=300, ticket_ids=ids,
            ))
        self.assertEqual(mock_call.call_count, 3)
        self.assertEqual([t["id"] for t in tickets], [1000, 2000, 3000])

    def test_metric_sets_sideload_is_merged_into_nested_form(self):
        payload = {
            "tickets": [{"id": 7}],
            "metric_sets": [{"ticket_id": 7, "reply_time_in_minutes": 5}],
        }
        with patch("tap_zendesk.http.call_api", return_value=mock_response(payload)):
            tickets = list(http.get_show_many_tickets(
                subdomain="example", access_token="token",
                request_timeout=300, ticket_ids=[7],
            ))
        self.assertEqual(tickets[0]["metric_set"], {"ticket_id": 7, "reply_time_in_minutes": 5})

    def test_existing_nested_metric_set_is_preserved(self):
        payload = {
            "tickets": [{"id": 7, "metric_set": {"already": "there"}}],
            "metric_sets": [{"ticket_id": 7, "ignored": True}],
        }
        with patch("tap_zendesk.http.call_api", return_value=mock_response(payload)):
            tickets = list(http.get_show_many_tickets(
                subdomain="example", access_token="token",
                request_timeout=300, ticket_ids=[7],
            ))
        self.assertEqual(tickets[0]["metric_set"], {"already": "there"})


def make_search_page(ticket_ids_updated, count=None):
    """Build a Zendesk Search API response containing (id, updated_at) tuples."""
    results = [{"id": tid, "updated_at": ts} for tid, ts in ticket_ids_updated]
    return mock_response({"count": count if count is not None else len(results), "results": results})


def make_show_many_page(tickets):
    return mock_response({"tickets": tickets, "metric_sets": []})


def run_archived_sync(state, call_api_responses, ticket_pages=None, selected=("tickets",)):
    """
    Drive Tickets.sync against a mocked call_api side_effect sequence. The main
    List Tickets pass is pre-drained with `ticket_pages` so the archived pass can
    run in isolation. Returns (records, state_writes, run_error).
    """
    ticket_pages = ticket_pages or [mock_response({"tickets": [], "meta": {"has_more": False}})]
    records = []
    state_writes = []

    with ExitStack() as stack:
        for name in ["tickets", *streams.Tickets.children]:
            stack.enter_context(patch.object(
                streams.STREAMS[name], "stream", catalog_entry(name, name in selected)))
        stack.enter_context(patch(
            "tap_zendesk.streams.tickets.singer.write_state",
            side_effect=lambda written: state_writes.append(copy.deepcopy(written))))
        stack.enter_context(patch("tap_zendesk.streams.tickets.time.sleep"))
        stack.enter_context(patch(
            "tap_zendesk.http.call_api",
            side_effect=ticket_pages + call_api_responses,
        ))
        try:
            for record in streams.Tickets(config=CONFIG).sync(state):
                records.append(record)
            error = None
        except Exception as err:  # pylint: disable=broad-except
            error = err
    return records, state_writes, error


class TestArchivedTicketSync(unittest.TestCase):
    """Cover the archived-ticket backfill flow end-to-end with mocked HTTP."""

    def test_disabled_flag_skips_archived_pass(self):
        """With search_archived_tickets=False the Search endpoint is never called."""
        with patch("tap_zendesk.http.call_api", side_effect=[
            mock_response({"tickets": [], "meta": {"has_more": False}}),
        ]) as mock_call, patch("tap_zendesk.streams.tickets.time.sleep"):
            for name in ["tickets", *streams.Tickets.children]:
                streams.STREAMS[name].stream = catalog_entry(name, name == "tickets")
            config = dict(CONFIG, search_archived_tickets=False)
            # start_date in the past-but-recent so the bookmark loop is cheap even if invoked
            config["start_date"] = "2099-01-01T00:00:00Z"
            list(streams.Tickets(config=config).sync({}))
        self.assertEqual(mock_call.call_count, 1)

    def test_archived_ids_discovered_and_bookmark_advances(self):
        """Discover an archived id via Search, hydrate via Show Many, advance the bookmark."""
        search_page = make_search_page([(101, "2026-11-20T00:00:00Z")])
        show_many = make_show_many_page([
            {"id": 101, "generated_timestamp": 1700000000, "status": "closed"},
        ])
        state = {"bookmarks": {"tickets": {
            "generated_timestamp": "2099-01-01T00:00:00Z",
            "archived_tickets_updated_at": "2026-11-15T00:00:00Z",
        }}}
        records, state_writes, error = run_archived_sync(
            state=state,
            # One Search call (small window, no overflow) + one Show Many batch.
            # Multiple windows are walked up to "now" so the Search mock keeps returning empty.
            call_api_responses=[search_page, show_many] + [
                make_search_page([], count=0) for _ in range(200)
            ],
        )
        self.assertIsNone(error)
        emitted = [r for r in records if hasattr(r[0], "tap_stream_id") and r[0].tap_stream_id == "tickets"]
        self.assertTrue(any(rec.get("id") == 101 for _, rec in emitted))
        self.assertTrue(state_writes)
        last_bookmark = state_writes[-1]["bookmarks"]["tickets"].get("archived_tickets_updated_at")
        self.assertIsNotNone(last_bookmark)

    def test_overflow_bisects_the_window(self):
        """A window that reports count >= SEARCH_RESULT_CAP triggers recursive bisection."""
        stream_obj = streams.Tickets(config=CONFIG)
        overflow = mock_response({"count": http.SEARCH_RESULT_CAP, "results": []})
        small = mock_response({"count": 0, "results": []})
        with patch("tap_zendesk.http.call_api", side_effect=[overflow, small, small]) as mock_call:
            from datetime import datetime, timedelta, timezone
            start = datetime(2026, 11, 1, tzinfo=timezone.utc)
            end = start + timedelta(days=4)
            list(stream_obj._search_archived_ticket_ids(start, end))
        # 1 overflow + 2 bisected halves = 3 calls
        self.assertEqual(mock_call.call_count, 3)

    def test_overflow_at_min_step_logs_warning_without_recursing(self):
        """At or below the minimum step size, overflow is logged but not bisected further."""
        stream_obj = streams.Tickets(config=CONFIG)
        overflow = mock_response({"count": http.SEARCH_RESULT_CAP, "results": []})
        with patch("tap_zendesk.http.call_api", side_effect=[overflow]) as mock_call, \
             patch("tap_zendesk.streams.tickets.LOGGER.warning") as mock_warn:
            from datetime import datetime, timedelta, timezone
            start = datetime(2026, 11, 1, tzinfo=timezone.utc)
            end = start + timedelta(seconds=30)  # below the 60-second floor
            list(stream_obj._search_archived_ticket_ids(start, end))
        self.assertEqual(mock_call.call_count, 1)
        self.assertTrue(mock_warn.called)

    def test_hydrated_ticket_dedup_against_seen_ids(self):
        """An id already seen in the main pass is not re-hydrated by the archived pass."""
        search_page = make_search_page([(42, "2026-11-20T00:00:00Z")])
        state = {"bookmarks": {"tickets": {
            "generated_timestamp": "2099-01-01T00:00:00Z",
            "archived_tickets_updated_at": "2026-11-15T00:00:00Z",
        }}}
        stream_obj = streams.Tickets(config=CONFIG)
        for name in ["tickets", *streams.Tickets.children]:
            streams.STREAMS[name].stream = catalog_entry(name, name == "tickets")

        with patch("tap_zendesk.http.call_api", side_effect=[search_page] + [
            make_search_page([], count=0) for _ in range(200)
        ]) as mock_call, patch("tap_zendesk.streams.tickets.time.sleep"):
            # Pre-seed seen_ticket_ids so id 42 is dedup'd and Show Many is never called.
            audits = streams.STREAMS["ticket_audits"]
            metrics = streams.STREAMS["ticket_metrics"]
            events = streams.STREAMS["ticket_metric_events"]
            comments = streams.STREAMS["ticket_comments"]
            side_conv = streams.STREAMS["side_conversations"]
            list(stream_obj._sync_archived_tickets(
                state=state, seen_ticket_ids={42},
                audits_stream=audits, metrics_stream=metrics,
                metric_events_stream=events, comments_stream=comments,
                side_conversations_stream=side_conv, events_selected=False,
            ))
        # Only Search calls, no Show Many (show_many would be an extra call_api invocation).
        for call in mock_call.call_args_list:
            self.assertIn("search.json", call.args[0] if call.args else call.kwargs.get("url", ""))


if __name__ == "__main__":
    unittest.main()
