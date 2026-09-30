from typing import Dict
import time
import asyncio
import singer
from tap_zendesk import http
from tap_zendesk import metrics as zendesk_metrics
from tap_zendesk.streams.abstracts import (
    PaginatedStream,
    AUDITS_REQUEST_PER_MINUTE,
    CONCURRENCY_LIMIT,
    LOGGER,
)
from tap_zendesk.streams.ticket_audits import TicketAudits
from tap_zendesk.streams.ticket_metrics import TicketMetrics
from tap_zendesk.streams.ticket_metric_events import TicketMetricEvents
from tap_zendesk.streams.ticket_comments import TicketComments
from tap_zendesk.streams.side_conversations import SideConversations


class Tickets(PaginatedStream):
    name = "tickets"
    replication_method = "INCREMENTAL"
    # `generated_timestamp` was only available on the (now deprecated)
    # `incremental/tickets/cursor.json` endpoint; the standard `tickets`
    # endpoint exposes `updated_at` instead.
    replication_key = "updated_at"
    item_key = "tickets"
    # `incremental/tickets/cursor.json` is deprecated for third-party apps
    # (https://support.zendesk.com/hc/en-us/articles/11249701485338); use the
    # standard, cursor-paginated `tickets` list endpoint and filter client-side.
    endpoint = "tickets"
    pagination_type = "cursor"
    children = ['ticket_audits', 'ticket_metrics', 'ticket_comments', 'ticket_metric_events', 'side_conversations']

    def get_objects(self, **kwargs): # pylint: disable=arguments-differ
        """
        Retrieve tickets from the standard `tickets` list endpoint, side loading
        `metric_events` (top-level, keyed by `ticket_id`) alongside each page of
        tickets. `self.metric_events_by_ticket` is refreshed per page and is only
        valid for the duration of iterating that page's tickets.
        """
        kwargs.setdefault('params', {})
        kwargs['params'].setdefault('sort', 'updated_at')
        kwargs['params'].setdefault('include', 'metric_events')

        parent_obj = kwargs.get('parent_obj', {})
        url = self.get_stream_endpoint(parent_obj=parent_obj)

        for page in http.get_cursor_based(url, self.config['access_token'], self.request_timeout,
                                           self.page_size, **kwargs):
            self.metric_events_by_ticket = {}
            for event in page.get('metric_events', []) or []:
                self.metric_events_by_ticket.setdefault(event.get('ticket_id'), []).append(event)

            yield from page.get(self.item_key, [])

    def sync(self, state, parent_obj: Dict = None): #pylint: disable=too-many-statements

        bookmark = self.get_bookmark(state, self.name)

        tickets = self.get_objects()

        audits_stream = TicketAudits(self.client, self.config)
        metrics_stream = TicketMetrics(self.client, self.config)
        metric_events_stream = TicketMetricEvents(self.client, self.config)
        comments_stream = TicketComments(self.client, self.config)
        side_conversations_stream = SideConversations(self.client, self.config)

        if audits_stream.is_selected():
            LOGGER.info("Syncing ticket_audits per ticket...")

        if side_conversations_stream.is_selected():
            LOGGER.info("Syncing side_conversations_stream per ticket...")

        ticket_ids = []
        counter = 0
        start_time = time.time()
        for ticket in tickets:
            zendesk_metrics.capture('ticket')

            self.update_bookmark(state, self.name, ticket.get('updated_at'))

            ticket.pop('fields') # NB: Fields is a duplicate of custom_fields, remove before emitting
            # yielding stream name with record in a tuple as it is used for obtaining only the parent records while sync
            if self.is_selected():
                yield (self.stream, ticket)

            # Skip deleted tickets because they don't have audits or comments
            if ticket.get('status') == 'deleted':
                continue

            if metrics_stream.is_selected() and ticket.get('metric_set'):
                zendesk_metrics.capture('ticket_metric')
                metrics_stream.count+=1
                yield (metrics_stream.stream, ticket["metric_set"])

            if metric_events_stream.is_selected():
                for event in getattr(self, 'metric_events_by_ticket', {}).get(ticket["id"], []):
                    zendesk_metrics.capture('ticket_metric_event')
                    metric_events_stream.count += 1
                    yield (metric_events_stream.stream, event)

            if side_conversations_stream.is_selected():
                yield from side_conversations_stream.sync(state=state, parent_obj=ticket)

            # Check if the number of ticket IDs has reached the batch size.
            ticket_ids.append(ticket["id"])
            if len(ticket_ids) >= CONCURRENCY_LIMIT:
                # Process audits and comments in batches
                records = self.sync_ticket_audits_and_comments(
                    comments_stream, audits_stream, ticket_ids)
                for audits, comments in records:
                    for audit in audits:
                        yield audit
                    for comment in comments:
                        yield comment
                # Reset the list of ticket IDs after processing the batch.
                ticket_ids = []
                # Write state after processing the batch.
                singer.write_state(state)
                counter += CONCURRENCY_LIMIT

                # Check if the number of records processed in a minute has reached the limit.
                if counter >= AUDITS_REQUEST_PER_MINUTE:
                    # Calculate elapsed time
                    elapsed_time = time.time() - start_time

                    # Calculate remaining time until the next minute, plus buffer of 2 more seconds
                    remaining_time = max(0, 60 - elapsed_time + 2)

                    # Sleep for the calculated time
                    time.sleep(remaining_time)
                    start_time = time.time()
                    counter = 0

        # Check if there are any remaining ticket IDs after the loop.
        if ticket_ids:
            records = self.sync_ticket_audits_and_comments(comments_stream, audits_stream, ticket_ids)
            for audits, comments in records:
                for audit in audits:
                    yield audit
                for comment in comments:
                    yield comment

        self.emit_sub_stream_metrics(audits_stream)
        self.emit_sub_stream_metrics(metrics_stream)
        self.emit_sub_stream_metrics(metric_events_stream)
        self.emit_sub_stream_metrics(comments_stream)
        self.emit_sub_stream_metrics(side_conversations_stream)
        singer.write_state(state)

    def sync_ticket_audits_and_comments(self, comments_stream, audits_stream, ticket_ids):
        if comments_stream.is_selected() or audits_stream.is_selected():
            return asyncio.run(audits_stream.sync_in_bulk(ticket_ids, comments_stream))
        # Return empty list of audits and comments if not selected
        return [([], [])]
