import asyncio
import time
from datetime import datetime, timezone
from typing import Dict

import pytz
import singer
from singer import utils

from tap_zendesk import http
from tap_zendesk import metrics as zendesk_metrics
from tap_zendesk.streams.abstracts import (AUDITS_REQUEST_PER_MINUTE,
                                           CONCURRENCY_LIMIT, LOGGER,
                                           START_DATE_FORMAT, PaginatedStream)
from tap_zendesk.streams.side_conversations import SideConversations
from tap_zendesk.streams.ticket_audits import TicketAudits
from tap_zendesk.streams.ticket_comments import TicketComments
from tap_zendesk.streams.ticket_metric_events import TicketMetricEvents
from tap_zendesk.streams.ticket_metrics import TicketMetrics


class Tickets(PaginatedStream):
    name = "tickets"
    replication_method = "INCREMENTAL"
    replication_key = "generated_timestamp"
    item_key = "tickets"
    endpoint = "tickets"
    pagination_type = "cursor"
    children = ['ticket_audits', 'ticket_metrics', 'ticket_metric_events', 'ticket_comments', 'side_conversations']

    def get_objects(self, **kwargs):
        """
        Yield tickets from List Tickets. `include=metric_sets` side-loads metric sets either as
        a top-level collection keyed by `ticket_id` or nested in each ticket as `metric_set`;
        both are normalised to the nested form read by `sync`.
        """
        pages = http.get_cursor_based(
            self.get_stream_endpoint(), http.get_credential(self.config),
            self.request_timeout, self.page_size, **kwargs)

        for page in pages:
            metric_sets = {metric_set.get('ticket_id'): metric_set
                           for metric_set in page.get('metric_sets') or []}
            for ticket in self.get_nested_value(page, self.item_key, []) or []:
                if not ticket.get('metric_set') and ticket.get('id') in metric_sets:
                    ticket['metric_set'] = metric_sets[ticket['id']]
                yield ticket

    def sync(self, state, parent_obj: Dict = None): #pylint: disable=too-many-statements,too-many-nested-blocks

        bookmark = self.get_bookmark(state, self.name)
        max_generated_timestamp = bookmark

        audits_stream = TicketAudits(self.client, self.config)
        metrics_stream = TicketMetrics(self.client, self.config)
        metric_events_stream = TicketMetricEvents(self.client, self.config)
        comments_stream = TicketComments(self.client, self.config)
        side_conversations_stream = SideConversations(self.client, self.config)

        # Metric events keep their own `time` bookmark, which can differ from the
        # tickets bookmark (e.g. a previous sync advanced one but failed before the other).
        # An event is not expected after its ticket's generated_timestamp, so walking tickets
        # from the earlier of the two bookmarks cannot skip events in that gap.
        events_selected = metric_events_stream.is_selected()
        events_bookmark = metric_events_stream.get_bookmark(state, metric_events_stream.name) if events_selected else bookmark
        max_event_time = events_bookmark
        traversal_floor = min(bookmark, events_bookmark)

        # List Tickets does not include archived tickets, so neither they nor their
        # children are synced. Records are ordered by updated_at, which can differ from the
        # generated_timestamp bookmark, so the order is never relied upon: both bookmarks are
        # committed only after every page has been walked, never part way through.
        # Metric events are not requested here, as Zendesk documents that sideload for a single
        # ticket only; they are fetched per ticket instead.
        # https://developer.zendesk.com/documentation/ticketing/using-the-zendesk-api/side_loading/#supported-endpoints
        tickets = self.get_objects(params={
            'sort': 'updated_at',
            'include': 'metric_sets'
        })

        if audits_stream.is_selected():
            LOGGER.info("Syncing ticket_audits per ticket...")

        if side_conversations_stream.is_selected():
            LOGGER.info("Syncing side_conversations_stream per ticket...")

        ticket_ids = []
        event_ticket_ids = []
        counter = 0
        start_time = time.time()

        def throttle():
            # Check if the number of records processed in a minute has reached the limit.
            nonlocal counter, start_time
            if counter >= AUDITS_REQUEST_PER_MINUTE:
                # Calculate elapsed time
                elapsed_time = time.time() - start_time

                # Calculate remaining time until the next minute, plus buffer of 2 more seconds
                remaining_time = max(0, 60 - elapsed_time + 2)

                # Sleep for the calculated time
                time.sleep(remaining_time)
                start_time = time.time()
                counter = 0

        def new_metric_events(ids):
            nonlocal max_event_time
            for events in self.sync_ticket_metric_events(metric_events_stream, ids):
                for event in events:
                    event_time = metric_events_stream.get_replication_datetime(event)
                    if event_time < events_bookmark:
                        continue
                    max_event_time = max(max_event_time, event_time)
                    zendesk_metrics.capture('ticket_metric_event')
                    metric_events_stream.count += 1
                    yield (metric_events_stream.stream, event)

        for ticket in tickets:
            zendesk_metrics.capture('ticket')

            generated_timestamp_dt = datetime.fromtimestamp(ticket.get('generated_timestamp'), tz=timezone.utc).replace(tzinfo=pytz.UTC)

            # The API doesn't filter by start_time, so filter client side.
            if generated_timestamp_dt < traversal_floor:
                continue

            # Tickets older than the tickets bookmark are only walked to reach events
            # that the event bookmark has not yet covered.
            sync_ticket = generated_timestamp_dt >= bookmark

            if sync_ticket:
                max_generated_timestamp = max(max_generated_timestamp, generated_timestamp_dt)

                ticket.pop('fields', None) # NB: Fields is a duplicate of custom_fields, remove before emitting. If not present, default to None
                # yielding stream name with record in a tuple as it is used for obtaining only the parent records while sync
                if self.is_selected():
                    yield (self.stream, ticket)

            # Skip deleted tickets because they don't have audits or comments
            if ticket.get('status') == 'deleted':
                continue

            if sync_ticket and metrics_stream.is_selected() and ticket.get('metric_set'):
                zendesk_metrics.capture('ticket_metric')
                metrics_stream.count+=1
                yield (metrics_stream.stream, ticket["metric_set"])

            if events_selected and generated_timestamp_dt >= events_bookmark:
                event_ticket_ids.append(ticket["id"])
                if len(event_ticket_ids) >= CONCURRENCY_LIMIT:
                    yield from new_metric_events(event_ticket_ids)
                    event_ticket_ids = []
                    counter += CONCURRENCY_LIMIT
                    throttle()

            if not sync_ticket:
                continue

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
                throttle()

        # Check if there are any remaining ticket IDs after the loop.
        if ticket_ids:
            records = self.sync_ticket_audits_and_comments(comments_stream, audits_stream, ticket_ids)
            for audits, comments in records:
                for audit in audits:
                    yield audit
                for comment in comments:
                    yield comment

        if event_ticket_ids:
            yield from new_metric_events(event_ticket_ids)

        # Commit the bookmarks only once everything was fetched. Neither is ordered like the
        # cursor, so advancing mid-run could skip older records if the sync is interrupted.
        self.update_bookmark(state, self.name, utils.strftime(max_generated_timestamp))
        if events_selected:
            self.update_bookmark(state, metric_events_stream.name,
                                 utils.strftime(max_event_time), key=metric_events_stream.replication_key)

        self.emit_sub_stream_metrics(audits_stream)
        self.emit_sub_stream_metrics(metrics_stream)
        self.emit_sub_stream_metrics(metric_events_stream)
        self.emit_sub_stream_metrics(comments_stream)
        self.emit_sub_stream_metrics(side_conversations_stream)
        singer.write_state(state)

    def sync_ticket_metric_events(self, metric_events_stream, ticket_ids):
        return asyncio.run(metric_events_stream.sync_in_bulk(ticket_ids))

    def sync_ticket_audits_and_comments(self, comments_stream, audits_stream, ticket_ids):
        if comments_stream.is_selected() or audits_stream.is_selected():
            return asyncio.run(audits_stream.sync_in_bulk(ticket_ids, comments_stream))
        # Return empty list of audits and comments if not selected
        return [([], [])]

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        url = self.get_stream_endpoint()
        # Convert start_date parameter to timestamp to pass with request param
        start_time = datetime.strptime(self.config['start_date'], START_DATE_FORMAT).timestamp()

        http.call_api(url, self.request_timeout, params={'start_time': start_time, 'per_page': 1},
                      headers=http.build_headers(http.get_credential(self.config)))
