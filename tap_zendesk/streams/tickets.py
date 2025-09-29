from datetime import datetime, timezone
import time
import asyncio
from typing import Dict
import pytz
import singer
from singer import utils
from singer.metrics import Point
from tap_zendesk import http
from tap_zendesk import metrics as zendesk_metrics
from tap_zendesk.streams.abstracts import (
    CursorBasedExportStream,
    AUDITS_REQUEST_PER_MINUTE,
    HEADERS,
    CONCURRENCY_LIMIT,
    LOGGER,
    START_DATE_FORMAT
)
from tap_zendesk.streams.ticket_audits import TicketAudits
from tap_zendesk.streams.ticket_metrics import TicketMetrics
from tap_zendesk.streams.ticket_comments import TicketComments
from tap_zendesk.streams.side_conversations import SideConversations


class Tickets(CursorBasedExportStream):
    name = "tickets"
    replication_method = "INCREMENTAL"
    replication_key = "generated_timestamp"
    item_key = "tickets"
    endpoint = "incremental/tickets/cursor.json"

    def sync(self, state, parent_obj: Dict = None): #pylint: disable=too-many-statements

        bookmark = self.get_bookmark(state)

        # Fetch tickets with side loaded metrics
        # https://developer.zendesk.com/documentation/ticketing/using-the-zendesk-api/side_loading/#supported-endpoints
        tickets = self.get_objects(bookmark, side_load='metric_sets')

        audits_stream = TicketAudits(self.client, self.config)
        metrics_stream = TicketMetrics(self.client, self.config)
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

            generated_timestamp_dt = datetime.fromtimestamp(ticket.get('generated_timestamp'), tz=timezone.utc).replace(tzinfo=pytz.UTC)

            self.update_bookmark(state, utils.strftime(generated_timestamp_dt))

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
        self.emit_sub_stream_metrics(comments_stream)
        self.emit_sub_stream_metrics(side_conversations_stream)
        singer.write_state(state)

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
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config["access_token"])

        http.call_api(url, self.request_timeout, params={'start_time': start_time, 'per_page': 1}, headers=HEADERS)
