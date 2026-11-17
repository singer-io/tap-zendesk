import asyncio
from itertools import chain

from aiohttp import ClientSession

from tap_zendesk import http
from tap_zendesk.exceptions import ZendeskNotFoundError
from tap_zendesk.streams.abstracts import Stream


def extract_metric_events(response):
    """
    Return the flat list of metric events from a Show Ticket response.

    Events are read from the top-level `metric_events` collection or, failing that, from
    the ticket itself. Either may be a flat list or a dict of lists keyed by metric name.
    """
    raw = response.get('metric_events')
    if raw is None:
        raw = (response.get('ticket') or {}).get('metric_events')

    if isinstance(raw, dict):
        return list(chain.from_iterable(raw.values()))
    if isinstance(raw, list):
        return raw
    return []


class TicketMetricEvents(Stream):
    name = "ticket_metric_events"
    replication_method = "INCREMENTAL"
    replication_key = "time"
    count = 0
    parent = 'tickets'
    # Zendesk documents the `metric_events` sideload for a single ticket only
    endpoint = 'tickets/{ticket_id}'

    async def get_objects(self, session, ticket_id):
        url = self.get_stream_endpoint(ticket_id=ticket_id)
        headers = http.build_headers(http.get_credential(self.config))
        try:
            response = await http.call_api_async(
                session, url, self.request_timeout, params={'include': 'metric_events'}, headers=headers)
        except ZendeskNotFoundError:
            # The ticket was deleted or archived after it was listed
            return []

        events = extract_metric_events(response)
        for event in events:
            event.setdefault('ticket_id', ticket_id)
        return events

    async def sync_in_bulk(self, ticket_ids):
        """
        Concurrently fetch the metric events of several tickets, in the order of `ticket_ids`.
        """
        async with ClientSession() as session:
            return await asyncio.gather(*(self.get_objects(session, ticket_id) for ticket_id in ticket_ids))

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # Metric events are read with the same permission as tickets, so tickets covers the check
        return
