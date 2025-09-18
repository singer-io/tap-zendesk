import datetime
import singer
from singer import utils
from tap_zendesk.streams.abstracts import Stream
from tap_zendesk.exceptions import ZendeskNotFoundError


class TicketMetricEvents(Stream):
    name = "ticket_metric_events"
    replication_method = "INCREMENTAL"
    replication_key = "time"
    count = 0

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        start = bookmark - datetime.timedelta(seconds=1)

        epoch_start = int(utils.now().timestamp())
        parsed_start = singer.strftime(start, "%Y-%m-%dT%H:%M:%SZ")
        ticket_metric_events = self.client.tickets.metrics_incremental(start_time=epoch_start)
        for event in ticket_metric_events:
            self.count += 1
            if bookmark < utils.strptime_with_tz(event.time):
                self.update_bookmark(state, event.time)
            if parsed_start <= event.time:
                yield (self.stream, event)

    def check_access(self):
        try:
            epoch_start = int(utils.now().timestamp())
            self.client.tickets.metrics_incremental(start_time=epoch_start)
        except ZendeskNotFoundError:
            #Skip 404 ZendeskNotFoundError error as goal is just to check whether TicketComments have read permission or not
            pass
