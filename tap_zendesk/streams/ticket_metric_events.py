from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class TicketMetricEvents(PaginatedStream):
    name = "ticket_metric_events"
    replication_method = "INCREMENTAL"
    replication_key = "time"
    count = 0
    pagination_type = "cursor"
    parent = 'tickets'

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # We load metric events as a side load of tickets (`include=metric_events`),
        # so we don't need to check access independently.
        return
