from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class TicketMetrics(PaginatedStream):
    name = "ticket_metrics"
    replication_method = "INCREMENTAL"
    count = 0
    pagination_type = "cursor"

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # We load metrics as side load of tickets, so we don't need to check access
        return
