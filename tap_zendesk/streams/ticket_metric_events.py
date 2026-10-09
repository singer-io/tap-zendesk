from tap_zendesk.streams.abstracts import Stream


class TicketMetricEvents(Stream):
    name = "ticket_metric_events"
    replication_method = "INCREMENTAL"
    replication_key = "time"
    count = 0
    parent = 'tickets'

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # We load metric events as a side load of tickets, so we don't need to check access
        return
