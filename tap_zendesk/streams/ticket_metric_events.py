from tap_zendesk import http
from tap_zendesk.streams.abstracts import PaginatedStream


class TicketMetricEvents(PaginatedStream):
    name = "ticket_metric_events"
    replication_method = "INCREMENTAL"
    replication_key = "time"
    endpoint = "incremental/ticket_metric_events"
    item_key = "ticket_metric_events"
    pagination_type = "cursor"

    def update_params(self, **kwargs):
        bookmark = self.get_bookmark(kwargs["state"], self.name)
        self.params = {"start_time": int(bookmark.timestamp()) - 1}

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # Use the same time-bounded endpoint as sync, independent of tickets.
        start_time = int(self.get_bookmark({}, self.name).timestamp()) - 1
        http.call_api(
            self.get_stream_endpoint(),
            self.request_timeout,
            params={"start_time": start_time, "page[size]": 1},
            headers=http.build_headers(self.config["access_token"]),
        )
