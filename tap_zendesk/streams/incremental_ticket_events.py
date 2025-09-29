from datetime import datetime
from tap_zendesk import http
from tap_zendesk.streams.abstracts import (
    HEADERS,
    START_DATE_FORMAT,
    CursorBasedExportStream
)

class IncrementalTicketEvents(CursorBasedExportStream):
    name = "incremental_ticket_events"
    replication_method = "INCREMENTAL"
    replication_key = "created_at"
    key_properties = ["id"]
    endpoint = 'incremental/ticket_events'
    item_key = 'ticket_events'

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        url = self.get_stream_endpoint()
        start_time = datetime.strptime(self.config['start_date'], START_DATE_FORMAT).timestamp()
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config["access_token"])

        http.call_api(url, self.request_timeout, params={'start_time': start_time, 'per_page': 1}, headers=HEADERS)
