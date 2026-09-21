from datetime import datetime
from tap_zendesk import http
from tap_zendesk.streams.abstracts import (
    HEADERS,
    START_DATE_FORMAT,
    DEFAULT_PAGE_SIZE,
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

    def get_objects(self, start_time, side_load=None):
        '''
        Retrieve objects from the incremental exports endpoint using cursor based pagination
        '''
        url = self.get_stream_endpoint()
        seen_ids = set()

        for page in http.get_incremental_export_offset(
            url,
            self.config['access_token'],
            self.request_timeout,
            DEFAULT_PAGE_SIZE,
            start_time
        ):
            for obj in page[self.item_key]:
                obj_id = obj.get('id')
                if obj_id is not None and obj_id not in seen_ids:
                    seen_ids.add(obj_id)
                    yield obj
