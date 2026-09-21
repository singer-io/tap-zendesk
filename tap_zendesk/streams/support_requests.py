from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class SupportRequests(PaginatedStream):
    name = "support_requests"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'requests'
    item_key = 'requests'
    pagination_type = "cursor"
