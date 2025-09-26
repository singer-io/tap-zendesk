from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class Requests(PaginatedStream):
    name = "requests"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'requests'
    item_key = 'requests'
    pagination_type = "offset"
