from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class Sessions(PaginatedStream):
    name = "sessions"
    replication_method = "INCREMENTAL"
    replication_key = "last_seen_at"
    key_properties = ["id"]
    endpoint = 'sessions'
    item_key = 'sessions'
    pagination_type = "offset"
