from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class Activities(PaginatedStream):
    name = "activities"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'activities'
    item_key = 'activities'
    pagination_type = "offset"
