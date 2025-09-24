from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class Brands(PaginatedStream):
    name = "brands"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'brands'
    item_key = 'brands'
    pagination_type = "offset"
