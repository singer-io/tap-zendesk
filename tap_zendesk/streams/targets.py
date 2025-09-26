from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class Targets(PaginatedStream):
    name = "targets"
    replication_method = "FULL_TABLE"
    key_properties = ["id"]
    endpoint = 'targets'
    item_key = 'targets'
    pagination_type = "offset"
