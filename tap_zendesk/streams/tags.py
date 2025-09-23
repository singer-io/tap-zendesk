from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class Tags(PaginatedStream):
    name = "tags"
    replication_method = "FULL_TABLE"
    key_properties = ["name"]
    endpoint = 'tags'
    item_key = 'tags'
    pagination_type = "cursor"
