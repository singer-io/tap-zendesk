from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class Bookmarks(PaginatedStream):
    name = "bookmarks"
    replication_method = "INCREMENTAL"
    replication_key = "created_at"
    key_properties = ["id"]
    endpoint = 'bookmarks'
    item_key = 'bookmarks'
    pagination_type = "offset"
