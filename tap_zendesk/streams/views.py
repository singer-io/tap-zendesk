from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class Views(PaginatedStream):
    name = "views"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'views'
    item_key = 'views'
    pagination_type = "cursor"
