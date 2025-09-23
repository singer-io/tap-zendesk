from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class Macros(PaginatedStream):
    name = "macros"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'macros'
    item_key = 'macros'
    pagination_type = "cursor"
