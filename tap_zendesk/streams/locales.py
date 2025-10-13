from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class Locales(PaginatedStream):
    name = "locales"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'locales'
    item_key = 'locales'
    pagination_type = "offset"
