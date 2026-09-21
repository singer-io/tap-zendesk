from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class TicketSkips(PaginatedStream):
    name = "ticket_skips"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'skips'
    item_key = 'skips'
    pagination_type = "cursor"
