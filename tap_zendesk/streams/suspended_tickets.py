from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class SuspendedTickets(PaginatedStream):
    name = "suspended_tickets"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'suspended_tickets'
    item_key = 'suspended_tickets'
    pagination_type = "offset"
