from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class DeletedTickets(PaginatedStream):
    name = "deleted_tickets"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'deleted_tickets'
    item_key = 'deleted_tickets'
    pagination_type = "offset"
