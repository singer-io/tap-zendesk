from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class TicketFields(PaginatedStream):
    name = "ticket_fields"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'ticket_fields'
    item_key = 'ticket_fields'
    pagination_type = "cursor"
