from tap_zendesk.streams.abstracts import (
    CursorBasedStream
)


class TicketFields(CursorBasedStream):
    name = "ticket_fields"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'ticket_fields'
    item_key = 'ticket_fields'
