from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class SideConversationsEvents(PaginatedStream):
    name = "side_conversations_events"
    replication_method = "INCREMENTAL"
    replication_key = "created_at"
    key_properties = ["id"]
    endpoint = 'tickets/side_conversations/events'
    item_key = 'events'
    pagination_type = "offset"
