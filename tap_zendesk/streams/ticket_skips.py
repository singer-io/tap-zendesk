from tap_zendesk.streams.abstracts import (
    PaginatedStream
)
from tap_zendesk.streams.users import UserSubStreamMixin

class TicketSkips(UserSubStreamMixin, PaginatedStream):
    name = "ticket_skips"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'users/{user_id}/skips'
    item_key = 'skips'
    pagination_type = "cursor"
    parent = "users"
