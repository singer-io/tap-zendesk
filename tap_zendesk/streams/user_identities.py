from tap_zendesk.streams.abstracts import (
    PaginatedStream, ChildBookmarkMixin
)
from tap_zendesk.streams.users import UserSubStreamMixin

class UserIdentities(UserSubStreamMixin, ChildBookmarkMixin, PaginatedStream):
    name = "user_identities"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'users/{user_id}/identities'
    item_key = 'identities'
    pagination_type = "cursor"
    parent = "users"
    bookmark_value = None
