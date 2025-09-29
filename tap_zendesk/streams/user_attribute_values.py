from tap_zendesk.streams.abstracts import (
    PaginatedStream
)
from tap_zendesk.streams.users import UserSubStreamMixin

class UserAttributeValues(UserSubStreamMixin, PaginatedStream):
    name = "user_attribute_values"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'routing/agents/{user_id}/instance_values'
    item_key = 'attribute_values'
    pagination_type = "offset"
    parent = "users"
