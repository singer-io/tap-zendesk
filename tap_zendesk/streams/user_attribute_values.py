from tap_zendesk.streams.abstracts import (
    PaginatedStream,
    ChildBookmarkMixin
)
from tap_zendesk.streams.users import UserSubStreamMixin

class UserAttributeValues(UserSubStreamMixin, ChildBookmarkMixin, PaginatedStream):
    name = "user_attribute_values"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'routing/agents/{user_id}/instance_values'
    item_key = 'attribute_values'
    pagination_type = "offset"
    parent = "users"
    bookmark_value = None

    def modify_object(self, record, **_kwargs):
        """
        Overriding modify_record to add `parent's id` key in records
        """
        parent_obj = _kwargs.get("parent_record", {})
        user_id = parent_obj.get("id")
        record['user_id'] = user_id
        return record
