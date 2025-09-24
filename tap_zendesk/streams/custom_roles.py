from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class CustomRoles(PaginatedStream):
    name = "custom_roles"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'custom_roles'
    item_key = 'custom_roles'
    pagination_type = "offset"
