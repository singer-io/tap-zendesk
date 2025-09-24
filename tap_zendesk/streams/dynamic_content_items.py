from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class DynamicContentItems(PaginatedStream):
    name = "dynamic_content_items"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'dynamic_content/items'
    item_key = 'items'
    pagination_type = "offset"
