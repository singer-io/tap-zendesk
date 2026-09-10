from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class TriggerCategories(PaginatedStream):
    name = "trigger_categories"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'trigger_categories'
    item_key = 'trigger_categories'
    pagination_type = "cursor"
