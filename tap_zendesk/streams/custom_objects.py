from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class CustomObjects(PaginatedStream):
    name = "custom_objects"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["key"]
    endpoint = 'custom_objects'
    item_key = 'custom_objects'
    pagination_type = "offset"
