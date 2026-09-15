from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class ResourceCollections(PaginatedStream):
    name = "resource_collections"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'resource_collections'
    item_key = 'resource_collections'
    pagination_type = "offset"
