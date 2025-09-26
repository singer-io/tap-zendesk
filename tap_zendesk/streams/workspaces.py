from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class Workspaces(PaginatedStream):
    name = "workspaces"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'workspaces'
    item_key = 'workspaces'
    pagination_type = "offset"
