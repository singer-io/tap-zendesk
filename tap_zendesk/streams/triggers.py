from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class Triggers(PaginatedStream):
    name = "triggers"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'triggers'
    item_key = 'triggers'
    pagination_type = "cursor"
