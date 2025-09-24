from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class TargetFailures(PaginatedStream):
    name = "target_failures"
    replication_method = "INCREMENTAL"
    replication_key = "created_at"
    key_properties = ["id"]
    endpoint = 'target_failures'
    item_key = 'target_failures'
    pagination_type = "offset"
