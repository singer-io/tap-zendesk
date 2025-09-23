from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class Groups(PaginatedStream):
    name = "groups"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'groups'
    item_key = 'groups'
    pagination_type = "cursor"
