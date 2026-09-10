from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class DeletedUsers(PaginatedStream):
    name = "deleted_users"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'deleted_users'
    item_key = 'deleted_users'
    pagination_type = "cursor"
