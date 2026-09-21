from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class AccountAttributes(PaginatedStream):
    name = "account_attributes"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'routing/attributes'
    item_key = 'attributes'
    pagination_type = "offset"
