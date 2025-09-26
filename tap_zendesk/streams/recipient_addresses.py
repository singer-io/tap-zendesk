from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class RecipientAddresses(PaginatedStream):
    name = "recipient_addresses"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'recipient_addresses'
    item_key = 'recipient_addresses'
    pagination_type = "offset"
