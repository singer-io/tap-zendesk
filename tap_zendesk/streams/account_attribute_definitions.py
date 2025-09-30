from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class AccountAttributeDefinitions(PaginatedStream):
    name = "account_attribute_definitions"
    replication_method = "FULL_TABLE"
    key_properties = []
    endpoint = 'routing/attributes/definitions'
    item_key = 'definitions'
    pagination_type = "offset"
