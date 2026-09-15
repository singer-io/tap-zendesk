from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class OrganizationSubscriptions(PaginatedStream):
    name = "organization_subscriptions"
    replication_method = "INCREMENTAL"
    replication_key = "created_at"
    key_properties = ["id"]
    endpoint = 'organization_subscriptions'
    item_key = 'organization_subscriptions'
    pagination_type = "cursor"
