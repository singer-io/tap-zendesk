from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class OrganizationMemberships(PaginatedStream):
    name = "organization_memberships"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'organization_memberships'
    item_key = 'organization_memberships'
    pagination_type = "cursor"
