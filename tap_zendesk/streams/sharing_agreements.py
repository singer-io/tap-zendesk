from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class SharingAgreements(PaginatedStream):
    name = "sharing_agreements"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'sharing_agreements'
    item_key = 'sharing_agreements'
    pagination_type = "offset"
