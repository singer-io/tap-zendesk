from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class Automations(PaginatedStream):
    name = "automations"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'automations'
    item_key = 'automations'
    pagination_type = "cursor"
