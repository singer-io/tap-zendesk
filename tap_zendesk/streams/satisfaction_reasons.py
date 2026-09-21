from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class SatisfactionReasons(PaginatedStream):
    name = "satisfaction_reasons"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'satisfaction_reasons'
    item_key = 'reasons'
    pagination_type = "offset"
