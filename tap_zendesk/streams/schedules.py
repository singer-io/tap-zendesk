from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class Schedules(PaginatedStream):
    name = "schedules"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'business_hours/schedules'
    item_key = 'schedules'
    pagination_type = "offset"
    children = ['schedule_holidays']
