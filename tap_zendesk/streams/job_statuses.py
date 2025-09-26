from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class JobStatuses(PaginatedStream):
    name = "job_statuses"
    replication_method = "FULL_TABLE"
    key_properties = ["id"]
    endpoint = 'job_statuses'
    item_key = 'job_statuses'
    pagination_type = "cursor"
