from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class AuditLogs(PaginatedStream):
    name = "audit_logs"
    replication_method = "INCREMENTAL"
    replication_key = "created_at"
    key_properties = ["id"]
    endpoint = 'audit_logs'
    item_key = 'audit_logs'
    pagination_type = "offset"
