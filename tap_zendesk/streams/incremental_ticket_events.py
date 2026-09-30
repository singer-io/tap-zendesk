from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class IncrementalTicketEvents(PaginatedStream):
    name = "incremental_ticket_events"
    replication_method = "INCREMENTAL"
    replication_key = "created_at"
    key_properties = ["id"]
    # `incremental/ticket_events` is deprecated for third-party apps
    # (https://support.zendesk.com/hc/en-us/articles/11249701485338); the
    # designated replacement is the global, cursor-paginated `ticket_audits`
    # list endpoint. An audit groups one or more child events together, so
    # each audit is remapped below into the shape previously produced by the
    # `ticket_events` endpoint (`child_events` <- `audit.events`, etc.).
    endpoint = 'ticket_audits'
    item_key = 'audits'
    pagination_type = 'cursor'

    def modify_object(self, record, **kwargs):
        record = dict(record)
        record['child_events'] = record.pop('events', None)
        record['updater_id'] = record.get('author_id')
        record['timestamp'] = record.get('created_at')
        return record
