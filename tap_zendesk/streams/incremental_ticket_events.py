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

    def get_objects(self, **kwargs): # pylint: disable=arguments-differ
        """
        The global `ticket_audits` cursor pagination is not guaranteed to be
        stable if audits are created while a long sync is in progress (the
        same audit can shift across the cursor boundary and be returned on
        more than one page). Deduplicate by id per sync, mirroring the
        `seen_ids` safeguard the old `incremental/ticket_events` endpoint's
        client relied on.
        """
        seen_ids = set()
        for record in super().get_objects(**kwargs):
            record_id = record.get('id')
            if record_id is not None and record_id in seen_ids:
                continue
            if record_id is not None:
                seen_ids.add(record_id)
            yield record

    def modify_object(self, record, **kwargs):
        record = dict(record)
        record['child_events'] = record.pop('events', None)
        record['updater_id'] = record.get('author_id')
        record['timestamp'] = record.get('created_at')
        return record
