from tap_zendesk.streams import STREAMS

def discover_streams():
    streams = []
    for s in STREAMS.values():
        s = s()
        streams.append({'stream': s.name, 'tap_stream_id': s.name, 'schema': s.load_schema(), 'metadata': s.load_metadata()})
    return streams

# * Audits (P1)

# * Ticket Comments (P2)
# * Others from Audit (P2)

# P2?
# Use "search" Endpoint; for things like ticket_metrics
#   * zenpy_client.search("some query", type='ticket_metrics', sort_by='created_at', sort_order='desc')


# P2?
# "Relationships" (?)
#  tickets + ticket_fields = a ticket row
