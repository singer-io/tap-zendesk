from tap_zendesk.streams import STREAMS

def discover_streams(one):
    streams = []
    for s in STREAMS.values():
        s = s()
        streams.append({'stream': s.name, 'tap_stream_id': s.name, 'schema': s.load_schema(), 'metadata': s.load_metadata()})
    return streams


class IncrementalStream():
    def sync(self, bookmark):
        self._get_records(bookmark)
    pass

class FullTableStream():
    pass







# * Users
# * Organizations
# * Tickets
# * Audits (P1)
# * Ticket Fields
# * Groups
# * Group Memberships
# * Macros
# * Tags
# * Ticket Metrics

# * Ticket Comments (P2)
# * Others from Audit (P2)

# Use Incremental endpoint
  # tickets
# Use "list" Endpoints; Drop records < bookmark / updated_at
  # groups
#
# Use "search" Endpoint; for things like ticket_metrics
#   * zenpy_client.search("some query", type='ticket_metrics', sort_by='created_at', sort_order='desc')


# "Relationships" (?)
#  tickets + ticket_fields = a ticket row
