import json
import singer
import singer.metrics as metrics

from tap_zendesk.streams import Tickets
from singer import Transformer
from zenpy.lib.api_objects import BaseObject

def sync_stream(client, state, stream):
    with metrics.record_counter(stream["tap_stream_id"]) as counter:
        if stream['stream'] == "tickets":
            tickets = Tickets(client)

        for ticket in tickets.sync():
            counter.increment()
            rec = json.dumps(ticket, cls=ZendeskEncoder)
            rec = json.loads(rec)
            with Transformer() as transformer:
                rec = transformer.transform(rec, stream['schema'])
            singer.write_record(stream["tap_stream_id"], rec)
        # write recrods
        return counter.value

class ZendeskEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, BaseObject):
            return obj.to_dict()
        return json.JSONEncoder.default(self, obj)
