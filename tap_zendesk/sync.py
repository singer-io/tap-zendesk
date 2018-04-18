import json
import singer
import singer.metrics as metrics

from singer import metadata
from tap_zendesk.streams import STREAMS
from singer import Transformer
from zenpy.lib.api_objects import BaseObject

LOGGER = singer.get_logger()

def process_record(record, mdata):
    rec_str = json.dumps(record, cls=ZendeskEncoder)
    rec_dict =json.loads(rec_str)

    for field_name in list(rec_dict.keys()):
        if not metadata.get(mdata, ('properties', field_name), 'selected'):
            rec_dict.pop(field_name)

    return rec_dict

def sync_stream(client, state, stream):
    instance = STREAMS[stream['tap_stream_id']](client)
    with metrics.record_counter(stream["tap_stream_id"]) as counter:
        for record in instance.sync():
            counter.increment()

            rec = process_record(record, metadata.to_map(stream['metadata']))
            with Transformer() as transformer:
                rec = transformer.transform(rec, stream['schema'])
            singer.write_record(stream["tap_stream_id"], rec)

        return counter.value

class ZendeskEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, BaseObject):
            return obj.to_dict()
        return json.JSONEncoder.default(self, obj)
