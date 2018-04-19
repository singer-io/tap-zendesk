import json
import singer
import singer.metrics as metrics

from singer import metadata
from tap_zendesk.streams import STREAMS
from singer import Transformer
from zenpy.lib.api_objects import BaseObject
from zenpy.lib.proxy import ProxyList
LOGGER = singer.get_logger()

def process_record(record, mdata):
    rec_str = json.dumps(record, cls=ZendeskEncoder)
    rec_dict =json.loads(rec_str)

    # SCHEMA_GEN: Uncomment this line
    #return rec_dict

    for field_name in list(rec_dict.keys()):
        selected = metadata.get(mdata, ('properties', field_name), 'selected')
        inclusion = metadata.get(mdata, ('properties', field_name), 'inclusion')
        if not selected and inclusion != 'automatic':
            rec_dict.pop(field_name)

    return rec_dict

def sync_stream(client, state, stream):
    # we do this before hand.
    instance = STREAMS[stream['tap_stream_id']](client)
    with metrics.record_counter(stream["tap_stream_id"]) as counter:
        for record in instance.sync():
            counter.increment()

            rec = process_record(record, metadata.to_map(stream['metadata']))
            # SCHEMA_GEN: Comment out transform
            with Transformer() as transformer:
                rec = transformer.transform(rec, stream['schema'])
            singer.write_record(stream["tap_stream_id"], rec)

        return counter.value

class ZendeskEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, BaseObject):
            obj_dict = obj.to_dict()
            for k, v in list(obj_dict.items()):
                # NB: This might fail if the object inside is callable
                if callable(v):
                    obj_dict.pop(k)
            return obj_dict
        elif isinstance(obj, ProxyList):
            return obj.copy()
        return json.JSONEncoder.default(self, obj)
