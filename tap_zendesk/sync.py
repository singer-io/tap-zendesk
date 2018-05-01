import json
import singer
import singer.metrics as metrics

from singer import metadata
from singer import Transformer
from zenpy.lib.api_objects import BaseObject
from zenpy.lib.proxy import ProxyList
from tap_zendesk.streams import STREAMS

LOGGER = singer.get_logger()

def process_record(record):
    """ Serializes Zenpy's internal classes into Python objects via ZendeskEncoder. """
    rec_str = json.dumps(record, cls=ZendeskEncoder)
    rec_dict = json.loads(rec_str)
    return rec_dict

def sync_stream(client, state, start_date, stream):
    instance = STREAMS[stream['tap_stream_id']](client)

    # If we have a bookmark, use it; otherwise use start_date
    if (instance.replication_method == 'INCREMENTAL' and
            not state.get('bookmarks', {}).get(stream['tap_stream_id'])):
        singer.write_bookmark(state,
                              stream['tap_stream_id'],
                              instance.replication_key,
                              start_date)

    with metrics.record_counter(stream['tap_stream_id']) as counter:
        for record in instance.sync(state):
            counter.increment()

            rec = process_record(record)
            # SCHEMA_GEN: Comment out transform
            with Transformer() as transformer:
                rec = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))

            singer.write_record(stream['tap_stream_id'], rec)
            # NB: We will only write state at the end of a stream's sync:
            #  We may find out that there exists a sync that takes too long and can never emit a bookmark
            #  but we don't know if we can guarentee the order of emitted records.

        if instance.replication_method == "INCREMENTAL":
            singer.write_state(state)

        return counter.value

class ZendeskEncoder(json.JSONEncoder):
    def default(self, obj): # pylint: disable=arguments-differ,method-hidden
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
