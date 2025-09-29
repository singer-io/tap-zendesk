import json
from typing import Dict
from zenpy.lib.api_objects import BaseObject
from zenpy.lib.proxy import ProxyList

import singer
from singer import (
    metrics,
    metadata,
    Transformer
)
from singer.transform import UNIX_SECONDS_INTEGER_DATETIME_PARSING

LOGGER = singer.get_logger()

def process_record(record):
    """ Serializes Zenpy's internal classes into Python objects via ZendeskEncoder. """
    rec_str = json.dumps(record, cls=ZendeskEncoder)
    rec_dict = json.loads(rec_str)
    return rec_dict

def update_currently_syncing(state: Dict, stream_name: str) -> None:
    """
    Update currently_syncing in state and write it
    """
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)

def sync_stream(state, start_date, instance):
    stream = instance.stream
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: {}".format(last_stream))

    # If we have a bookmark, use it; otherwise use start_date
    if (instance.replication_method == 'INCREMENTAL' and
            not state.get('bookmarks', {}).get(stream.tap_stream_id, {}).get(instance.replication_key)):
        singer.write_bookmark(state,
                              stream.tap_stream_id,
                              instance.replication_key,
                              start_date)

    parent_stream = stream
    update_currently_syncing(state, stream.tap_stream_id)
    with metrics.record_counter(stream.tap_stream_id) as counter, Transformer(
        integer_datetime_fmt=UNIX_SECONDS_INTEGER_DATETIME_PARSING
    ) as transformer:
        for (stream, record) in instance.sync(state):
            # NB: Only count parent records in the case of sub-streams
            if stream.tap_stream_id == parent_stream.tap_stream_id:
                counter.increment()

            rec = process_record(record)
            # SCHEMA_GEN: Comment out transform
            rec = transformer.transform(rec, stream.schema.to_dict(), metadata.to_map(stream.metadata))

            singer.write_record(stream.tap_stream_id, rec)
            # NB: We will only write state at the end of a stream's sync:
            #  We may find out that there exists a sync that takes too long and can never emit a bookmark
            #  but we don't know if we can guarentee the order of emitted records.

        if instance.replication_method == "INCREMENTAL":
            singer.write_state(state)

        update_currently_syncing(state, None)

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
