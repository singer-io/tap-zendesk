import json
from zenpy.lib.api_objects import BaseObject
from zenpy.lib.proxy import ProxyList

import singer
from singer import metrics
from singer import metadata
from singer import Transformer

LOGGER = singer.get_logger()

def process_record(record):
    """ Serializes Zenpy's internal classes into Python objects via ZendeskEncoder. """
    rec_str = json.dumps(record, cls=ZendeskEncoder)
    rec_dict = json.loads(rec_str)
    return rec_dict

def get_user_fields(instance):
    user_fields_api_object = instance.client.user_fields
    api_response = user_fields_api_object._call_api(
        # We need this session.get object because it will get
        # converted to 'GET' by `_call_api`
        user_fields_api_object.session.get,

        # `_build_url()` is how zenpy constructs the url for other
        # streams so we follow that pattern here
        user_fields_api_object._build_url(
            endpoint=user_fields_api_object.endpoint()
        )
    )

    api_response.raise_for_status()

    return api_response.json()

def merge_user_fields_into_users(rec, user_fields):
    for key, field_metadata in user_fields.items():
        rec_value = rec['user_fields'].get(key)
        rec['user_fields'][key] = field_metadata
        rec['user_fields'][key]['value'] = rec_value
    return rec

def sync_stream(state, start_date, instance):
    stream = instance.stream

    # If we have a bookmark, use it; otherwise use start_date
    if (instance.replication_method == 'INCREMENTAL' and
            not state.get('bookmarks', {}).get(stream.tap_stream_id, {}).get(instance.replication_key)):
        singer.write_bookmark(state,
                              stream.tap_stream_id,
                              instance.replication_key,
                              start_date)

    parent_stream = stream
    with metrics.record_counter(stream.tap_stream_id) as counter, Transformer() as transformer:

        if stream.tap_stream_id == 'users':
            user_fields = {}
            resp = get_user_fields(instance)
            for field in resp['user_fields']:
                user_fields[field['key']] = field

            while resp.get('next'):
                resp = get_user_fields(instance)
                for field in resp['user_fields']:
                    user_fields[field['key']] = field

        for (stream, record) in instance.sync(state):
            # NB: Only count parent records in the case of sub-streams
            if stream.tap_stream_id == parent_stream.tap_stream_id:
                counter.increment()

            rec = process_record(record)
            # SCHEMA_GEN: Comment out transform
            rec = transformer.transform(rec, stream.schema.to_dict(), metadata.to_map(stream.metadata))

            if stream.tap_stream_id == 'users':
                rec = merge_user_fields_into_users(rec, user_fields)

            singer.write_record(stream.tap_stream_id, rec)
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
