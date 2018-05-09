from tap_zendesk.streams import STREAMS

def discover_streams(client):
    streams = []
    for s in STREAMS.values():
        s = s(client)
        streams.append({'stream': s.name, 'tap_stream_id': s.name, 'schema': s.load_schema(), 'metadata': s.load_metadata()})
    return streams
