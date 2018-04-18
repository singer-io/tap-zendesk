import datetime
import os
import json

from singer import metadata

KEY_PROPERTIES = ['id']

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

class Tickets():

    name = "tickets"

    def __init__(self, client):
        self.client = client

    @staticmethod
    def load_schema():
        schema_file = "schemas/tickets.json"

        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)

        return schema

    @staticmethod
    def load_metadata():
        schema = Tickets.load_schema()
        mdata = metadata.new()

        mdata = metadata.write(mdata, (), 'table-key-properties', KEY_PROPERTIES)
        mdata = metadata.write(mdata, (), 'forced-replication-method', 'INCREMENTAL')

        for field_name, props in schema['properties'].items():
            if field_name in KEY_PROPERTIES:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)

    def sync(self, bookmark=None):
        bookmark = datetime.datetime.now() - datetime.timedelta(days=3)
        return self.client.tickets.incremental(start_time=bookmark)

STREAMS = [
    Tickets,
]


    # stream = {
    #     "tap_stream_id": stream_name,
    #     "stream": stream_name,
    #     "key_properties": ["Id"],
    #     "schema": {
    #         "type": "object",
    #         "additionalProperties": False,
    #         "properties": properties,
    #     },
    #     'metadata': metadata.to_list(mdata)
    # }
