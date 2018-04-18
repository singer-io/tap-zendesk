import datetime
import os
import json

from singer import metadata

KEY_PROPERTIES = ['id']

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

class Stream():
    def __init__(self, client=None):
        self.client = client

    def load_schema(self):
        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)
        return schema

    def load_metadata(self):
        schema = self.load_schema()
        mdata = metadata.new()

        mdata = metadata.write(mdata, (), 'table-key-properties', KEY_PROPERTIES)
        mdata = metadata.write(mdata, (), 'forced-replication-method', 'INCREMENTAL')

        for field_name, props in schema['properties'].items():
            if field_name in KEY_PROPERTIES:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)


class Tickets(Stream):
    name = "tickets"

    def sync(self, bookmark=None):
        bookmark = datetime.datetime.now() - datetime.timedelta(days=3)
        return self.client.tickets.incremental(start_time=bookmark)

STREAMS = {
    "tickets": Tickets,
}


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
