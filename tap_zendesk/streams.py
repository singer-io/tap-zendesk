import datetime
import os
import json

from singer import metadata
from singer import utils

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

class Organizations(Stream):
    name = "organizations"

    def sync(self, bookmark=None):
        bookmark = datetime.datetime.now() - datetime.timedelta(days=3)
        return self.client.organizations.incremental(start_time=bookmark)

class Users(Stream):
    name = "users"

    def sync(self, bookmark=None):
        bookmark = datetime.datetime.now() - datetime.timedelta(days=3)
        return self.client.users.incremental(start_time=bookmark)

class Tickets(Stream):
    name = "tickets"

    # def __init__(self, client, ticket_audits):
      # pass
      # Get state up here somewhere

    def sync(self, bookmark=None):
        bookmark = datetime.datetime.now() - datetime.timedelta(days=3)
        # Get first audit, and if it's not in state, add the cursor value for it to state
        return self.client.tickets.incremental(start_time=bookmark)

class TicketAudits(Stream):
    name = "ticket-audits"

    def sync(self, bookmark=None):
        # The bookmark value is not a datetime here
        # Ex: zenpy_client.tickets.audits(cursor='fDE1MTc2MjkwNTQuMHx8')
        # Max of 1000 (default)
        #bookmark = datetime.datetime.now() - datetime.timedelta(days=3)
        return self.client.tickets.audits(cursor=bookmark)

class Groups(Stream):
    name = "groups"

    def sync(self, bookmark=None):
        bookmark = datetime.datetime.now() - datetime.timedelta(days=3)
        groups = self.client.groups()
        for group in groups:
            if utils.strptime(group.updated_at) >= bookmark:
                yield group

class Macros(Stream):
    name = "macros"

    def sync(self, bookmark=None):
        bookmark = datetime.datetime.now() - datetime.timedelta(days=3)
        macros = self.client.macros()
        for macro in macros:
            if utils.strptime(macro.updated_at) >= bookmark:
                yield macro

class Tags(Stream):
    name = "tags"

    # NB: This stream is actually syncing FULL TABLE -> Need to properly set metadata
    def sync(self, bookmark=None):
        return self.client.tags()

class TicketFields(Stream):
    name = "ticket-fields"

    def sync(self, bookmark=None):
        bookmark = datetime.datetime.now() - datetime.timedelta(days=3)
        fields = self.client.ticket_fields()
        for field in fields:
            if utils.strptime(field.updated_at) >= bookmark:
                yield field

class TicketMetrics(Stream):
    name = "ticket-metrics"

    def sync(self, bookmark=None):
        bookmark = datetime.datetime.now() - datetime.timedelta(days=3)
        return self.client.tickets.metrics_incremental(start_time=bookmark)


class GroupMemberships(Stream):
    name = "group-memberships"

    # NB: This stream is actually syncing FULL TABLE -> Need to properly set metadata
    def sync(self, bookmark=None):
        return self.client.group_memberships()


STREAMS = {
    "tickets": Tickets,
    "groups": Groups,
    "users": Users,
    "organizations": Organizations,
    #    "ticket-audits": TicketAudits,
    "ticket-fields": TicketFields,
    "group-memberships": GroupMemberships,
    "macros": Macros,
    "tags": Tags,
    "ticket-metrics": TicketMetrics
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
