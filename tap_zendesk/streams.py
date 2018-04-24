import datetime
import os
import json

from singer import metadata
from singer import utils

KEY_PROPERTIES = ['id']

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

class Stream():
    key_properties = KEY_PROPERTIES

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

        mdata = metadata.write(mdata, (), 'table-key-properties', self.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method', self.replication_method)

        for field_name, props in schema['properties'].items():
            if field_name in KEY_PROPERTIES:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)

class Organizations(Stream):
    name = "organizations"
    replication_method = "INCREMENTAL"

    def sync(self, bookmark=None):
        return self.client.organizations.incremental(start_time=bookmark)

class Users(Stream):
    name = "users"
    replication_method = "INCREMENTAL"

    def sync(self, bookmark=None):
        return self.client.users.incremental(start_time=bookmark)

class Tickets(Stream):
    name = "tickets"
    replication_method = "INCREMENTAL"

    # def __init__(self, client, ticket_audits):
      # pass
      # Get state up here somewhere

    def sync(self, bookmark=None):
        # Get first audit, and if it's not in state, add the cursor value for it to state
        return self.client.tickets.incremental(start_time=bookmark)

class TicketAudits(Stream):
    name = "ticket-audits"
    replication_method = "INCREMENTAL"

    def sync(self, bookmark=None):
        # The bookmark value is not a datetime here
        # Ex: zenpy_client.tickets.audits(cursor='fDE1MTc2MjkwNTQuMHx8')
        # Max of 1000 (default)
        #bookmark = datetime.datetime.now() - datetime.timedelta(days=3)
        return self.client.tickets.audits(cursor=bookmark)

class Groups(Stream):
    name = "groups"
    replication_method = "INCREMENTAL"

    def sync(self, bookmark=None):
        groups = self.client.groups()
        for group in groups:
            if utils.strptime_with_tz(group.updated_at) >= bookmark:
                yield group

class Macros(Stream):
    name = "macros"
    replication_method = "INCREMENTAL"

    def sync(self, bookmark=None):
        macros = self.client.macros()
        for macro in macros:
            if utils.strptime_with_tz(macro.updated_at) >= bookmark:
                yield macro

class Tags(Stream):
    name = "tags"
    replication_method = "FULL_TABLE"
    key_properties = []

    def sync(self, bookmark=None):
        return self.client.tags()

class TicketFields(Stream):
    name = "ticket-fields"
    replication_method = "INCREMENTAL"

    def sync(self, bookmark=None):
        fields = self.client.ticket_fields()
        for field in fields:
            if utils.strptime_with_tz(field.updated_at) >= bookmark:
                yield field

class TicketMetrics(Stream):
    name = "ticket-metrics"
    replication_method = "INCREMENTAL"

    def sync(self, bookmark=None):
        return self.client.tickets.metrics_incremental(start_time=bookmark)


class GroupMemberships(Stream):
    name = "group-memberships"
    replication_method = "INCREMENTAL"

    def sync(self, bookmark=None):
        memberships = self.client.group_memberships()
        for membership in memberships:
            if utils.strptime_with_tz(membership.updated_at) >= bookmark:
                yield membership


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
