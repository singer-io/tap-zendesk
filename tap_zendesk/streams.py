import os
import json
import singer

from singer import metadata
from singer import utils
from singer.metrics import Point

LOGGER = singer.get_logger()
KEY_PROPERTIES = ['id']

CUSTOM_TYPES = {
    'text': 'string',
    'textarea': 'string',
    'date': 'string',
    'regexp': 'string',
    'dropdown': 'string',
    'integer': 'integer',
    'decimal': 'number',
    'checkbox': 'boolean',
}

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def process_custom_field(field):
    """ Take a custom field description and return a schema for it. """
    zendesk_type = field.type
    json_type = CUSTOM_TYPES.get(zendesk_type)
    if json_type is None:
        raise Exception("Discovered unsupported type for custom field {} (key: {}): {}"
                        .format(field.title,
                                field.key,
                                zendesk_type))
    field_schema = {'type': [
        json_type,
        'null'
    ]}

    if zendesk_type == 'date':
        field_schema['format'] = 'datetime'
    if zendesk_type == 'dropdown':
        field_schema['enum'] = [o['value'] for o in field.custom_field_options]

    return field_schema

class Stream():
    name = None
    replication_method = None
    replication_key = None
    key_properties = KEY_PROPERTIES
    stream = None

    def __init__(self, client=None):
        self.client = client

    def get_bookmark(self, state):
        return utils.strptime_with_tz(singer.get_bookmark(state, self.name, self.replication_key))

    def update_bookmark(self, state, value):
        current_bookmark = self.get_bookmark(state)
        if value and utils.strptime_with_tz(value) > current_bookmark:
            singer.write_bookmark(state, self.name, self.replication_key, value)

    def load_schema(self):
        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)
        return self._add_custom_fields(schema)

    def _add_custom_fields(self, schema): # pylint: disable=no-self-use
        return schema

    def load_metadata(self):
        schema = self.load_schema()
        mdata = metadata.new()

        mdata = metadata.write(mdata, (), 'table-key-properties', self.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method', self.replication_method)

        if self.replication_key:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', [self.replication_key])

        for field_name in schema['properties'].keys():
            if field_name in self.key_properties or field_name == self.replication_key:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)

    def is_selected(self):
        return self.stream is not None

class Organizations(Stream):
    name = "organizations"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def _add_custom_fields(self, schema):
        endpoint = self.client.organizations.endpoint
        # NB: Zenpy doesn't have a public endpoint for this at time of writing
        #     Calling into underlying query method to grab all fields
        field_gen = self.client.organizations._query_zendesk(endpoint.organization_fields, # pylint: disable=protected-access
                                                             'organization_field')
        schema['properties']['organization_fields']['properties'] = {}
        for field in field_gen:
            schema['properties']['organization_fields']['properties'][field.key] = process_custom_field(field)

        return schema

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        organizations = self.client.organizations.incremental(start_time=bookmark)
        for organization in organizations:
            self.update_bookmark(state, organization.updated_at)
            yield (self.stream, organization)

class Users(Stream):
    name = "users"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def _add_custom_fields(self, schema):
        field_gen = self.client.user_fields()
        schema['properties']['user_fields']['properties'] = {}
        for field in field_gen:
            schema['properties']['user_fields']['properties'][field.key] = process_custom_field(field)

        return schema

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        users = self.client.users.incremental(start_time=bookmark)
        for user in users:
            self.update_bookmark(state, user.updated_at)
            yield (self.stream, user)

class Tickets(Stream):
    name = "tickets"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    
    def sync(self, state):
        bookmark = self.get_bookmark(state)
        tickets = self.client.tickets.incremental(start_time=bookmark)
        audits_stream = TicketAudits(self.client)
        ticket_buffer = []
        audit_buffer = []
        if audits_stream.is_selected():
            LOGGER.info("Syncing ticket_audits per ticket...")

        for ticket in tickets:
            if utils.strptime_with_tz(ticket.updated_at) < bookmark:
                # NB: Skip tickets that might show up because of Zendesk behavior:
                #   The Incremental Ticket Export endpoint also returns tickets that
                #   were updated for reasons not related to ticket events, such as a system update or a database backfill.
                continue
            self.update_bookmark(state, ticket.updated_at)
            ticket_dict = ticket.to_dict()
            ticket_dict.pop('fields') # NB: Fields is a duplicate of custom_fields, remove before emitting

            ticket_buffer.append((self.stream, ticket_dict))
            if len(ticket_buffer) >= 100:
                for t in ticket_buffer:
                    yield t
                ticket_buffer = []

            if audits_stream.is_selected():
                if ticket_dict["status"] == "deleted":
                    LOGGER.warn("Unable to retrieve audits for deleted ticket (ID: %s), skipping...", ticket_dict["id"])
                    continue
                for audit in audits_stream.sync(ticket_dict["id"]):
                    audit_buffer.append(audit)

                    if len(audit_buffer) >= 100:
                        for a in audit_buffer:
                            yield a
                        audit_buffer = []
        if ticket_buffer:
            for t in ticket_buffer:
                yield t
        if audit_buffer:
            for a in audit_buffer:
                yield a

        singer.metrics.log(LOGGER, Point(metric_type='counter',
                                         metric=audits_stream.stream.tap_stream_id,
                                         value=audits_stream.count,
                                         tags=None))

class TicketAudits(Stream):
    name = "ticket_audits"
    replication_method = "INCREMENTAL"
    oldest_date = utils.strptime_with_tz('2018-07-06T18:01:35Z')
    count = 0
    
    def sync(self, ticket_id):
        ticket_audits = self.client.tickets.audits(ticket=ticket_id)
        for ticket_audit in ticket_audits:
            ticket_audit_dict = ticket_audit.to_dict()
            self.count += 1
                
            if utils.strptime_with_tz(ticket_audit_dict["created_at"]) < self.oldest_date:
                self.oldest_date = utils.strptime_with_tz(ticket_audit_dict["created_at"])
            yield (self.stream, ticket_audit_dict)

class Groups(Stream):
    name = "groups"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        groups = self.client.groups()
        for group in groups:
            if utils.strptime_with_tz(group.updated_at) >= bookmark:
                self.update_bookmark(state, group.updated_at)
                yield (self.stream, group)

class Macros(Stream):
    name = "macros"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        macros = self.client.macros()
        for macro in macros:
            if utils.strptime_with_tz(macro.updated_at) >= bookmark:
                self.update_bookmark(state, macro.updated_at)
                yield (self.stream, macro)

class Tags(Stream):
    name = "tags"
    replication_method = "FULL_TABLE"
    key_properties = ["name"]

    def sync(self, state): # pylint: disable=unused-argument
        # NB: Setting page to force it to paginate all tags, instead of just the
        #     top 100 popular tags
        return self.client.tags(page=1)

class TicketFields(Stream):
    name = "ticket_fields"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        fields = self.client.ticket_fields()
        for field in fields:
            if utils.strptime_with_tz(field.updated_at) >= bookmark:
                self.update_bookmark(state, field.updated_at)
                yield (self.stream, field)

class TicketMetrics(Stream):
    name = "ticket_metrics"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        ticket_metrics = self.client.ticket_metrics()

        for ticket_metric in ticket_metrics:
            if utils.strptime_with_tz(ticket_metric.updated_at) >= bookmark:
                self.update_bookmark(state, ticket_metric.updated_at)
                yield (self.stream, ticket_metric)

class GroupMemberships(Stream):
    name = "group_memberships"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        memberships = self.client.group_memberships()
        for membership in memberships:
            if utils.strptime_with_tz(membership.updated_at) >= bookmark:
                self.update_bookmark(state, membership.updated_at)
                yield (self.stream, membership)


STREAMS = {
    "tickets": Tickets,
    "groups": Groups,
    "users": Users,
    "organizations": Organizations,
    "ticket_audits": TicketAudits,
    "ticket_fields": TicketFields,
    "group_memberships": GroupMemberships,
    "macros": Macros,
    "tags": Tags,
    "ticket_metrics": TicketMetrics
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
