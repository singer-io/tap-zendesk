import os
import json
import datetime
import time
import pytz
import zenpy
from zenpy.lib.exception import RecordNotFoundException
import singer
from singer import metadata
from singer import utils
from singer.metrics import Point
from tap_zendesk import metrics as zendesk_metrics
from tap_zendesk import http


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

DEFAULT_SEARCH_WINDOW_SIZE = (60 * 60 * 24) * 30 # defined in seconds, default to a month (30 days)

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
        field_schema['enum'] = [o.value for o in field.custom_field_options]

    return field_schema

class Stream():
    name = None
    replication_method = None
    replication_key = None
    key_properties = KEY_PROPERTIES
    stream = None
    item_key = None
    endpoint = None

    def __init__(self, client=None, config=None):
        self.client = client
        self.config = config

    def get_objects(self, **kwargs):
        '''
        Cursor based object retrieval
        '''
        url = self.endpoint.format(self.config['subdomain'])

        for page in http.get_cursor_based(url, self.config['access_token'], **kwargs):
            yield from page[self.item_key]

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

def raise_or_log_zenpy_apiexception(schema, stream, e):
    # There are multiple tiers of Zendesk accounts. Some of them have
    # access to `custom_fields` and some do not. This is the specific
    # error that appears to be return from the API call in the event that
    # it doesn't have access.
    if not isinstance(e, zenpy.lib.exception.APIException):
        raise ValueError("Called with a bad exception type") from e
    if json.loads(e.args[0])['error']['message'] == "You do not have access to this page. Please contact the account owner of this help desk for further help.":
        LOGGER.warning("The account credentials supplied do not have access to `%s` custom fields.",
                       stream)
        return schema
    else:
        raise e


class Organizations(Stream):
    name = "organizations"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/organizations'
    item_key = 'organizations'

    def _add_custom_fields(self, schema):
        endpoint = self.client.organizations.endpoint
        # NB: Zenpy doesn't have a public endpoint for this at time of writing
        #     Calling into underlying query method to grab all fields
        try:
            field_gen = self.client.organizations._query_zendesk(endpoint.organization_fields, # pylint: disable=protected-access
                                                                 'organization_field')
        except zenpy.lib.exception.APIException as e:
            return raise_or_log_zenpy_apiexception(schema, self.name, e)
        schema['properties']['organization_fields']['properties'] = {}
        for field in field_gen:
            schema['properties']['organization_fields']['properties'][field.key] = process_custom_field(field)

        return schema

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        organizations = self.get_objects()
        for organization in organizations:
            if utils.strptime_with_tz(organization['updated_at']) >= bookmark:
                self.update_bookmark(state, organization['updated_at'])
                yield (self.stream, organization)


class Users(Stream):
    name = "users"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def _add_custom_fields(self, schema):
        try:
            field_gen = self.client.user_fields()
        except zenpy.lib.exception.APIException as e:
            return raise_or_log_zenpy_apiexception(schema, self.name, e)
        schema['properties']['user_fields']['properties'] = {}
        for field in field_gen:
            schema['properties']['user_fields']['properties'][field.key] = process_custom_field(field)

        return schema

    def sync(self, state):
        original_search_window_size = int(self.config.get('search_window_size', DEFAULT_SEARCH_WINDOW_SIZE))
        search_window_size = original_search_window_size
        bookmark = self.get_bookmark(state)
        start = bookmark - datetime.timedelta(seconds=1)
        end = start + datetime.timedelta(seconds=search_window_size)
        sync_end = singer.utils.now() - datetime.timedelta(minutes=1)
        parsed_sync_end = singer.strftime(sync_end, "%Y-%m-%dT%H:%M:%SZ")

        # ASSUMPTION: updated_at value always comes back in utc
        num_retries = 0
        while start < sync_end:
            parsed_start = singer.strftime(start, "%Y-%m-%dT%H:%M:%SZ")
            parsed_end = min(singer.strftime(end, "%Y-%m-%dT%H:%M:%SZ"), parsed_sync_end)
            LOGGER.info("Querying for users between %s and %s", parsed_start, parsed_end)
            users = self.client.search("", updated_after=parsed_start, updated_before=parsed_end, type="user")

            # NB: Zendesk will return an error on the 1001st record, so we
            # need to check total response size before iterating
            # See: https://develop.zendesk.com/hc/en-us/articles/360022563994--BREAKING-New-Search-API-Result-Limits
            if users.count > 1000:
                if search_window_size > 1:
                    search_window_size = search_window_size // 2
                    end = start + datetime.timedelta(seconds=search_window_size)
                    LOGGER.info("users - Detected Search API response size too large. Cutting search window in half to %s seconds.", search_window_size)
                    continue

                raise Exception("users - Unable to get all users within minimum window of a single second ({}), found {} users within this timestamp. Zendesk can only provide a maximum of 1000 users per request. See: https://develop.zendesk.com/hc/en-us/articles/360022563994--BREAKING-New-Search-API-Result-Limits".format(parsed_start, users.count))

            # Consume the records to account for dates lower than window start
            users = [user for user in users] # pylint: disable=unnecessary-comprehension

            if not all(parsed_start <= user.updated_at for user in users):
                # Only retry up to 30 minutes (60 attempts at 30 seconds each)
                if num_retries < 60:
                    LOGGER.info("users - Record found before date window start. Waiting 30 seconds, then retrying window for consistency. (Retry #%s)", num_retries + 1)
                    time.sleep(30)
                    num_retries += 1
                    continue
                bad_users = [user for user in users if user.updated_at < parsed_start]
                raise AssertionError("users - Record (user-id: {}) found before date window start and did not resolve after 30 minutes of retrying. Details: window start ({}) is not less than or equal to updated_at value(s) {}".format(
                    [user.id for user in bad_users],
                    parsed_start,
                    [str(user.updated_at) for user in bad_users]))

            # If we make it here, all quality checks have passed. Reset retry count.
            num_retries = 0
            for user in users:
                if parsed_start <= user.updated_at <= parsed_end:
                    yield (self.stream, user)
            self.update_bookmark(state, parsed_end)

            # Assumes that the for loop got everything
            singer.write_state(state)
            if search_window_size <= original_search_window_size // 2:
                search_window_size = search_window_size * 2
                LOGGER.info("Successfully requested records. Doubling search window to %s seconds", search_window_size)
            start = end - datetime.timedelta(seconds=1)
            end = start + datetime.timedelta(seconds=search_window_size)


class Tickets(Stream):
    name = "tickets"
    replication_method = "INCREMENTAL"
    replication_key = "generated_timestamp"

    last_record_emit = {}
    buf = {}
    buf_time = 60
    def _buffer_record(self, record):
        stream_name = record[0].tap_stream_id
        if self.last_record_emit.get(stream_name) is None:
            self.last_record_emit[stream_name] = utils.now()

        if self.buf.get(stream_name) is None:
            self.buf[stream_name] = []
        self.buf[stream_name].append(record)

        if (utils.now() - self.last_record_emit[stream_name]).total_seconds() > self.buf_time:
            self.last_record_emit[stream_name] = utils.now()
            return True

        return False

    def _empty_buffer(self):
        for stream_name, stream_buf in self.buf.items():
            for rec in stream_buf:
                yield rec
            self.buf[stream_name] = []

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        tickets = self.client.tickets.incremental(start_time=bookmark, paginate_by_time=False)

        audits_stream = TicketAudits(self.client)
        metrics_stream = TicketMetrics(self.client)
        comments_stream = TicketComments(self.client)

        def emit_sub_stream_metrics(sub_stream):
            if sub_stream.is_selected():
                singer.metrics.log(LOGGER, Point(metric_type='counter',
                                                 metric=singer.metrics.Metric.record_count,
                                                 value=sub_stream.count,
                                                 tags={'endpoint':sub_stream.stream.tap_stream_id}))
                sub_stream.count = 0

        if audits_stream.is_selected():
            LOGGER.info("Syncing ticket_audits per ticket...")

        for ticket in tickets:
            zendesk_metrics.capture('ticket')
            generated_timestamp_dt = datetime.datetime.utcfromtimestamp(ticket.generated_timestamp).replace(tzinfo=pytz.UTC)
            self.update_bookmark(state, utils.strftime(generated_timestamp_dt))

            ticket_dict = ticket.to_dict()
            ticket_dict.pop('fields') # NB: Fields is a duplicate of custom_fields, remove before emitting
            should_yield = self._buffer_record((self.stream, ticket_dict))

            if audits_stream.is_selected():
                try:
                    for audit in audits_stream.sync(ticket_dict["id"]):
                        zendesk_metrics.capture('ticket_audit')
                        self._buffer_record(audit)
                except RecordNotFoundException:
                    LOGGER.warning("Unable to retrieve audits for ticket (ID: %s), " \
                    "the Zendesk API returned a RecordNotFound error", ticket_dict["id"])

            if metrics_stream.is_selected():
                try:
                    for metric in metrics_stream.sync(ticket_dict["id"]):
                        zendesk_metrics.capture('ticket_metric')
                        self._buffer_record(metric)
                except RecordNotFoundException:
                    LOGGER.warning("Unable to retrieve metrics for ticket (ID: %s), " \
                    "the Zendesk API returned a RecordNotFound error", ticket_dict["id"])

            if comments_stream.is_selected():
                try:
                    # add ticket_id to ticket_comment so the comment can
                    # be linked back to it's corresponding ticket
                    for comment in comments_stream.sync(ticket_dict["id"]):
                        zendesk_metrics.capture('ticket_comment')
                        comment[1].ticket_id = ticket_dict["id"]
                        self._buffer_record(comment)
                except RecordNotFoundException:
                    LOGGER.warning("Unable to retrieve comments for ticket (ID: %s), " \
                    "the Zendesk API returned a RecordNotFound error", ticket_dict["id"])

            if should_yield:
                for rec in self._empty_buffer():
                    yield rec
                emit_sub_stream_metrics(audits_stream)
                emit_sub_stream_metrics(metrics_stream)
                emit_sub_stream_metrics(comments_stream)
                singer.write_state(state)

        for rec in self._empty_buffer():
            yield rec
        emit_sub_stream_metrics(audits_stream)
        emit_sub_stream_metrics(metrics_stream)
        emit_sub_stream_metrics(comments_stream)
        singer.write_state(state)

class TicketAudits(Stream):
    name = "ticket_audits"
    replication_method = "INCREMENTAL"
    count = 0

    def sync(self, ticket_id):
        ticket_audits = self.client.tickets.audits(ticket=ticket_id)
        for ticket_audit in ticket_audits:
            self.count += 1
            yield (self.stream, ticket_audit)

class TicketMetrics(Stream):
    name = "ticket_metrics"
    replication_method = "INCREMENTAL"
    count = 0

    def sync(self, ticket_id):
        ticket_metric = self.client.tickets.metrics(ticket=ticket_id)
        self.count += 1
        yield (self.stream, ticket_metric)

class TicketComments(Stream):
    name = "ticket_comments"
    replication_method = "INCREMENTAL"
    count = 0

    def sync(self, ticket_id):
        ticket_comments = self.client.tickets.comments(ticket=ticket_id)
        for ticket_comment in ticket_comments:
            self.count += 1
            yield (self.stream, ticket_comment)

class SatisfactionRatings(Stream):
    name = "satisfaction_ratings"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/satisfaction_ratings'
    item_key = 'satisfaction_ratings'

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        epoch_bookmark = int(bookmark.timestamp())
        params = {'start_time': epoch_bookmark}
        ratings = self.get_objects(params=params)
        for rating in ratings:
            if utils.strptime_with_tz(rating['updated_at']) >= bookmark:
                self.update_bookmark(state, rating['updated_at'])
                yield (self.stream, rating)


class Groups(Stream):
    name = "groups"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/groups'
    item_key = 'groups'

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        groups = self.get_objects()
        for group in groups:
            if utils.strptime_with_tz(group['updated_at']) >= bookmark:
                # NB: We don't trust that the records come back ordered by
                # updated_at (we've observed out-of-order records),
                # so we can't save state until we've seen all records
                self.update_bookmark(state, group['updated_at'])
                yield (self.stream, group)

class Macros(Stream):
    name = "macros"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/macros'
    item_key = 'macros'

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        macros = self.get_objects()
        for macro in macros:
            if utils.strptime_with_tz(macro['updated_at']) >= bookmark:
                # NB: We don't trust that the records come back ordered by
                # updated_at (we've observed out-of-order records),
                # so we can't save state until we've seen all records
                self.update_bookmark(state, macro['updated_at'])
                yield (self.stream, macro)

class Tags(Stream):
    name = "tags"
    replication_method = "FULL_TABLE"
    key_properties = ["name"]
    endpoint = 'https://{}.zendesk.com/api/v2/tags'
    item_key = 'tags'

    def sync(self, state): # pylint: disable=unused-argument
        tags = self.get_objects()

        for tag in tags:
            yield (self.stream, tag)

class TicketFields(Stream):
    name = "ticket_fields"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/ticket_fields'
    item_key = 'ticket_fields'

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        fields = self.get_objects()
        for field in fields:
            if utils.strptime_with_tz(field['updated_at']) >= bookmark:
                # NB: We don't trust that the records come back ordered by
                # updated_at (we've observed out-of-order records),
                # so we can't save state until we've seen all records
                self.update_bookmark(state, field['updated_at'])
                yield (self.stream, field)

class TicketForms(Stream):
    name = "ticket_forms"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        forms = self.client.ticket_forms()
        for form in forms:
            if utils.strptime_with_tz(form.updated_at) >= bookmark:
                # NB: We don't trust that the records come back ordered by
                # updated_at (we've observed out-of-order records),
                # so we can't save state until we've seen all records
                self.update_bookmark(state, form.updated_at)
                yield (self.stream, form)

class GroupMemberships(Stream):
    name = "group_memberships"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/group_memberships'
    item_key = 'group_memberships'


    def sync(self, state):
        bookmark = self.get_bookmark(state)
        memberships = self.get_objects()

        for membership in memberships:
            # some group memberships come back without an updated_at
            if membership['updated_at']:
                if utils.strptime_with_tz(membership['updated_at']) >= bookmark:
                    # NB: We don't trust that the records come back ordered by
                    # updated_at (we've observed out-of-order records),
                    # so we can't save state until we've seen all records
                    self.update_bookmark(state, membership['updated_at'])
                    yield (self.stream, membership)
            else:
                if membership['id']:
                    LOGGER.info('group_membership record with id: ' + str(membership['id']) +
                                ' does not have an updated_at field so it will be syncd...')
                    yield (self.stream, membership)
                else:
                    LOGGER.info('Received group_membership record with no id or updated_at, skipping...')

class SLAPolicies(Stream):
    name = "sla_policies"
    replication_method = "FULL_TABLE"

    def sync(self, state): # pylint: disable=unused-argument
        for policy in self.client.sla_policies():
            yield (self.stream, policy)

STREAMS = {
    "tickets": Tickets,
    "groups": Groups,
    "users": Users,
    "organizations": Organizations,
    "ticket_audits": TicketAudits,
    "ticket_comments": TicketComments,
    "ticket_fields": TicketFields,
    "ticket_forms": TicketForms,
    "group_memberships": GroupMemberships,
    "macros": Macros,
    "satisfaction_ratings": SatisfactionRatings,
    "tags": Tags,
    "ticket_metrics": TicketMetrics,
    "sla_policies": SLAPolicies,
}
