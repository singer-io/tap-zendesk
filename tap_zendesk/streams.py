import os
import json
import datetime
import asyncio
import time
import pytz
from zenpy.lib.exception import APIException
from aiohttp import ClientSession
import singer
from singer import metadata
from singer import utils
from singer.metrics import Point
from tap_zendesk import metrics as zendesk_metrics
from tap_zendesk import http


LOGGER = singer.get_logger()
KEY_PROPERTIES = ['id']

DEFAULT_PAGE_SIZE = 100
REQUEST_TIMEOUT = 300
CONCURRENCY_LIMIT = 20
# Reference: https://developer.zendesk.com/api-reference/introduction/rate-limits/#endpoint-rate-limits:~:text=List%20Audits%20for,requests%20per%20minute
AUDITS_REQUEST_PER_MINUTE = 450
START_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}
CUSTOM_TYPES = {
    'text': 'string',
    'textarea': 'string',
    'date': 'string',
    'regexp': 'string',
    'dropdown': 'string',
    'integer': 'integer',
    'decimal': 'number',
    'checkbox': 'boolean',
    'lookup': 'integer',
}

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def process_custom_field(field):
    """ Take a custom field description and return a schema for it. """
    if field.type not in CUSTOM_TYPES:
        LOGGER.critical("Discovered unsupported type for custom field %s (key: %s): %s",
                        field.title, field.key, field.type)

    json_type = CUSTOM_TYPES.get(field.type, "string")
    field_schema = {'type': [json_type, 'null']}
    if field.type == 'date':
        field_schema['format'] = 'datetime'
    if field.type == 'dropdown':
        field_schema['enum'] = [o.value for o in field.custom_field_options]

    return field_schema

class Stream():
    name = None
    replication_method = None
    replication_key = None
    key_properties = KEY_PROPERTIES
    stream = None
    endpoint = None
    request_timeout = None
    page_size = None

    def __init__(self, client=None, config=None):
        self.client = client
        self.config = config
        # Set and pass request timeout to config param `request_timeout` value.
        config_request_timeout = self.config.get('request_timeout')
        if config_request_timeout and float(config_request_timeout):
            self.request_timeout = float(config_request_timeout)
        else:
            self.request_timeout = REQUEST_TIMEOUT # If value is 0,"0","" or not passed then it set default to 300 seconds.

        # To avoid infinite loop behavior we should not configure search window less than 2
        if config.get('search_window_size') and int(config.get('search_window_size')) < 2:
            raise ValueError('Search window size cannot be less than 2')

        config_page_size = self.config.get('page_size')
        if config_page_size and 1 <= int(config_page_size) <= 1000: # Zendesk's max page size
            self.page_size = int(config_page_size)
        else:
            self.page_size = DEFAULT_PAGE_SIZE

    def get_bookmark(self, state):
        return utils.strptime_with_tz(singer.get_bookmark(state, self.name, self.replication_key))

    def update_bookmark(self, state, value):
        current_bookmark = self.get_bookmark(state)
        if value and utils.strptime_with_tz(value) > current_bookmark:
            singer.write_bookmark(state, self.name, self.replication_key, value)


    def load_schema(self):
        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file), encoding='UTF-8') as f:
            schema = json.load(f)
        return self._add_custom_fields(schema)

    def _add_custom_fields(self, schema):
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

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        url = self.endpoint.format(self.config['subdomain'])
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config["access_token"])

        http.call_api(url, self.request_timeout, params={'per_page': 1}, headers=HEADERS)

class CursorBasedStream(Stream):
    item_key = None
    endpoint = None

    def get_objects(self, **kwargs):
        '''
        Cursor based object retrieval
        '''
        url = self.endpoint.format(self.config['subdomain'])
        # Pass `request_timeout` parameter
        for page in http.get_cursor_based(url, self.config['access_token'], self.request_timeout, self.page_size, **kwargs):
            yield from page[self.item_key]

class CursorBasedExportStream(Stream):
    endpoint = None
    item_key = None

    def get_objects(self, start_time, side_load=None):
        '''
        Retrieve objects from the incremental exports endpoint using cursor based pagination
        '''
        url = self.endpoint.format(self.config['subdomain'])
        # Pass `request_timeout` parameter
        for page in http.get_incremental_export(url, self.config['access_token'], self.request_timeout, start_time, side_load):
            yield from page[self.item_key]


def raise_or_log_zenpy_apiexception(schema, stream, e):
    # There are multiple tiers of Zendesk accounts. Some of them have
    # access to `custom_fields` and some do not. This is the specific
    # error that appears to be return from the API call in the event that
    # it doesn't have access.
    if not isinstance(e, APIException):
        raise ValueError("Called with a bad exception type") from e

    #If read permission is not available in OAuth access_token, then it returns the below error.
    if json.loads(e.args[0]).get('description') == "You are missing the following required scopes: read":
        LOGGER.warning("The account credentials supplied do not have access to `%s` custom fields.",
                       stream)
        return schema
    error = json.loads(e.args[0]).get('error')
    # check if the error is of type dictionary and the message retrieved from the dictionary
    # is the expected message. If so, only then print the logger message and return the schema
    if isinstance(error, dict) and error.get('message', None) == "You do not have access to this page. Please contact the account owner of this help desk for further help.":
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
        except APIException as e:
            return raise_or_log_zenpy_apiexception(schema, self.name, e)
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

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # Convert datetime object to standard format with timezone. Used utcnow to reduce API call burden at discovery time.
        # Because API will return records from now which will be very less
        start_time = datetime.datetime.utcnow().strftime(START_DATE_FORMAT)
        self.client.organizations.incremental(start_time=start_time)

class Users(CursorBasedExportStream):
    name = "users"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    item_key = "users"
    endpoint = "https://{}.zendesk.com/api/v2/incremental/users/cursor.json"

    def _add_custom_fields(self, schema):
        try:
            field_gen = self.client.user_fields()
        except APIException as e:
            return raise_or_log_zenpy_apiexception(schema, self.name, e)
        schema['properties']['user_fields']['properties'] = {}
        for field in field_gen:
            schema['properties']['user_fields']['properties'][field.key] = process_custom_field(field)

        return schema

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        epoch_bookmark = int(bookmark.timestamp())
        users = self.get_objects(epoch_bookmark)

        for user in users:
            self.update_bookmark(state, user["updated_at"])
            yield (self.stream, user)

        singer.write_state(state)


    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # Convert datetime object to standard format with timezone. Used utcnow to reduce API call burden at discovery time.
        # Because API will return records from now which will be very less
        start_time = datetime.datetime.utcnow().strftime(START_DATE_FORMAT)
        self.client.search("", updated_after=start_time, updated_before='2000-01-02T00:00:00Z', type="user")

class Tickets(CursorBasedExportStream):
    name = "tickets"
    replication_method = "INCREMENTAL"
    replication_key = "generated_timestamp"
    item_key = "tickets"
    endpoint = "https://{}.zendesk.com/api/v2/incremental/tickets/cursor.json"

    def sync(self, state): #pylint: disable=too-many-statements

        bookmark = self.get_bookmark(state)

        # Fetch tickets with side loaded metrics
        # https://developer.zendesk.com/documentation/ticketing/using-the-zendesk-api/side_loading/#supported-endpoints
        tickets = self.get_objects(bookmark, side_load='metric_sets')

        audits_stream = TicketAudits(self.client, self.config)
        metrics_stream = TicketMetrics(self.client, self.config)
        comments_stream = TicketComments(self.client, self.config)

        def emit_sub_stream_metrics(sub_stream):
            if sub_stream.is_selected():
                singer.metrics.log(LOGGER, Point(metric_type='counter',
                                                 metric=singer.metrics.Metric.record_count,
                                                 value=sub_stream.count,
                                                 tags={'endpoint':sub_stream.stream.tap_stream_id}))
                sub_stream.count = 0

        if audits_stream.is_selected():
            LOGGER.info("Syncing ticket_audits per ticket...")

        ticket_ids = []
        counter = 0
        start_time = time.time()
        for ticket in tickets:
            zendesk_metrics.capture('ticket')

            generated_timestamp_dt = datetime.datetime.utcfromtimestamp(ticket.get('generated_timestamp')).replace(tzinfo=pytz.UTC)

            self.update_bookmark(state, utils.strftime(generated_timestamp_dt))

            ticket.pop('fields') # NB: Fields is a duplicate of custom_fields, remove before emitting
            # yielding stream name with record in a tuple as it is used for obtaining only the parent records while sync
            yield (self.stream, ticket)

            # Skip deleted tickets because they don't have audits or comments
            if ticket.get('status') == 'deleted':
                continue

            if metrics_stream.is_selected() and ticket.get('metric_set'):
                zendesk_metrics.capture('ticket_metric')
                metrics_stream.count+=1
                yield (metrics_stream.stream, ticket["metric_set"])

            # Check if the number of ticket IDs has reached the batch size.
            ticket_ids.append(ticket["id"])
            if len(ticket_ids) >= CONCURRENCY_LIMIT:
                # Process audits and comments in batches
                records = self.sync_ticket_audits_and_comments(
                    comments_stream, audits_stream, ticket_ids)
                for audits, comments in records:
                    for audit in audits:
                        yield audit
                    for comment in comments:
                        yield comment
                # Reset the list of ticket IDs after processing the batch.
                ticket_ids = []
                # Write state after processing the batch.
                singer.write_state(state)
                counter += CONCURRENCY_LIMIT

                # Check if the number of records processed in a minute has reached the limit.
                if counter >= AUDITS_REQUEST_PER_MINUTE:
                    # Calculate elapsed time
                    elapsed_time = time.time() - start_time

                    # Calculate remaining time until the next minute, plus buffer of 2 more seconds
                    remaining_time = max(0, 60 - elapsed_time + 2)

                    # Sleep for the calculated time
                    time.sleep(remaining_time)
                    start_time = time.time()
                    counter = 0

        # Check if there are any remaining ticket IDs after the loop.
        if ticket_ids:
            records = self.sync_ticket_audits_and_comments(comments_stream, audits_stream, ticket_ids)
            for audits, comments in records:
                for audit in audits:
                    yield audit
                for comment in comments:
                    yield comment

        emit_sub_stream_metrics(audits_stream)
        emit_sub_stream_metrics(metrics_stream)
        emit_sub_stream_metrics(comments_stream)
        singer.write_state(state)

    def sync_ticket_audits_and_comments(self, comments_stream, audits_stream, ticket_ids):
        if comments_stream.is_selected() or audits_stream.is_selected():
            return asyncio.run(audits_stream.sync_in_bulk(ticket_ids, comments_stream))
        # Return empty list of audits and comments if not selected
        return [([], [])]

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        url = self.endpoint.format(self.config['subdomain'])
        # Convert start_date parameter to timestamp to pass with request param
        start_time = datetime.datetime.strptime(self.config['start_date'], START_DATE_FORMAT).timestamp()
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config["access_token"])

        http.call_api(url, self.request_timeout, params={'start_time': start_time, 'per_page': 1}, headers=HEADERS)


class TicketAudits(Stream):
    name = "ticket_audits"
    replication_method = "INCREMENTAL"
    count = 0
    endpoint='https://{}.zendesk.com/api/v2/tickets/{}/audits.json'
    item_key='audits'

    async def sync_in_bulk(self, ticket_ids, comments_stream):
        """
        Asynchronously fetch ticket audits for multiple tickets
        """
        # Create an asynchronous HTTP session
        async with ClientSession() as session:
            tasks = [self.sync(session, ticket_id, comments_stream)
                     for ticket_id in ticket_ids]
            # Run all tasks concurrently and wait for them to complete
            return await asyncio.gather(*tasks)

    async def get_objects(self, session, ticket_id):
        url = self.endpoint.format(self.config['subdomain'], ticket_id)
        # Fetch the ticket audits using pagination
        records = await http.paginate_ticket_audits(session, url, self.config['access_token'], self.request_timeout, self.page_size)

        return records[self.item_key]

    async def sync(self, session, ticket_id, comments_stream):
        """
        Fetch ticket audits for a single ticket. Also exctract comments for each audit.
        """
        audit_records, comment_records = [], []
        try:
            # Fetch ticket audits for the given ticket ID
            ticket_audits = await self.get_objects(session, ticket_id)
            for ticket_audit in ticket_audits:
                if self.is_selected():
                    zendesk_metrics.capture('ticket_audit')
                    self.count += 1
                    audit_records.append((self.stream, ticket_audit))

                if comments_stream.is_selected():
                    # Extract comments from ticket audit
                    ticket_comments = (
                        event for event in ticket_audit['events'] if event['type'] == 'Comment')
                    zendesk_metrics.capture('ticket_comments')
                    for ticket_comment in ticket_comments:
                        # Update the comment with additional information
                        ticket_comment.update({
                            'created_at': ticket_audit['created_at'],
                            'via': ticket_audit['via'],
                            'metadata': ticket_audit['metadata'],
                            'ticket_id': ticket_id
                        })

                        comments_stream.count += 1
                        comment_records.append(
                            (comments_stream.stream, ticket_comment))
        except http.ZendeskNotFoundError:
            return audit_records, comment_records

        return audit_records, comment_records

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''

        url = self.endpoint.format(self.config['subdomain'], '1')
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config["access_token"])
        try:
            http.call_api(url, self.request_timeout, params={'per_page': 1}, headers=HEADERS)
        except http.ZendeskNotFoundError:
            #Skip 404 ZendeskNotFoundError error as goal is just to check whether TicketComments have read permission or not
            pass

class TicketMetrics(CursorBasedStream):
    name = "ticket_metrics"
    replication_method = "INCREMENTAL"
    count = 0

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # We load metrics as side load of tickets, so we don't need to check access
        return

class TicketMetricEvents(Stream):
    name = "ticket_metric_events"
    replication_method = "INCREMENTAL"
    replication_key = "time"
    count = 0

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        start = bookmark - datetime.timedelta(seconds=1)

        epoch_start = int(start.strftime('%s'))
        parsed_start = singer.strftime(start, "%Y-%m-%dT%H:%M:%SZ")
        ticket_metric_events = self.client.tickets.metrics_incremental(start_time=epoch_start)
        for event in ticket_metric_events:
            self.count += 1
            if bookmark < utils.strptime_with_tz(event.time):
                self.update_bookmark(state, event.time)
            if parsed_start <= event.time:
                yield (self.stream, event)

    def check_access(self):
        try:
            epoch_start = int(utils.now().strftime('%s'))
            self.client.tickets.metrics_incremental(start_time=epoch_start)
        except http.ZendeskNotFoundError:
            #Skip 404 ZendeskNotFoundError error as goal is just to check whether TicketComments have read permission or not
            pass

class TicketComments(Stream):
    name = "ticket_comments"
    replication_method = "INCREMENTAL"
    count = 0

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # We load comments as side load of ticket_audits, so we don't need to check access
        return

class TalkPhoneNumbers(Stream):
    name = 'talk_phone_numbers'
    replication_method = "FULL_TABLE"

    def sync(self, state): # pylint: disable=unused-argument
        for phone_number in self.client.talk.phone_numbers():
            yield (self.stream, phone_number)

    def check_access(self):
        try:
            self.client.talk.phone_numbers()
        except http.ZendeskNotFoundError:
            #Skip 404 ZendeskNotFoundError error as goal is to just check to whether TicketComments have read permission or not
            pass

class SatisfactionRatings(CursorBasedStream):
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


class Groups(CursorBasedStream):
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

class Macros(CursorBasedStream):
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

class Tags(CursorBasedStream):
    name = "tags"
    replication_method = "FULL_TABLE"
    key_properties = ["name"]
    endpoint = 'https://{}.zendesk.com/api/v2/tags'
    item_key = 'tags'

    def sync(self, state): # pylint: disable=unused-argument
        tags = self.get_objects()

        for tag in tags:
            yield (self.stream, tag)

class TicketFields(CursorBasedStream):
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

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        self.client.ticket_forms()

class GroupMemberships(CursorBasedStream):
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

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        self.client.sla_policies()

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
    "ticket_metric_events": TicketMetricEvents,
    "sla_policies": SLAPolicies,
    "talk_phone_numbers": TalkPhoneNumbers,
}
