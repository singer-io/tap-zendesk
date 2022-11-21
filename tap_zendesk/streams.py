import os
import json
import datetime
import math
import pytz
import zenpy
from zenpy.lib.exception import RecordNotFoundException
import singer
from singer import metadata
from singer import utils
from singer.metrics import Point
from tap_zendesk import metrics as zendesk_metrics


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

    def __init__(self, client=None, config=None):
        self.client = client
        self.config = config
        if config:
            self.start_date = utils.strptime_with_tz(config['start_date'])
        else:
            self.start_date = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

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

    def _get_bookmark_key(self, log: bool=False) -> str:
        bookmark_key = self.replication_key
        if self.replication_key is None:
            # When we serialize None to json, it turns into 'null'. However, when we deserialize the
            # state, the key to search for is 'null' and not None. This means that it will always use the start date
            # since the key is never found.
            bookmark_key = 'null'
            if log:
                LOGGER.warning(f'no replication key set for {self.name}, bookmark key set to "{bookmark_key}"')
        return bookmark_key

    def get_bookmark(self, state):
        bookmark_key = self._get_bookmark_key()
        state_bookmark = singer.get_bookmark(state, self.name, bookmark_key)
        return utils.strptime_with_tz(state_bookmark)

    def update_bookmark(self, state, value):
        current_bookmark = self.get_bookmark(state)
        bookmark_key = self._get_bookmark_key()
        if value and utils.strptime_with_tz(value) > current_bookmark:
            singer.write_bookmark(state, self.name, bookmark_key, value)

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
        organizations = self.client.organizations.incremental(start_time=bookmark)
        for organization in organizations:
            self.update_bookmark(state, organization.updated_at)
            yield (self.stream, organization)

class Users(Stream):
    name = "users"
    replication_method = "FULL_TABLE"
    key_properties = ["id"]

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
        users = self.client.users(role=['agent', 'admin'])
        for user in users:
            yield (self.stream, user)

class Tickets(Stream):
    name = "tickets"
    replication_method = "INCREMENTAL"
    replication_key = "generated_timestamp"

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        metrics_stream = TicketMetrics(self.client)

        include = []
        if metrics_stream.is_selected():
            include.append('metric_sets')
        tickets = self.client.tickets.incremental(start_time=bookmark, include=include)

        def emit_sub_stream_metrics(sub_stream):
            if sub_stream.is_selected():
                singer.metrics.log(LOGGER, Point(metric_type='counter',
                                                 metric=singer.metrics.Metric.record_count,
                                                 value=sub_stream.count,
                                                 tags={'endpoint':sub_stream.stream.tap_stream_id}))
                sub_stream.count = 0

        for ticket in tickets:
            zendesk_metrics.capture('ticket')
            generated_timestamp_dt = datetime.datetime.utcfromtimestamp(ticket.generated_timestamp).replace(tzinfo=pytz.UTC)
            self.update_bookmark(state, utils.strftime(generated_timestamp_dt))

            ticket_dict = ticket.to_dict()
            ticket_dict.pop('fields') # NB: Fields is a duplicate of custom_fields, remove before emitting
            should_yield = self._buffer_record((self.stream, ticket_dict))

            if metrics_stream.is_selected():
                try:
                    for metric in metrics_stream.sync(ticket_dict):
                        zendesk_metrics.capture('ticket_metric')
                        self._buffer_record(metric)
                except RecordNotFoundException:
                    LOGGER.warning("Unable to retrieve metrics for ticket (ID: %s), " \
                    "the Zendesk API returned a RecordNotFound error", ticket_dict["id"])

            if should_yield:
                for rec in self._empty_buffer():
                    yield rec
                emit_sub_stream_metrics(metrics_stream)
                singer.write_state(state)

        for rec in self._empty_buffer():
            yield rec
        emit_sub_stream_metrics(metrics_stream)
        singer.write_state(state)

class TicketAudits(Stream):
    name = "ticket_audits"
    replication_method = "INCREMENTAL"
    replication_key = "created_at"

    # The default (and max) limit is 1000. I've observed more than 1000 results
    # come back sometimes, so it's conceivable that less than 1000 could come
    # back on a normal request. If we have fewer than 500 results in a request,
    # we assume we are running out of results to get and we'll try again later.
    MINIMUM_REQUIRED_RESULT_SET_SIZE = 500

    def sync(self, state, lookback_minutes):
        try:
            sync_thru = self.get_bookmark(state)
        except TypeError:  # Happens when there is no bookmark yet
            sync_thru = self.start_date

        if not lookback_minutes:
            # Happens when there are no "lookback_minutes" set in the config.
            # Rather than fail here we can use 10 minutes as a default, and if
            # a customer has few ticket audits per minute, this number can be
            # increased in the config json within the TapConfig object
            lookback_minutes = 10

        sync_thru = max(sync_thru, self.start_date)
        next_synced_thru = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
        curr_synced_thru = datetime.datetime.max.replace(tzinfo=datetime.timezone.utc)

        events_stream = TicketAuditEvents(self.client)

        def lt(lookback_minutes, dt1, dt2):
            # Since audits are only roughly ordered, we want to be sure
            # that dt1 is less than dt2 by several minutes so there is a
            # low chance we miss any.
            return dt1 < dt2 - datetime.timedelta(minutes=lookback_minutes)

        audits_generator = self.client.tickets.audits()
        ticket_audits = reversed(audits_generator)

        for audit in ticket_audits:
            next_synced_thru = max(next_synced_thru, utils.strptime_with_tz(audit.created_at))
            curr_synced_thru = min(curr_synced_thru, utils.strptime_with_tz(audit.created_at))
            if lt(lookback_minutes, curr_synced_thru, sync_thru):
                self.update_bookmark(state, utils.strftime(next_synced_thru))
                return

            zendesk_metrics.capture('ticket_audit')
            yield (self.stream, audit)

            if events_stream.is_selected():
                yield from events_stream.sync(audit)
            else:
                LOGGER.info('not syncing ticket_audit_events (stream not selected)')


class TicketAuditEvents(Stream):
    name = "ticket_audit_events"
    replication_method = "INCREMENTAL"
    count = 0

    def sync(self, audit):
        for event in audit.events:
            self.count += 1
            event = {
                **event,
                'ticket_audit_id': audit.id,
                'ticket_audit_created_at': audit.created_at,
                'ticket_id': audit.ticket_id,
            }
            if event.get('value'):
                event['value'] = json.dumps(event['value'])
            yield (self.stream, event)


class TicketMetrics(Stream):
    name = "ticket_metrics"
    replication_method = "INCREMENTAL"
    count = 0

    def sync(self, ticket_dict):
        ticket_metric = ticket_dict['metric_set']
        self.count += 1
        yield (self.stream, ticket_metric)

class TicketEvents(Stream):
    name = "ticket_events"
    replication_method = "INCREMENTAL"
    replication_key = 'timestamp'

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        comments_stream = TicketComments(self.client)
        include = []
        if comments_stream.is_selected():
            include.append('comment_events')

        def emit_sub_stream_metrics(sub_stream):
            if sub_stream.is_selected():
                singer.metrics.log(LOGGER, Point(metric_type='counter',
                                                 metric=singer.metrics.Metric.record_count,
                                                 value=sub_stream.count,
                                                 tags={'endpoint':sub_stream.stream.tap_stream_id}))
                sub_stream.count = 0

        ticket_events = self.client.tickets.events(bookmark, include=include)
        for event in ticket_events:
            event_timestamp = datetime.datetime.fromtimestamp(event.timestamp, pytz.utc)
            # See the following URL for why we check the timestamp against the bookmark:
            # https://developer.zendesk.com/rest_api/docs/support/incremental_export#excluding-system-updates
            if event_timestamp >= bookmark:
                self.update_bookmark(state, event_timestamp.isoformat())
                should_yield = self._buffer_record((self.stream, event.to_dict()))

                if comments_stream.is_selected():
                    for comment in comments_stream.sync(event):
                        self._buffer_record(comment)

                if should_yield:
                    for rec in self._empty_buffer():
                        yield rec
                    emit_sub_stream_metrics(comments_stream)
                    singer.write_state(state)

        for rec in self._empty_buffer():
            yield rec
        emit_sub_stream_metrics(comments_stream)
        singer.write_state(state)

class TicketComments(Stream):
    name = "ticket_comments"
    replication_method = "INCREMENTAL"
    count = 0

    def sync(self, event):
        for child_event in event.child_events:
            if child_event.get('event_type') == 'Comment':
                self.count += 1
                child_event['ticket_id'] = event.ticket_id
                yield (self.stream, child_event)

class TicketMetricEvents(Stream):
    name = "ticket_metric_events"
    replication_method = "INCREMENTAL"
    key_properties = ['id']
    replication_key = 'time'

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        start_time = int(bookmark.timestamp())
        ticket_metric_events = self.client.ticket_metric_events(start_time=start_time)
        for event in ticket_metric_events:
            self.update_bookmark(state, event.time)
            yield (self.stream, event)

class SatisfactionRatings(Stream):
    name = "satisfaction_ratings"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        original_search_window_size = DEFAULT_SEARCH_WINDOW_SIZE
        search_window_size = original_search_window_size
        # We substract a second here because the API seems to compare
        # start_time with a >, but we typically prefer a >= behavior.
        # Also, the start_time query parameter filters based off of
        # created_at, but zendesk support confirmed with us that
        # satisfaction_ratings are immutable so that created_at =
        # updated_at
        start = bookmark - datetime.timedelta(seconds=1)
        end = start + datetime.timedelta(seconds=search_window_size)
        sync_end = singer.utils.now() - datetime.timedelta(minutes=1)
        epoch_sync_end = int(sync_end.strftime('%s'))
        parsed_sync_end = singer.strftime(sync_end, "%Y-%m-%dT%H:%M:%SZ")

        while start < sync_end:
            epoch_start = int(start.strftime('%s'))
            parsed_start = singer.strftime(start, "%Y-%m-%dT%H:%M:%SZ")
            epoch_end = int(end.strftime('%s'))
            parsed_end = singer.strftime(end, "%Y-%m-%dT%H:%M:%SZ")

            LOGGER.info("Querying for satisfaction ratings between %s and %s", parsed_start, min(parsed_end, parsed_sync_end))
            satisfaction_ratings = self.client.satisfaction_ratings(start_time=epoch_start,
                                                                    end_time=min(epoch_end, epoch_sync_end))
            # NB: We've observed that the tap can sync 50k records in ~15
            # minutes, due to this, the tap will adjust the time range
            # dynamically to ensure bookmarks are able to be written in
            # cases of high volume.
            if satisfaction_ratings.count > 50000:
                search_window_size = search_window_size // 2
                end = start + datetime.timedelta(seconds=search_window_size)
                LOGGER.info("satisfaction_ratings - Detected Search API response size for this window is too large (> 50k). Cutting search window in half to %s seconds.", search_window_size)
                continue
            for satisfaction_rating in satisfaction_ratings:
                assert parsed_start <= satisfaction_rating.updated_at, "satisfaction_ratings - Record found before date window start. Details: window start ({}) is not less than or equal to updated_at ({})".format(parsed_start, satisfaction_rating.updated_at)
                if bookmark < utils.strptime_with_tz(satisfaction_rating.updated_at) <= end:
                    # NB: We don't trust that the records come back ordered by
                    # updated_at (we've observed out-of-order records),
                    # so we can't save state until we've seen all records
                    self.update_bookmark(state, satisfaction_rating.updated_at)
                if parsed_start <= satisfaction_rating.updated_at <= parsed_end:
                    yield (self.stream, satisfaction_rating)
            if search_window_size <= original_search_window_size // 2:
                search_window_size = search_window_size * 2
                LOGGER.info("Successfully requested records. Doubling search window to %s seconds", search_window_size)
            singer.write_state(state)

            start = end - datetime.timedelta(seconds=1)
            end = start + datetime.timedelta(seconds=search_window_size)

class AgentsActivity(Stream):
    name = "agents_activity"
    replication_method = "FULL_TABLE" # This doesn't matter
    key_properties = ["sync_date", "agent_id"]

    def sync(self, state):
        # https://developer.zendesk.com/api-reference/voice/talk-api/stats/#list-agents-activity
        agents_activity = self.client.talk.agents_activity()

        # The API docs note that the timeframe of this data happens for:
        # "the current day from midnight in your account's timezone to the moment you make the request."
        # So mark the date as a local date given the account_timezone in the config
        # as we can't fetch the account information using the current client
        sync_date = datetime.datetime.utcnow()
        if self.config.get('account_timezone'):
            tz = pytz.timezone(self.config['account_timezone'])
            sync_date = sync_date.astimezone(tz)
            LOGGER.info(f'Agent activities sync_date is local aware, using date of: {sync_date}')
        else:
            LOGGER.info(f'Agent activities sync_date is not local aware (assuming UTC), using date of: {sync_date}')

        sync_date = str(sync_date.date())
        for agent_activity in agents_activity:
            agent_activity = agent_activity.to_dict()
            agent_activity["sync_date"] = sync_date
            yield (self.stream, agent_activity)


class Groups(Stream):
    name = "groups"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        groups = self.client.groups()
        for group in groups:
            if utils.strptime_with_tz(group.updated_at) >= bookmark:
                # NB: We don't trust that the records come back ordered by
                # updated_at (we've observed out-of-order records),
                # so we can't save state until we've seen all records
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
                # NB: We don't trust that the records come back ordered by
                # updated_at (we've observed out-of-order records),
                # so we can't save state until we've seen all records
                self.update_bookmark(state, macro.updated_at)
                yield (self.stream, macro)

class Tags(Stream):
    name = "tags"
    replication_method = "FULL_TABLE"
    key_properties = ["name"]

    def sync(self, state): # pylint: disable=unused-argument
        # NB: Setting page to force it to paginate all tags, instead of just the
        #     top 100 popular tags
        tags = self.client.tags(page=1)
        for tag in tags:
            yield (self.stream, tag)

class TicketFields(Stream):
    name = "ticket_fields"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        fields = self.client.ticket_fields()
        for field in fields:
            if utils.strptime_with_tz(field.updated_at) >= bookmark:
                # NB: We don't trust that the records come back ordered by
                # updated_at (we've observed out-of-order records),
                # so we can't save state until we've seen all records
                self.update_bookmark(state, field.updated_at)
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

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        memberships = self.client.group_memberships()
        for membership in memberships:
            # some group memberships come back without an updated_at
            if membership.updated_at:
                if utils.strptime_with_tz(membership.updated_at) >= bookmark:
                    # NB: We don't trust that the records come back ordered by
                    # updated_at (we've observed out-of-order records),
                    # so we can't save state until we've seen all records
                    self.update_bookmark(state, membership.updated_at)
                    yield (self.stream, membership)
            else:
                if membership.id:
                    LOGGER.info('group_membership record with id: ' + str(membership.id) +
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


class Calls(Stream):
    name = "calls"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def sync(self, state):
        # The incremental Talk endpoint isn't currently supported by the Zenpy
        # library, though there is an open PR for getting that in there:
        # https://github.com/facetoe/zenpy/pull/454
        # If/when that gets merged we can update, but for now we have this!

        bookmark = self.get_bookmark(state)
        bookmark = math.floor(bookmark.timestamp())
        next_page = f'https://{self.client.talk.subdomain}.zendesk.com/api/v2/channels/voice/stats/incremental/calls?start_time={bookmark}'
        count = 50

        # this endpoint will always return a value for next_page, so instead we
        # use the count property to determine if more items are available
        MINIMUM_REQUIRED_RESULT_SET_SIZE = 50

        while count >= MINIMUM_REQUIRED_RESULT_SET_SIZE:
            resp = self.client.talk._call_api(self.client.talk.session.get, next_page)
            result = resp.json()
            calls = result['calls']
            for call in calls:
                self.update_bookmark(state, call['updated_at'])
                yield (self.stream, call)

            next_page = result['next_page']
            count = result['count']

class CallLegs(Stream):
    name = "call_legs"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        bookmark = math.floor(bookmark.timestamp())
        next_page = f'https://{self.client.talk.subdomain}.zendesk.com/api/v2/channels/voice/stats/incremental/legs?start_time={bookmark}'
        count = 50

        # this endpoint will always return a value for next_page, so instead we
        # use the count property to determine if more items are available
        MINIMUM_REQUIRED_RESULT_SET_SIZE = 50

        while count >= MINIMUM_REQUIRED_RESULT_SET_SIZE:
            resp = self.client.talk._call_api(self.client.talk.session.get, next_page)
            result = resp.json()
            call_legs = result['legs']
            for leg in call_legs:
                self.update_bookmark(state, leg['updated_at'])
                yield (self.stream, leg)

            next_page = result['next_page']
            count = result['count']


STREAMS = {
    "tickets": Tickets,
    "groups": Groups,
    "users": Users,
    "organizations": Organizations,
    "ticket_audits": TicketAudits,
    "ticket_audit_events": TicketAuditEvents,
    "ticket_events": TicketEvents,
    "ticket_comments": TicketComments,
    "ticket_fields": TicketFields,
    "ticket_forms": TicketForms,
    "ticket_metric_events": TicketMetricEvents,
    "group_memberships": GroupMemberships,
    "macros": Macros,
    "satisfaction_ratings": SatisfactionRatings,
    "tags": Tags,
    "ticket_metrics": TicketMetrics,
    "sla_policies": SLAPolicies,
    "calls": Calls,
    "call_legs": CallLegs,
    "agents_activity": AgentsActivity
}
