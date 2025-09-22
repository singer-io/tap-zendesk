import os
import json
from urllib.parse import urljoin
from zenpy.lib.exception import APIException
import singer
from singer import metadata
from singer import utils
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
        self.metadata = None
        self.params = {}
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
        schema_file = os.path.join("..", "schemas", f"{self.name}.json")
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
        self.metadata = metadata.to_map(self.stream.metadata)
        return metadata.get(self.metadata, (), "selected")

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        url = self.endpoint.format(self.config['subdomain'])
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config["access_token"])

        http.call_api(url, self.request_timeout, params={'per_page': 1}, headers=HEADERS)

    def update_params(self, **kwargs) -> None:
        """
        Update params for the stream
        """
        self.params = {}    # Reset before each sync

    def get_stream_endpoint(self, **kwargs) -> str:
        """
        Build the full API URL by joining the static BASE_URL and dynamic endpoint
        """
        format_values = self.config.copy()
        try:
            base_url = http.BASE_URL.format(**format_values)
            endpoint_path = self.endpoint.format(**kwargs)
        except KeyError as e:
            raise ValueError(f"Missing required placeholder in config: {e}")
        return urljoin(base_url + '/', endpoint_path.lstrip('/'))


class CursorBasedStream(Stream):
    item_key = None
    endpoint = None

    def get_objects(self, **kwargs):
        '''
        Cursor based object retrieval
        '''
        url = self.get_stream_endpoint()
        # Pass `request_timeout` parameter
        for page in http.get_cursor_based(url, self.config['access_token'], self.request_timeout, self.page_size, **kwargs):
            yield from page[self.item_key]

    def sync(self, state):
        """
        Implementation for `type: CursorBasedStream` stream.
        """
        self.update_params(state=state)
        records = self.get_objects(params=self.params)

        for record in records:
            if self.replication_method == "INCREMENTAL":
                replication_value = record.get(self.replication_key)
                if replication_value is None:
                    raise ValueError(
                        f"Record has missing replication key '{self.replication_key}': {record}"
                    )
                bookmark = self.get_bookmark(state)
                if utils.strptime_with_tz(replication_value) >= bookmark:
                    self.update_bookmark(state, replication_value)
                    yield (self.stream, record)
            elif self.replication_method == "FULL_TABLE":
                yield (self.stream, record)
            else:
                raise ValueError(f"Unknown replication method: {self.replication_method}")


class CursorBasedExportStream(Stream):
    endpoint = None
    item_key = None

    def get_objects(self, start_time, side_load=None):
        '''
        Retrieve objects from the incremental exports endpoint using cursor based pagination
        '''
        url = self.get_stream_endpoint()
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
