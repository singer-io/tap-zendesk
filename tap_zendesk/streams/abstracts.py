import os
import json
from typing import Dict, Any
from urllib.parse import urljoin
from zenpy.lib.exception import APIException
import singer
from singer import (
    metadata,
    utils
)
from singer.metrics import Point
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
    parent = ""
    children = []
    count = 0

    def __init__(self, client=None, config=None):
        self.client = client
        self.config = config
        self.metadata = None
        self.params = {}
        self.child_to_sync = []
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
        return utils.strptime_with_tz(
            singer.get_bookmark(
                state, self.name,
                self.replication_key,
                self.config["start_date"]
            )
        )

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
        url = self.get_stream_endpoint()
        HEADERS['Authorization'] = 'Bearer {}'.format(self.config["access_token"])

        http.call_api(url, self.request_timeout, params={'per_page': 1}, headers=HEADERS)

    def update_params(self, **kwargs) -> None:  # pylint: disable=unused-argument
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
            raise ValueError(f"Missing required placeholder in config: {e}") from e
        return urljoin(base_url + '/', endpoint_path.lstrip('/'))

    def modify_object(self, record: Dict, **_kwargs) -> Dict:
        """
        Modify the record before writing to the stream
        """
        return record

    def get_nested_value(self, data, key_path, default=None):
        """
        Recursively get a value from nested dicts using dot-separated key path.
        """
        keys = key_path.split(".")
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key, default)
            else:
                return default
        return data

    def emit_sub_stream_metrics(self, sub_stream):
        if sub_stream.is_selected():
            singer.metrics.log(LOGGER, Point(metric_type='counter',
                                                metric=singer.metrics.Metric.record_count,
                                                value=sub_stream.count,
                                                tags={'endpoint':sub_stream.stream.tap_stream_id}))
            sub_stream.count = 0

class PaginatedStream(Stream):
    pagination_type = None
    item_key = None
    endpoint = None

    def get_objects(self, **kwargs):
        parent_obj = kwargs.get('parent_obj', {})
        url = self.get_stream_endpoint(parent_obj=parent_obj)

        pagination_methods = {
            "cursor": http.get_cursor_based,
            "offset": http.get_offset_based,
        }

        pagination_func = pagination_methods.get(self.pagination_type)
        if pagination_func is None:
            raise ValueError(f"Unsupported pagination type: {self.pagination_type}")

        pages = pagination_func(
            url,
            self.config['access_token'],
            self.request_timeout,
            self.page_size,
            **kwargs
        )

        for page in pages:
            raw_records = self.get_nested_value(page, self.item_key, [])
            if isinstance(raw_records, dict):
                yield from [raw_records]

            elif isinstance(raw_records, list):
                yield from raw_records

            else:
                yield from []

    def sync(self, state: Dict, parent_obj: Dict = None):
        """
        Implementation for `type: Paginated` stream.
        """
        bookmark_date = self.get_bookmark(state)
        current_max_bookmark_date = bookmark_date
        self.update_params(state=state)

        for record in self.get_objects(params=self.params, parent_obj=parent_obj):
            record = self.modify_object(record, parent_record=parent_obj)
            if self.replication_method == "INCREMENTAL":
                replication_value = record.get(self.replication_key)
                if replication_value is None:
                    raise ValueError(
                        f"Record has missing replication key '{self.replication_key}': {record}"
                    )

                replication_datetime = (
                    utils.strptime_with_tz(replication_value)
                    if isinstance(replication_value, str)
                    else replication_value
                )

                if replication_datetime >= bookmark_date:
                    current_max_bookmark_date = max(
                        current_max_bookmark_date, replication_datetime
                    )
                    if self.is_selected():
                        self.count += 1
                        yield (self.stream, record)
                    self.update_bookmark(state, current_max_bookmark_date.isoformat())
            elif self.replication_method == "FULL_TABLE":
                if self.is_selected():
                    self.count += 1
                    yield (self.stream, record)
            else:
                raise ValueError(f"Unknown replication method: {self.replication_method}")

            for child in self.child_to_sync:
                yield from child.sync(state=state, parent_obj=record)
                self.emit_sub_stream_metrics(child)

class CursorBasedExportStream(Stream):
    endpoint = None
    item_key = None

    def get_objects(self, start_time, side_load=None):
        '''
        Retrieve objects from the incremental exports endpoint using cursor based pagination
        '''
        url = self.get_stream_endpoint()
        for page in http.get_incremental_export(url, self.config['access_token'], self.request_timeout, start_time, side_load):
            yield from page[self.item_key]

    def sync(self, state, parent_obj: Dict = None):
        bookmark_date = self.get_bookmark(state)
        current_max_bookmark_date = bookmark_date
        epoch_bookmark = int(bookmark_date.timestamp())
        records = self.get_objects(epoch_bookmark)

        for record in records:
            record = self.modify_object(record, parent_record=parent_obj)
            if self.replication_method == "INCREMENTAL":
                replication_value = record.get(self.replication_key)
                if replication_value is None:
                    raise ValueError(
                        f"Record has missing replication key '{self.replication_key}': {record}"
                    )
                replication_datetime = (
                    utils.strptime_with_tz(replication_value)
                    if isinstance(replication_value, str)
                    else replication_value
                )
                current_max_bookmark_date = max(
                        current_max_bookmark_date, replication_datetime
                    )
                if self.is_selected():
                    yield (self.stream, record)
                self.update_bookmark(state, current_max_bookmark_date.isoformat())
            elif self.replication_method == "FULL_TABLE":
                if self.is_selected():
                    yield (self.stream, record)
            else:
                raise ValueError(f"Unknown replication method: {self.replication_method}")

            for child in self.child_to_sync:
                yield from child.sync(state=state, parent_obj=record)
                self.emit_sub_stream_metrics(child)

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

class ParentChildBookmarkMixin:
    """
    Mixin to extend bookmark handling for streams with child streams.
    """
    def get_bookmark(self, state: Dict) -> int:
        """
        Get the minimum bookmark value among the parent and its incremental children,
        excluding full-table replication children.
        """
        min_parent_bookmark = super().get_bookmark(state) if self.is_selected() else ""

        for child in self.child_to_sync:
            if not child.is_selected():
                continue
            if getattr(child, "replication_method", "").upper() == "FULL_TABLE":
                continue

            bookmark_key = f"{self.name}_{self.replication_key}"
            child_bookmark = super().get_bookmark(state, child.name, key=bookmark_key)

            if min_parent_bookmark:
                min_parent_bookmark = min(min_parent_bookmark, child_bookmark)
            else:
                min_parent_bookmark = child_bookmark

        return min_parent_bookmark

    def write_bookmark(self, state: Dict, stream: str, value: Any = None) -> Dict:
        """
        Write the bookmark value to the parent and all incremental children.
        """
        if self.is_selected():
            super().write_bookmark(state, stream, value=value)

        for child in self.child_to_sync:
            if not child.is_selected():
                continue
            if getattr(child, "replication_method", "").upper() == "FULL_TABLE":
                continue

            bookmark_key = f"{self.tap_stream_id}_{self.replication_key}"
            super().write_bookmark(state, child.tap_stream_id, key=bookmark_key, value=value)

        return state
