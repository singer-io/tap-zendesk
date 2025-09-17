import datetime
import singer
from zenpy.lib.exception import APIException
from tap_zendesk.streams.abstracts import (
    CursorBasedExportStream,
    process_custom_field,
    raise_or_log_zenpy_apiexception,
    START_DATE_FORMAT
)


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
