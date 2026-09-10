from datetime import datetime, timezone
from zenpy.lib.exception import APIException
from tap_zendesk.streams.abstracts import (
    CursorBasedExportStream,
    ParentChildBookmarkMixin,
    process_custom_field,
    raise_or_log_zenpy_apiexception,
    CONCURRENCY_LIMIT,
    START_DATE_FORMAT
)
from tap_zendesk.exceptions import ZendeskNotFoundError

class Users(ParentChildBookmarkMixin, CursorBasedExportStream):
    name = "users"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    item_key = "users"
    endpoint = "incremental/users/cursor.json"
    children = ['user_identities', 'user_attribute_values']

    def sync(self, state, parent_obj=None):
        """
        Sync users, fetching the `user_identities` child stream concurrently
        in batches (one HTTP request per user otherwise dominates sync time).
        Other children continue to sync inline, per user, as before.
        """
        bookmark_date = self.get_bookmark(state, self.name)
        current_max_bookmark_date = bookmark_date
        epoch_bookmark = int(bookmark_date.timestamp())
        records = self.get_objects(epoch_bookmark)

        identities_stream = next(
            (child for child in self.child_to_sync if child.name == "user_identities"), None
        )
        other_children = [child for child in self.child_to_sync if child.name != "user_identities"]

        identities_batch = []
        for record in records:
            record = self.modify_object(record, parent_record=parent_obj)
            replication_datetime = self.get_replication_datetime(record)

            if replication_datetime < bookmark_date:
                continue

            current_max_bookmark_date = max(current_max_bookmark_date, replication_datetime)
            if self.is_selected():
                self.count += 1
                yield (self.stream, record)

            for child in other_children:
                yield from child.sync(state=state, parent_obj=record)
                self.emit_sub_stream_metrics(child)

            if identities_stream is not None and identities_stream.is_selected():
                identities_batch.append(record)
                if len(identities_batch) >= CONCURRENCY_LIMIT:
                    yield from identities_stream.sync_batch(state, identities_batch)
                    self.emit_sub_stream_metrics(identities_stream)
                    identities_batch = []

        if identities_batch:
            yield from identities_stream.sync_batch(state, identities_batch)
            self.emit_sub_stream_metrics(identities_stream)

        self.update_bookmark(state, self.name, current_max_bookmark_date)

    def _add_custom_fields(self, schema):
        try:
            field_gen = self.client.user_fields()
        except APIException as e:
            return raise_or_log_zenpy_apiexception(schema, self.name, e)
        schema['properties']['user_fields']['properties'] = {}
        for field in field_gen:
            schema['properties']['user_fields']['properties'][field.key] = process_custom_field(field)

        return schema

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # Convert datetime object to standard format with timezone. Used utcnow to reduce API call burden at discovery time.
        # Because API will return records from now which will be very less
        start_time = datetime.now(timezone.utc).strftime(START_DATE_FORMAT)
        self.client.search("", updated_after=start_time, updated_before='2000-01-02T00:00:00Z', type="user")


class UserSubStreamMixin:
    def check_access(self):
        """
        No-op because access is implicitly granted via parent stream.
        """
        return

    def get_stream_endpoint(self, **kwargs) -> str:
        """
        Build the full API URL using the user ID from parent object.
        """
        parent_record = kwargs.get("parent_obj", {})
        user_id = parent_record.get("id")
        if user_id:
            kwargs["user_id"] = user_id

        return super().get_stream_endpoint(**kwargs)

    def get_objects(self, **kwargs):
        try:
            yield from super().get_objects(**kwargs)
        except ZendeskNotFoundError:
            # User identities not found (unverified/deleted user)
            yield from []
