from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class TriggerRevisions(PaginatedStream):
    name = "trigger_revisions"
    replication_method = "INCREMENTAL"
    replication_key = "created_at"
    key_properties = ["id", "trigger_id"]
    endpoint = 'triggers/{trigger_id}/revisions'
    item_key = 'trigger_revisions'
    pagination_type = "offset"
    parent = "triggers"

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # We load metrics as side load of triggers, so we don't need to check access
        return

    def get_stream_endpoint(self, **kwargs) -> str:
        """
        Build the full API URL by joining the static BASE_URL and dynamic endpoint
        """
        parent_record = kwargs.get("parent_obj", {})
        trigger_id = parent_record.get("id", None)
        if trigger_id:
            kwargs["trigger_id"] = trigger_id

        return super().get_stream_endpoint(**kwargs)

    def modify_object(self, record, **_kwargs):
        """
        Overriding modify_record to add `parent's id` key in records
        """
        parent_obj = _kwargs.get("parent_record", {})
        trigger_id = parent_obj.get("id")
        record['trigger_id'] = trigger_id
        return record
