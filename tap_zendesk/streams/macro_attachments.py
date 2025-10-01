from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class MacroAttachments(PaginatedStream):
    name = "macro_attachments"
    replication_method = "FULL_TABLE"
    key_properties = ["id"]
    endpoint = 'macros/{macro_id}/attachments'
    item_key = 'actions'
    pagination_type = "offset"
    parent = 'macros'

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
        macro_id = parent_record.get("id", None)
        if macro_id:
            kwargs["macro_id"] = macro_id

        return super().get_stream_endpoint(**kwargs)

    def modify_object(self, record, **_kwargs):
        """
        Overriding modify_record to add `parent's id` key in records
        """
        parent_obj = _kwargs.get("parent_record", {})
        macro_id = parent_obj.get("id")
        record['macro_id'] = macro_id
        return record
