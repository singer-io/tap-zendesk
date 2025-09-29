from tap_zendesk.streams.abstracts import (
    PaginatedStream
)


class SideConversations(PaginatedStream):
    name = "side_conversations"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'tickets/{ticket_id}/side_conversations'
    item_key = 'side_conversations'
    pagination_type = "offset"
    parent = 'tickets'

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
        ticket_id = parent_record.get("id", None)
        if ticket_id:
            kwargs["ticket_id"] = ticket_id

        return super().get_stream_endpoint(**kwargs)
