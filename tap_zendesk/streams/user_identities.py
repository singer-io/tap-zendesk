from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class UserIdentities(PaginatedStream):
    name = "user_identities"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'users/{user_id}/identities'
    item_key = 'identities'
    pagination_type = "cursor"
    parent = "users"

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
        user_id = parent_record.get("id", None)
        if user_id:
            kwargs["user_id"] = user_id

        return super().get_stream_endpoint(**kwargs)
