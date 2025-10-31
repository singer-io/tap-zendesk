from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class SatisfactionRatings(PaginatedStream):
    name = "satisfaction_ratings"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'satisfaction_ratings'
    item_key = 'satisfaction_ratings'
    pagination_type = "cursor"

    def update_params(self, **kwargs):
        """
        Overriding Update params for the stream
        """
        state = kwargs.get("state")
        bookmark = self.get_bookmark(state, self.name)
        epoch_bookmark = int(bookmark.timestamp())
        self.params.update({'start_time': epoch_bookmark})
