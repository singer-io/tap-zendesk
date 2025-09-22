from singer import utils
from tap_zendesk.streams.abstracts import (
    CursorBasedStream
)


class SatisfactionRatings(CursorBasedStream):
    name = "satisfaction_ratings"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'satisfaction_ratings'
    item_key = 'satisfaction_ratings'

    def update_params(self, **kwargs):
        """
        Overriding Update params for the stream
        """
        state = kwargs.get("state")
        bookmark = self.get_bookmark(state)
        epoch_bookmark = int(bookmark.timestamp())
        self.params.update({'start_time': epoch_bookmark})
