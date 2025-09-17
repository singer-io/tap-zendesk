from singer import utils
from tap_zendesk.streams.abstracts import (
    CursorBasedStream
)


class SatisfactionRatings(CursorBasedStream):
    name = "satisfaction_ratings"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/satisfaction_ratings'
    item_key = 'satisfaction_ratings'

    def sync(self, state):
        bookmark = self.get_bookmark(state)
        epoch_bookmark = int(bookmark.timestamp())
        params = {'start_time': epoch_bookmark}
        ratings = self.get_objects(params=params)
        for rating in ratings:
            if utils.strptime_with_tz(rating['updated_at']) >= bookmark:
                self.update_bookmark(state, rating['updated_at'])
                yield (self.stream, rating)
