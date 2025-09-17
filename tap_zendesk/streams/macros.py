from singer import utils
from tap_zendesk.streams.abstracts import (
    CursorBasedStream
)


class Macros(CursorBasedStream):
    name = "macros"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'https://{}.zendesk.com/api/v2/macros'
    item_key = 'macros'

    def sync(self, state):
        bookmark = self.get_bookmark(state)

        macros = self.get_objects()
        for macro in macros:
            if utils.strptime_with_tz(macro['updated_at']) >= bookmark:
                # NB: We don't trust that the records come back ordered by
                # updated_at (we've observed out-of-order records),
                # so we can't save state until we've seen all records
                self.update_bookmark(state, macro['updated_at'])
                yield (self.stream, macro)
