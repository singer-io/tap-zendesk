from tap_zendesk.streams.abstracts import (
    CursorBasedStream
)


class Tags(CursorBasedStream):
    name = "tags"
    replication_method = "FULL_TABLE"
    key_properties = ["name"]
    endpoint = 'https://{}.zendesk.com/api/v2/tags'
    item_key = 'tags'

    def sync(self, state): # pylint: disable=unused-argument
        tags = self.get_objects()

        for tag in tags:
            yield (self.stream, tag)
