from tap_zendesk.streams.abstracts import (
    CursorBasedStream
)


class Tags(CursorBasedStream):
    name = "tags"
    replication_method = "FULL_TABLE"
    key_properties = ["name"]
    endpoint = 'tags'
    item_key = 'tags'
