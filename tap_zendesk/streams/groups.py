from singer import utils
from tap_zendesk.streams.abstracts import (
    CursorBasedStream
)


class Groups(CursorBasedStream):
    name = "groups"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'groups'
    item_key = 'groups'
