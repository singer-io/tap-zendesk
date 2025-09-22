from singer import utils
from tap_zendesk.streams.abstracts import (
    CursorBasedStream
)

class Macros(CursorBasedStream):
    name = "macros"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'macros'
    item_key = 'macros'
