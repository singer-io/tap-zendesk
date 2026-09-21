from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class MacroDefinitions(PaginatedStream):
    name = "macro_definitions"
    replication_method = "FULL_TABLE"
    key_properties = ["subject"]
    endpoint = 'macros/definitions'
    item_key = 'definitions.actions'
    pagination_type = "offset"
