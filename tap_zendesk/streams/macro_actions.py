from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class MacroActions(PaginatedStream):
    name = "macro_actions"
    replication_method = "FULL_TABLE"
    key_properties = ["field"]
    endpoint = 'macros/actions'
    item_key = 'actions'
    pagination_type = "offset"
