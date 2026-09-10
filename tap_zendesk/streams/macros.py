from tap_zendesk.streams.abstracts import (
    PaginatedStream,
    ParentChildBookmarkMixin
)

class Macros(ParentChildBookmarkMixin, PaginatedStream):
    name = "macros"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'macros'
    item_key = 'macros'
    pagination_type = "cursor"
    children = ['macro_attachments']
