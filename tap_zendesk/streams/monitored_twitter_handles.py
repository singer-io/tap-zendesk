from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class MonitoredTwitterHandles(PaginatedStream):
    name = "monitored_twitter_handles"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    key_properties = ["id"]
    endpoint = 'channels/twitter/monitored_twitter_handles'
    item_key = 'monitored_twitter_handles'
    pagination_type = "offset"
