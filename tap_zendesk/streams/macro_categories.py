from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class MacroCategories(PaginatedStream):
    name = "macro_categories"
    replication_method = "FULL_TABLE"
    key_properties = ["name"]
    endpoint = 'macros/categories'
    item_key = 'categories'
    pagination_type = "offset"

    def modify_object(self, record, **_kwargs):
        """
        Overriding modify_record to add `name` key in recordssss
        """
        record_dict = {
            "name": record
        }
        return record_dict
