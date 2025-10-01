from tap_zendesk.streams.abstracts import (
    PaginatedStream
)

class ScheduleHolidays(PaginatedStream):
    name = "schedule_holidays"
    replication_method = "INCREMENTAL"
    replication_key = "start_date"
    key_properties = ["id"]
    endpoint = 'business_hours/schedules/{schedule_id}/holidays'
    item_key = 'schedules'
    pagination_type = "offset"
    parent = 'schedules'

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # We load metrics as side load of triggers, so we don't need to check access
        return

    def get_stream_endpoint(self, **kwargs) -> str:
        """
        Build the full API URL by joining the static BASE_URL and dynamic endpoint
        """
        parent_record = kwargs.get("parent_obj", {})
        schedule_id = parent_record.get("id", None)
        if schedule_id:
            kwargs["schedule_id"] = schedule_id

        return super().get_stream_endpoint(**kwargs)

    def modify_object(self, record, **_kwargs):
        """
        Overriding modify_record to add `parent's id` key in records
        """
        parent_obj = _kwargs.get("parent_record", {})
        schedule_id = parent_obj.get("id")
        record['schedule_id'] = schedule_id
        return record
