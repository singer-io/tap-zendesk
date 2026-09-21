from typing import Dict
from singer import utils
from tap_zendesk.streams.abstracts import (
    PaginatedStream,
    LOGGER
)

class GroupMemberships(PaginatedStream):
    name = "group_memberships"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'group_memberships'
    item_key = 'group_memberships'
    pagination_type = "cursor"

    def sync(self, state, parent_obj: Dict = None):
        bookmark = self.get_bookmark(state, self.name)
        memberships = self.get_objects()

        for membership in memberships:
            membership = self.modify_object(membership, parent_record=parent_obj)
            # some group memberships come back without an updated_at
            if membership['updated_at']:
                if utils.strptime_with_tz(membership['updated_at']) >= bookmark:
                    # NB: We don't trust that the records come back ordered by
                    # updated_at (we've observed out-of-order records),
                    # so we can't save state until we've seen all records
                    self.update_bookmark(state, self.name, membership['updated_at'])
                    yield (self.stream, membership)
            else:
                if membership['id']:
                    LOGGER.info('group_membership record with id: ' + str(membership['id']) +
                                ' does not have an updated_at field so it will be syncd...')
                    yield (self.stream, membership)
                else:
                    LOGGER.info('Received group_membership record with no id or updated_at, skipping...')
