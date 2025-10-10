from singer import utils
from tap_zendesk.streams.abstracts import Stream


class TicketForms(Stream):
    name = "ticket_forms"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"

    def sync(self, state):
        bookmark = self.get_bookmark(state, self.name)

        forms = self.client.ticket_forms()
        for form in forms:
            if utils.strptime_with_tz(form.updated_at) >= bookmark:
                # NB: We don't trust that the records come back ordered by
                # updated_at (we've observed out-of-order records),
                # so we can't save state until we've seen all records
                self.update_bookmark(state, self.name, form.updated_at)
                yield (self.stream, form)

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        self.client.ticket_forms()
