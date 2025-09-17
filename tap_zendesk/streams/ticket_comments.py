from tap_zendesk.streams.abstracts import Stream


class TicketComments(Stream):
    name = "ticket_comments"
    replication_method = "INCREMENTAL"
    count = 0

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        # We load comments as side load of ticket_audits, so we don't need to check access
        return
