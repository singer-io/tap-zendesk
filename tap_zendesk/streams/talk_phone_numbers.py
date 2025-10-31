from tap_zendesk.exceptions import ZendeskNotFoundError
from tap_zendesk.streams.abstracts import Stream


class TalkPhoneNumbers(Stream):
    name = 'talk_phone_numbers'
    replication_method = "FULL_TABLE"

    def sync(self, state): # pylint: disable=unused-argument
        for phone_number in self.client.talk.phone_numbers():
            yield (self.stream, phone_number)

    def check_access(self):
        try:
            self.client.talk.phone_numbers()
        except ZendeskNotFoundError:
            #Skip 404 ZendeskNotFoundError error as goal is to just check to whether TicketComments have read permission or not
            pass
