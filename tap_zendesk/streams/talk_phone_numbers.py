import requests
from zenpy.lib.exception import APIException
from tap_zendesk.exceptions import ZendeskNotFoundError, ZendeskForbiddenError
from tap_zendesk.streams.abstracts import Stream, raise_forbidden_if_access_denied


class TalkPhoneNumbers(Stream):
    name = 'talk_phone_numbers'
    replication_method = "FULL_TABLE"
    is_optional = True

    def sync(self, state): # pylint: disable=unused-argument
        for phone_number in self.client.talk.phone_numbers():
            yield (self.stream, phone_number)

    def check_access(self):
        try:
            self.client.talk.phone_numbers()
        except ZendeskNotFoundError:
            # Skip 404 as goal is to check whether TalkPhoneNumbers have read permission
            pass
        except requests.exceptions.HTTPError as e:
            # Zenpy's Talk API raises requests.HTTPError directly for certain HTTP errors.
            # Convert 403 Forbidden to ZendeskForbiddenError for consistent handling in discover.py.
            if e.response is not None and e.response.status_code == 403:
                raise ZendeskForbiddenError(str(e)) from None
            raise
        except APIException as e:
            raise_forbidden_if_access_denied(e)
