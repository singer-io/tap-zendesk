from zenpy.lib.exception import APIException
from tap_zendesk.streams.abstracts import Stream, raise_forbidden_if_access_denied


class SLAPolicies(Stream):
    name = "sla_policies"
    replication_method = "FULL_TABLE"
    is_optional = True

    def sync(self, state): # pylint: disable=unused-argument
        for policy in self.client.sla_policies():
            yield (self.stream, policy)

    def check_access(self):
        '''
        Check whether the permission was given to access stream resources or not.
        '''
        try:
            self.client.sla_policies()
        except APIException as e:
            raise_forbidden_if_access_denied(e)
