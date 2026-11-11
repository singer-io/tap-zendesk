from zenpy.lib.exception import APIException

from tap_zendesk.streams.abstracts import (PaginatedStream,
                                           process_custom_field,
                                           raise_or_log_zenpy_apiexception)


class Organizations(PaginatedStream):
    name = "organizations"
    replication_method = "INCREMENTAL"
    replication_key = "updated_at"
    endpoint = 'organizations'
    item_key = 'organizations'
    pagination_type = "cursor"

    def update_params(self, **kwargs):
        """
        GET /api/v2/organizations has no start_time filter, unlike the deprecated
        incremental endpoint. Sort ascending by the replication key and rely on
        client-side bookmark filtering (handled by `process_records`) instead.
        """
        self.params = {'sort': 'updated_at'}

    def _add_custom_fields(self, schema):
        endpoint = self.client.organizations.endpoint
        # NB: Zenpy doesn't have a public endpoint for this at time of writing
        #     Calling into underlying query method to grab all fields
        try:
            field_gen = self.client.organizations._query_zendesk(endpoint.organization_fields, # pylint: disable=protected-access
                                                                 'organization_field')
        except APIException as e:
            return raise_or_log_zenpy_apiexception(schema, self.name, e)
        schema['properties']['organization_fields']['properties'] = {}
        for field in field_gen:
            schema['properties']['organization_fields']['properties'][field.key] = process_custom_field(field)

        return schema
