import os
import json
import singer
import zenpy
from tap_zendesk.streams import STREAMS
from tap_zendesk.http import ZendeskForbiddenError

LOGGER = singer.get_logger()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_shared_schema_refs():
    ref_sub_path = 'shared'
    shared_schemas_path = get_abs_path('schemas/' + ref_sub_path)

    shared_file_names = [f for f in os.listdir(shared_schemas_path)
                         if os.path.isfile(os.path.join(shared_schemas_path, f))]

    shared_schema_refs = {}
    for shared_file in shared_file_names:
        with open(os.path.join(shared_schemas_path, shared_file), encoding='UTF-8') as data_file:
            shared_schema_refs[ref_sub_path + '/' + shared_file] = json.load(data_file)

    return shared_schema_refs

def discover_streams(client, config):
    streams = []
    error_list = []
    refs = load_shared_schema_refs()

    for stream in STREAMS.values():
        # for each stream in the `STREAMS` check if the user has the permission to access the data of that stream
        stream = stream(client, config)
        schema = singer.resolve_schema_references(stream.load_schema(), refs)
        try:
            # Call check_access to verify the account has read permission for this stream.
            stream.check_access()
        except ZendeskForbiddenError:
            if stream.is_optional:
                # This stream depends on a plan tier or paid add-on that is not available
                # for this account (e.g. Talk, SLA Policies, Ticket Forms, Satisfaction Ratings).
                # Per Option B: exclude it from the catalog so it does not appear in the
                # stream selection list, rather than failing the connection.
                LOGGER.warning(
                    "Stream '%s' is not available for this account (plan tier or add-on not "
                    "provisioned). It will be excluded from the available streams.", stream.name)
                continue  # Do NOT append to streams list
            # Essential stream: collect name; reported at the end of discovery.
            error_list.append(stream.name)
        except zenpy.lib.exception.APIException as e:
            args0 = json.loads(e.args[0])
            err = args0.get('error')

            # check if the error is of type dictionary and the message retrieved from the dictionary
            # is the expected message. If so, only then print the logger message and return the schema
            if isinstance(err, dict):
                if err.get('message', None) == "Access to this resource is restricted. Please contact the account administrator for assistance.":
                    error_list.append(stream.name)
            elif args0.get('description') == "Missing the following required scopes: read":
                error_list.append(stream.name)
            else:
                raise e from None # raise error if it is other than 403 forbidden error

        streams.append({'stream': stream.name, 'tap_stream_id': stream.name, 'schema': schema, 'metadata': stream.load_metadata()})

    if error_list:
        # Use only essential (non-optional) streams as the threshold for the hard-fail check,
        # since optional streams are excluded from the catalog rather than added to error_list.
        total_essential_streams = sum(1 for s in STREAMS.values() if not s.is_optional)
        streams_name = ", ".join(error_list)
        if len(error_list) != total_essential_streams:
            message = "The account credentials supplied do not have 'read' access to the following stream(s): {}. "\
                "The data for these streams would not be collected due to lack of required permission.".format(streams_name)
            # If at least one essential stream has read permission, warn about the others.
            LOGGER.warning(message)
        else:
            message = "HTTP-error-code: 403, Error: The account credentials supplied do not have 'read' access to any "\
                "of streams supported by the tap. Data collection cannot be initiated due to lack of permissions."
            # If none of the essential streams are accessible, fail the connection.
            raise ZendeskForbiddenError(message)

    return streams
