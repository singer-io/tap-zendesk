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
        with open(os.path.join(shared_schemas_path, shared_file)) as data_file:
            shared_schema_refs[ref_sub_path + '/' + shared_file] = json.load(data_file)

    return shared_schema_refs

def discover_streams(client, config):
    streams = []
    error_list = []
    refs = load_shared_schema_refs()


    for s in STREAMS.values():
        s = s(client, config)
        schema = singer.resolve_schema_references(s.load_schema(), refs)
        try:
            # Here it call the check_access method to check whether stream have read permission or not.
            # If stream does not have read permission then append that stream name to list and at the end of all streams
            # raise forbidden error with proper message containinn stream names.
            s.check_access()
        except ZendeskForbiddenError as e:
            error_list.append(s.name) # Append stream name to the
        except zenpy.lib.exception.APIException as e:
            err = json.loads(e.args[0]).get('error')

            if isinstance(err, dict):
                if err.get('message', None) == "You do not have access to this page. Please contact the account owner of this help desk for further help.":
                    error_list.append(s.name)
            elif json.loads(e.args[0]).get('description') == "You are missing the following required scopes: read":
                error_list.append(s.name)
            else:
                raise e from None # raise error if it is other than 403 forbidden error

        streams.append({'stream': s.name, 'tap_stream_id': s.name, 'schema': schema, 'metadata': s.load_metadata()})

    if error_list:
        streams_name = ", ".join(error_list)
        message = "HTTP-error-code: 403, Error: You are missing the following required scopes: read. "\
                    "The account credentials supplied do not have read access for the following stream(s):  {}".format(streams_name)
        raise ZendeskForbiddenError(message)


    return streams
