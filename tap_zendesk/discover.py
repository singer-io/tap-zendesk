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
            s.check_access()
        except ZendeskForbiddenError as e:
            error = e
            error_list.append(s.name)
        except zenpy.lib.exception.APIException as e:
            err = json.loads(e.args[0]).get('error')

            if isinstance(err, dict):
                if err.get('message', None) == "You do not have access to this page. Please contact the account owner of this help desk for further help.":
                    error_list.append(s.name)
                    error = e
            elif json.loads(e.args[0]).get('description') == "You are missing the following required scopes: read":
                error_list.append(s.name)
                error = e
            else:
                raise e

        streams.append({'stream': s.name, 'tap_stream_id': s.name, 'schema': schema, 'metadata': s.load_metadata()})

    if error_list:
        st = ", ".join(error_list)
        LOGGER.critical("The account credentials supplied do not have read access for the following stream(s):  %s", st)
        raise error


    return streams
