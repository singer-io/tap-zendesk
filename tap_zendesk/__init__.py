#!/usr/bin/env python3
import json
import sys
import requests
from requests.adapters import HTTPAdapter
from zenpy import Zenpy
from zenpy.lib.exception import ZenpyException
import singer
from singer import metadata
from tap_zendesk.discover import discover_streams
from tap_zendesk.sync import sync_stream
from tap_zendesk.streams import STREAMS
from tap_zendesk import metrics as zendesk_metrics

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "subdomain",
]

# default authentication
OAUTH_CONFIG_KEYS = [
    "access_token",
]

# email + api_token authentication
API_TOKEN_CONFIG_KEYS = [
    "email",
    "api_token",
]

def do_discover(client):
    LOGGER.info("Starting discover")
    catalog = {"streams": discover_streams(client)}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")

def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)

def get_selected_streams(catalog):
    selected_stream_names = []
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        if stream_is_selected(mdata):
            selected_stream_names.append(stream.tap_stream_id)
    return selected_stream_names


SUB_STREAMS = {
    'tickets': ['ticket_metrics'],
    'ticket_events': ['ticket_comments'],
    'ticket_audits': ['ticket_audit_events'],
}

def get_sub_stream_names():
    sub_stream_names = []
    for parent_stream in SUB_STREAMS:
        sub_stream_names.extend(SUB_STREAMS[parent_stream])
    return sub_stream_names

class DependencyException(Exception):
    pass

def validate_dependencies(selected_stream_ids):
    errs = []
    msg_tmpl = ("Unable to extract {0} data. "
                "To receive {0} data, you also need to select {1}.")
    for parent_stream_name in SUB_STREAMS:
        sub_stream_names = SUB_STREAMS[parent_stream_name]
        for sub_stream_name in sub_stream_names:
            if sub_stream_name in selected_stream_ids and parent_stream_name not in selected_stream_ids:
                errs.append(msg_tmpl.format(sub_stream_name, parent_stream_name))

    if errs:
        raise DependencyException(" ".join(errs))

def populate_class_schemas(catalog, selected_stream_names):
    for stream in catalog.streams:
        if stream.tap_stream_id in selected_stream_names:
            STREAMS[stream.tap_stream_id].stream = stream

def do_sync(client, config, catalog, state):
    selected_stream_names = get_selected_streams(catalog)
    validate_dependencies(selected_stream_names)
    populate_class_schemas(catalog, selected_stream_names)
    all_sub_stream_names = get_sub_stream_names()

    sync_errors = 0

    for stream in catalog.streams:
        stream_name = stream.tap_stream_id
        mdata = metadata.to_map(stream.metadata)
        if stream_name not in selected_stream_names:
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        # if starting_stream:
        #     if starting_stream == stream_name:
        #         LOGGER.info("%s: Resuming", stream_name)
        #         starting_stream = None
        #     else:
        #         LOGGER.info("%s: Skipping - already synced", stream_name)
        #         continue
        # else:
        #     LOGGER.info("%s: Starting", stream_name)


        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream.schema.to_dict(), key_properties)

        sub_stream_names = SUB_STREAMS.get(stream_name)
        if sub_stream_names:
            for sub_stream_name in sub_stream_names:
                if sub_stream_name not in selected_stream_names:
                    continue
                sub_stream = STREAMS[sub_stream_name].stream
                sub_mdata = metadata.to_map(sub_stream.metadata)
                sub_key_properties = metadata.get(sub_mdata, (), 'table-key-properties')
                singer.write_schema(sub_stream.tap_stream_id, sub_stream.schema.to_dict(), sub_key_properties)

        # parent stream will sync sub stream
        if stream_name in all_sub_stream_names:
            continue

        LOGGER.info("%s: Starting sync", stream_name)
        instance = STREAMS[stream_name](client, config)
        try:
            counter_value = sync_stream(state, instance)
            singer.write_state(state)
            LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)
            zendesk_metrics.log_aggregate_rates()
        except Exception:
            # Don't allow an error in one stream prevent other streams
            # from running
            sync_errors += 1
            LOGGER.exception("%s: Sync failed", stream_name)

    singer.write_state(state)
    LOGGER.info("Finished sync")
    zendesk_metrics.log_aggregate_rates()

    sys.exit(sync_errors)

def oauth_auth(args):
    if not set(OAUTH_CONFIG_KEYS).issubset(args.config.keys()):
        LOGGER.debug("OAuth authentication unavailable.")
        return None

    LOGGER.info("Using OAuth authentication.")
    return {
        "subdomain": args.config['subdomain'],
        "oauth_token": args.config['access_token'],
    }

def api_token_auth(args):
    if not set(API_TOKEN_CONFIG_KEYS).issubset(args.config.keys()):
        LOGGER.debug("API Token authentication unavailable.")
        return None

    LOGGER.info("Using API Token authentication.")
    return {
        "subdomain": args.config['subdomain'],
        "email": args.config['email'],
        "token": args.config['api_token']
    }

def get_session(config):
    """ Add partner information to requests Session object if specified in the config. """
    if not all(k in config for k in ["marketplace_name",
                                     "marketplace_organization_id",
                                     "marketplace_app_id"]):
        return None
    session = requests.Session()
    # Using Zenpy's default adapter args, following the method outlined here:
    # https://github.com/facetoe/zenpy/blob/master/docs/zenpy.rst#usage
    session.mount("https://", HTTPAdapter(**Zenpy.http_adapter_kwargs()))
    session.headers["X-Zendesk-Marketplace-Name"] = config.get("marketplace_name", "")
    session.headers["X-Zendesk-Marketplace-Organization-Id"] = str(config.get("marketplace_organization_id", ""))
    session.headers["X-Zendesk-Marketplace-App-Id"] = str(config.get("marketplace_app_id", ""))
    return session

@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    # OAuth has precedence
    creds = oauth_auth(parsed_args) or api_token_auth(parsed_args)
    session = get_session(parsed_args.config)

    # Pass some config options into the client
    rate_limit_settings = {}
    for key in ('proactive_ratelimit', 'proactive_ratelimit_request_interval', 'ratelimit_budget'):
        if key in parsed_args.config:
            rate_limit_settings[key] = parsed_args.config.pop(key)
    LOGGER.info('zenpy rate limit settings = %r', rate_limit_settings)
    client = Zenpy(session=session, **rate_limit_settings, **creds)

    if not client:
        LOGGER.error("""No suitable authentication keys provided.""")

    if parsed_args.discover:
        do_discover(client)
    elif parsed_args.catalog:
        state = parsed_args.state
        
        filtered_config = {
            k: v for k, v in parsed_args.config.items()
            if k not in OAUTH_CONFIG_KEYS + API_TOKEN_CONFIG_KEYS
        }
        do_sync(client, filtered_config, parsed_args.catalog, state)
