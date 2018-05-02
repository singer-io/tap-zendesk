#!/usr/bin/env python3
import json
import sys
import singer

from singer import metadata
from zenpy import Zenpy
from tap_zendesk.discover import discover_streams
from tap_zendesk.sync import sync_stream

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "subdomain",
    "access_token"
]

def do_discover(client):
    LOGGER.info("Starting discover")
    catalog = {"streams": discover_streams(client)}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")

def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)

def do_sync(client, catalog, state, start_date):

    for stream in catalog.streams:
        stream_name = stream.tap_stream_id
        mdata = metadata.to_map(stream.metadata)
        if not stream_is_selected(mdata):
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

        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream.schema.to_dict(), key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(client, state, start_date, stream.to_dict())
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    singer.write_state(state)
    LOGGER.info("Finished sync")


@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    creds = {
        "subdomain": parsed_args.config['subdomain'],
        "oauth_token": parsed_args.config['access_token'],
    }
    client = Zenpy(**creds)

    if parsed_args.discover:
        do_discover(client)
    elif parsed_args.catalog:
        state = parsed_args.state
        do_sync(client, parsed_args.catalog, state, parsed_args.config['start_date'])
