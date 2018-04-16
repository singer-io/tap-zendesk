#!/usr/bin/env python3
import singer

LOGGER = singer.get_logger()

@singer.utils.handle_top_exception(LOGGER)
def main():
    print('test')
