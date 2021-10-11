from time import sleep
import backoff
import requests
import singer


LOGGER = singer.get_logger()


def is_fatal(exception):
    status_code = exception.response.status_code

    if status_code == 429:
        sleep_time = int(exception.response.headers['Retry-After'])
        LOGGER.info("Caught HTTP 429, retrying request in %s seconds", sleep_time)
        sleep(sleep_time)
        return False

    return 400 <= status_code < 500

@backoff.on_exception(backoff.expo,
                      requests.exceptions.HTTPError,
                      max_tries=10,
                      giveup=is_fatal)
def call_api(url, params, headers):
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response

def get_cursor_based(url, access_token, cursor=None, **kwargs):
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Bearer {}'.format(access_token),
        **kwargs.get('headers', {})
    }

    params = {
        'page[size]': 100,
        **kwargs.get('params', {})
    }

    if cursor:
        params['page[after]'] = cursor

    response = call_api(url, params=params, headers=headers)
    response_json = response.json()

    yield response_json

    has_more = response_json['meta']['has_more']

    while has_more:
        cursor = response_json['meta']['after_cursor']
        params['page[after]'] = cursor

        response = call_api(url, params=params, headers=headers)
        response_json = response.json()

        yield response_json
        has_more = response_json['meta']['has_more']

def get_offset_based(url, access_token, **kwargs):
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Bearer {}'.format(access_token),
        **kwargs.get('headers', {})
    }

    params = {
        'per_page': 100,
        **kwargs.get('params', {})
    }

    response = call_api(url, params=params, headers=headers)
    response_json = response.json()

    yield response_json

    next_url = response_json.get('next_page')

    while next_url:
        response = call_api(next_url, params=None, headers=headers)
        response_json = response.json()

        yield response_json
        next_url = response_json.get('next_page')

def get_incremental_export(url, access_token, start_time):
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Bearer {}'.format(access_token),
    }

    params = {'start_time': start_time.timestamp()}

    response = call_api(url, params=params, headers=headers)
    response_json = response.json()

    yield response_json

    end_of_stream = response_json.get('end_of_stream')

    while not end_of_stream:
        cursor = response_json['after_cursor']

        params = {'cursor': cursor}
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        response_json = response.json()

        yield response_json

        end_of_stream = response_json.get('end_of_stream')
