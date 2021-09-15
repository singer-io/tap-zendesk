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

    return 400 <=status_code < 500

@backoff.on_exception(backoff.expo,
                      requests.exceptions.HTTPError,
                      max_tries=10,
                      giveup=is_fatal)
def call_api(url, params, headers):
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response



def get_cursor_based(url, access_token, cursor=None, **kwargs):
    # something like this
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

        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        response_json = response.json()

        yield response_json
        has_more = response_json['meta']['has_more']
