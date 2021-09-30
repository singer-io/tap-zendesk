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

    end_of_stream = response_json['end_of_stream']



    while not end_of_stream:
        cursor = response_json['after_cursor']

        params = {'cursor': cursor}
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        response_json = response.json()

        yield response_json

        end_of_stream = response_json['end_of_stream']


    # response has a cursor, it also has an end_time
    #   end_time -> "The most recent time present in the result set expressed as a Unix epoch time. Use as the start_time to fetch the next page of results"
    # could we use this end_time as a bookmark??
    # i didn't see 'end_time' on the response, only the 'after_cursor'
#INFO Request:...
#INFO METRIC: {"type": "timer", "metric": "http_request_duration", "value": 8.35524868965149, "tags": {"status": "succeeded"}}
# ipdb> after_first_sync = set([x['id'] for x in response.json()['tickets']])
# ipdb> len(after_first_sync)
# 1000
# ipdb> c
#INFO Request:...
#INFO METRIC: {"type": "timer", "metric": "http_request_duration", "value": 8.608530044555664, "tags": {"status": "succeeded"}}
# ipdb> after_second_sync = set([x['id'] for x in response.json()['tickets']])
# ipdb> len(after_second_sync)
# 1000
# ipdb> len(after_second_sync.union(after_first_sync))
# 2000
#
# ^so we're getting 1000 different ids on the second sync
# so it seems like we're paginating correctly
