import requests


#@backoff(something)
def get_cursor_based(url, access_token, cursor=None):
    # something like this
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': 'Bearer {}'.format(access_token),
    }

    params = {
        'page[size]': 100,
    }

    if cursor:
        params['page[after]'] = cursor

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
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
