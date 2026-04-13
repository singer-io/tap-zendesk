"""
Handles refreshing access_token and refresh_token for Zendesk OAuth.

On every session, a lightweight credential check (GET /api/v2/users/me) is
performed.  If the API returns HTTP 401 (token expired / revoked), new
access_token and refresh_token values are obtained via the refresh_token
grant type and persisted back to the config file.

When the tokens are renewed the old ones are invalidated by Zendesk.

Backward compatibility: if refresh_token is absent from the config, token
refresh is skipped.

Zendesk docs:
  https://developer.zendesk.com/api-reference/ticketing/oauth/grant_type_tokens/
"""

import json
import requests
import singer
from tap_zendesk.http import ZendeskError

LOGGER = singer.get_logger()

# Zendesk OAuth token endpoint
TOKEN_REFRESH_URL = "https://{subdomain}.zendesk.com/oauth/tokens"

# Lightweight endpoint used to verify that the current access_token is valid
CREDENTIAL_CHECK_URL = "https://{subdomain}.zendesk.com/api/v2/users/me.json"


def _is_token_expired(_config):
    """
    Make a lightweight API call to Zendesk to check whether the current
    access_token is still valid.

    Returns True if the token is expired/invalid (HTTP 401), False otherwise.
    """
    subdomain = _config['subdomain']
    url = CREDENTIAL_CHECK_URL.format(subdomain=subdomain)
    headers = {
        'Authorization': 'Bearer {}'.format(_config['access_token']),
        'Accept': 'application/json',
    }

    try:
        response = requests.get(url, headers=headers, timeout=60)
    except requests.RequestException as exc:
        LOGGER.warning("Credential check request failed: %s. Will attempt token refresh.", exc)
        return True

    if response.status_code == 200:
        return False

    return True


def _refresh_access_token(_config):
    """
    POST to Zendesk /oauth/tokens with grant_type=refresh_token.
    Returns the full token response dict.
    """
    subdomain = _config['subdomain']
    url = TOKEN_REFRESH_URL.format(subdomain=subdomain)

    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': _config['refresh_token'],
        'client_id': _config['client_id'],
        'client_secret': _config['client_secret'],
    }

    response = requests.post(url, json=payload, timeout=60)

    if response.status_code != 200:
        raise ZendeskError(
            "OAuth token refresh failed. HTTP {}: {}".format(
                response.status_code, response.text))

    token_data = response.json()

    LOGGER.info("Successfully refreshed access_token and refresh_token.")
    return token_data


def _write_config(config_path, _config):
    """
    Write updated tokens back to the config file.
    """
    with open(config_path, encoding='utf-8') as file:
        disk_config = json.load(file)

    disk_config['refresh_token'] = _config['refresh_token']
    disk_config['access_token'] = _config['access_token']

    with open(config_path, 'w', encoding='utf-8') as file:
        json.dump(disk_config, file, indent=2)


def refresh_credentials(_config, config_path, dev_mode=False):
    """
    Main entry point for OAuth credential management.
    """

    if dev_mode:
        return _config

    # api_token/email auth doesn't use access_token/refresh_token, so skip
    if 'access_token' not in _config:
        return _config

    # Backward compatibility: no refresh_token means legacy flow — do nothing
    if 'refresh_token' not in _config:
        return _config

    if not _is_token_expired(_config):
        return _config

    token_data = _refresh_access_token(_config)

    _config['access_token'] = token_data['access_token']
    _config['refresh_token'] = token_data['refresh_token']

    # Persist updated tokens back to config file
    _write_config(config_path, _config)

    return _config
