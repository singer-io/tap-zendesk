"""
On every session, new access_token and refresh_token values are generated
via the refresh_token grant type, and the updated tokens are persisted back to the config file.

When the access_token and refresh_token are renewed, the old tokens are invalidated and cannot be used again.

Backward compatibility: if refresh_token is absent from the config, token refresh is skipped.
"""

import json
import requests
import singer
from tap_zendesk.http import ZendeskError

LOGGER = singer.get_logger()

# Zendesk OAuth token endpoint
TOKEN_REFRESH_URL = "https://{subdomain}.zendesk.com/oauth/tokens"

# access_token validity: 48 hours (172800 seconds)
ACCESS_TOKEN_EXPIRES_IN = 172800

def _refresh_access_token(config):
    """
    POST to Zendesk /oauth/tokens with grant_type=refresh_token.
    Returns the full token response dict.
    """
    subdomain = config['subdomain']
    url = TOKEN_REFRESH_URL.format(subdomain=subdomain)

    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': config['refresh_token'],
        'client_id': config['client_id'],
        'client_secret': config['client_secret'],
        'expires_in': ACCESS_TOKEN_EXPIRES_IN
    }

    response = requests.post(url, json=payload, timeout=60)

    if response.status_code != 200:
        raise ZendeskError(
            "OAuth token refresh failed. HTTP {}: {}".format(
                response.status_code, response.text))

    token_data = response.json()

   # Validate that the expected fields are present in the token response
    missing_fields = [
        field for field in ("access_token", "refresh_token")
        if field not in token_data
    ]
    if missing_fields:
        raise ZendeskError(
            "OAuth token refresh response missing required field(s) {}.".format(", ".join(missing_fields))
        )

    LOGGER.info("Successfully refreshed access_token.")
    return token_data


def _write_config(config_path, config):
    """
    Write updated config back to the config file.
    """
    with open(config_path, encoding='utf-8') as file:
        disk_config = json.load(file)

    disk_config['refresh_token'] = config['refresh_token']
    disk_config['access_token'] = config['access_token']

    with open(config_path, 'w', encoding='utf-8') as file:
        json.dump(disk_config, file, indent=2)


def refresh_credentials(config, config_path, dev_mode=False):
    """
    Main entry point for OAuth credential management.
    """

    if dev_mode:
        return config

    # api_token/email auth doesn't use access_token/refresh_token, so skip
    if 'access_token' not in config:
        return config

    # Backward compatibility: no refresh_token means legacy flow — do nothing
    if 'refresh_token' not in config:
        return config

    token_data = _refresh_access_token(config)

    config['access_token'] = token_data['access_token']
    config['refresh_token'] = token_data['refresh_token']

    # Persist updated tokens back to config file
    _write_config(config_path, config)

    return config
