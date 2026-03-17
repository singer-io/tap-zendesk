"""
Handles refreshing access_token and refresh_token according to Zendesk's
OAuth token expiration policy:
  - access_token: max 172,800 seconds (2 days / 48 hours)

On every session, new access_token and refresh_token values are generated
via the refresh_token grant type, and the updated tokens are persisted back to the config file.

When the access_token and refresh_token are renewed, the old tokens are invalidated and cannot be used again.

Backward compatibility: if refresh_token is absent from the config, token refresh is skipped.

Zendesk docs: https://developer.zendesk.com/api-reference/ticketing/oauth/grant_type_tokens/
"""

import json
import datetime
import requests
import singer
from tap_zendesk.http import ZendeskError

LOGGER = singer.get_logger()

# Zendesk OAuth token endpoint
TOKEN_REFRESH_URL = "https://{subdomain}.zendesk.com/oauth/tokens"

# access_token validity: 48 hours (172800 seconds)
ACCESS_TOKEN_EXPIRES_IN = 172800

# Renew access_token 30 minutes before its expiry
ACCESS_TOKEN_RENEWAL_BUFFER_MINUTES = 30

def _is_access_token_expiring(_config):
    """
    Check whether the access_token is within 30 minutes of its expiry.
    If 'access_token_expires_at' is not stored in config, assume it needs refreshing.
    """
    expires_at_str = _config.get('access_token_expires_at')
    if not expires_at_str:
        LOGGER.info("No 'access_token_expires_at' in config. Will refresh the access_token.")
        return True

    try:
        expires_at = datetime.datetime.fromisoformat(expires_at_str)
    except (ValueError, TypeError):
        LOGGER.warning("Invalid 'access_token_expires_at': %s. Will refresh.", expires_at_str)
        return True

    now = datetime.datetime.now(datetime.timezone.utc)
    buffer = datetime.timedelta(minutes=ACCESS_TOKEN_RENEWAL_BUFFER_MINUTES)

    if now >= (expires_at - buffer):
        LOGGER.info("Access token expiring soon (expires_at: %s). Refreshing it now.", expires_at_str)
        return True

    return False

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


def _write_config(config_path, _config):
    """
    Write updated config back to the config file.
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

    if not _is_access_token_expiring(_config):
        return _config

    token_data = _refresh_access_token(_config)

    _config['access_token'] = token_data['access_token']
    _config['refresh_token'] = token_data['refresh_token']

    # Compute and store access_token_expires_at
    expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        seconds=ACCESS_TOKEN_EXPIRES_IN)
    _config['access_token_expires_at'] = expires_at.isoformat()

    # Persist updated tokens back to config file
    _write_config(config_path, _config)

    return _config
