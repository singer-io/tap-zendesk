"""
OAuth token management for Zendesk API.

Handles refreshing access_token and refresh_token according to Zendesk's
OAuth token expiration policy:
  - access_token: max 172,800 seconds (2 days / 48 hours)
  - refresh_token: max 7,776,000 seconds (90 days)

Zendesk docs: https://developer.zendesk.com/api-reference/ticketing/oauth/grant_type_tokens/

On every session a new access_token is generated via the refresh_token grant type.
The refresh_token is refreshed 1 day before its expiry and the updated tokens
are persisted back to the config file.

Backward compatible: when refresh_token is absent from config, nothing happens.
"""

import json
import datetime

import requests
import singer

LOGGER = singer.get_logger()

# Zendesk OAuth token endpoint
TOKEN_REFRESH_URL = "https://{subdomain}.zendesk.com/oauth/tokens"

# access_token validity: 48 hours (172800 seconds)
ACCESS_TOKEN_EXPIRES_IN = 172800

# refresh_token validity: 90 days (7776000 seconds)
REFRESH_TOKEN_EXPIRES_IN = 7776000

# Renew refresh_token 1 day before its expiry
REFRESH_TOKEN_RENEWAL_BUFFER_DAYS = 1

def _is_refresh_token_expiring(config):
    """
    Check whether the refresh_token is within 1 day of its expiry.
    If 'refresh_token_expires_at' is not stored in config, assume it needs refreshing.
    """
    expires_at_str = config.get('refresh_token_expires_at')
    if not expires_at_str:
        LOGGER.info("No 'refresh_token_expires_at' in config. Will refresh the refresh_token.")
        return True

    try:
        expires_at = datetime.datetime.fromisoformat(expires_at_str)
    except (ValueError, TypeError):
        LOGGER.warning("Invalid 'refresh_token_expires_at': %s. Will refresh.", expires_at_str)
        return True

    now = datetime.datetime.now(datetime.timezone.utc)
    buffer = datetime.timedelta(days=REFRESH_TOKEN_RENEWAL_BUFFER_DAYS)

    if now >= (expires_at - buffer):
        LOGGER.info("Refresh token expiring soon (expires_at: %s). Refreshing it now.", expires_at_str)
        return True

    return False


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
        'scope': 'read',
        'expires_in': ACCESS_TOKEN_EXPIRES_IN,
        'refresh_token_expires_in': REFRESH_TOKEN_EXPIRES_IN
    }

    response = requests.post(url, json=payload, timeout=60)

    if response.status_code != 200:
        raise Exception(
            "OAuth token refresh failed. HTTP {}: {}".format(
                response.status_code, response.text))

    token_data = response.json()
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
    disk_config['refresh_token_expires_at'] = config['refresh_token_expires_at']

    with open(config_path, 'w', encoding='utf-8') as file:
        json.dump(disk_config, file, indent=2)


def refresh_credentials(config, config_path, dev_mode=False):
    """
    Main entry point for OAuth credential management.
    """

    # api_token/email auth doesn't use access_token/refresh_token, so skip
    if 'access_token' not in config:
        return config

    # Backward compatibility: no refresh_token means legacy flow — do nothing
    if 'refresh_token' not in config:
        return config

    if dev_mode:
        return config

    if not _is_refresh_token_expiring(config):
        return config

    token_data = _refresh_access_token(config)

    config['access_token'] = token_data['access_token']
    config['refresh_token'] = token_data['refresh_token']

    # Compute and store refresh_token_expires_at
    expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        seconds=REFRESH_TOKEN_EXPIRES_IN)
    config['refresh_token_expires_at'] = expires_at.isoformat()

    # Persist updated tokens back to config file
    _write_config(config_path, config)

    return config
