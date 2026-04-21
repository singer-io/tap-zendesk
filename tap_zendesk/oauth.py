"""
Handles refreshing access_token and refresh_token for Zendesk OAuth.

On every session, the current token is inspected via
GET /api/v2/oauth/tokens/current.json.  If the token will expire within
a 3-hour buffer window (Buffer for ongoing sync), new
access_token and refresh_token values are obtained via the refresh_token
grant type and persisted back to the config file.

When the tokens are renewed the old ones are invalidated by Zendesk.

Backward compatibility: if refresh_token is absent from the config, token
refresh is skipped.

Zendesk docs:
  https://developer.zendesk.com/api-reference/ticketing/oauth/grant_type_tokens/
"""

import json
import time
from datetime import datetime
import requests
import singer
from tap_zendesk.http import ZendeskError

LOGGER = singer.get_logger()

# Zendesk OAuth token endpoint
TOKEN_REFRESH_URL = "https://{subdomain}.zendesk.com/oauth/tokens"

# Endpoint to inspect the current OAuth token
TOKEN_INFO_URL = "https://{subdomain}.zendesk.com/api/v2/oauth/tokens/current.json"

# Refresh the token if it expires within this many seconds (3 hours)
EXPIRY_BUFFER_SECONDS = 3 * 60 * 60

ACCESS_TOKEN_VALIDITY_SECONDS = 48 * 60 * 60

def _is_token_expired(_config):
    """
    Call GET /api/v2/oauth/tokens/current.json to retrieve the token's
    ``expires_at`` (Unix timestamp).

    Returns True when:
      - The API returns a non-200 status (token already invalid/revoked).
      - The token will expire within the EXPIRY_BUFFER_SECONDS window.

    Returns False if the token is still valid beyond the buffer.
    """
    subdomain = _config['subdomain']
    url = TOKEN_INFO_URL.format(subdomain=subdomain)
    headers = {
        'Authorization': 'Bearer {}'.format(_config['access_token']),
        'Accept': 'application/json',
    }

    try:
        response = requests.get(url, headers=headers, timeout=60)
    except requests.RequestException as exc:
        LOGGER.warning("Token info request failed: %s. Will attempt token refresh.", exc)
        return True

    if response.status_code != 200:
        LOGGER.info("Token info endpoint returned HTTP %s. Token needs to be refreshed.", response.status_code)
        return True

    token_info = response.json()['token']
    expires_at = token_info['expires_at']

    # Token has no expiration date. Means it does not expire.
    if not expires_at:
        return False

    now = time.time()
    # expires_at is an ISO 8601 string (e.g. "2026-04-22T06:32:35Z"); convert to Unix timestamp
    expires_at_ts = datetime.fromisoformat(expires_at).timestamp()
    remaining = expires_at_ts - now

    if remaining <= EXPIRY_BUFFER_SECONDS:
        LOGGER.info("Token will expire in %.0f seconds. Will attempt token refresh.", remaining)
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
        'expires_in': ACCESS_TOKEN_VALIDITY_SECONDS
    }

    response = requests.post(url, json=payload, timeout=60)

    if response.status_code != 200:
        raise ZendeskError("Failed to refresh access token. Refresh token might have expired or been revoked. Please re-authorize the connection.")

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
