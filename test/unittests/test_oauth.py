import unittest
from unittest.mock import patch, MagicMock

from tap_zendesk.oauth import (
    refresh_credentials,
    _refresh_access_token,
    _is_token_expired,
)
from tap_zendesk.http import ZendeskError

# ---------------------------------------------------------------------------
# Helper: mock response object matching requests.Response interface
# ---------------------------------------------------------------------------
class MockResponse:
    def __init__(self, status_code, json_data=None, text=''):
        self.status_code = status_code
        self._json = json_data
        self.text = text

    def json(self):
        if self._json is None:
            raise ValueError("No JSON")
        return self._json


# ===========================================================================
# 1. BACKWARD COMPATIBILITY
# ===========================================================================
class TestBackwardCompatibility(unittest.TestCase):
    """Config without refresh_token should pass through untouched."""

    def test_no_refresh_token_access_token_only(self):
        """Legacy OAuth: access_token present, no refresh_token."""
        config = {
            'subdomain': 'acme',
            'access_token': 'legacy_at',
            'start_date': '2024-01-01T00:00:00Z',
        }
        result = refresh_credentials(config, '/tmp/fake.json')
        self.assertEqual(result['access_token'], 'legacy_at')
        self.assertNotIn('refresh_token', result)

    def test_api_token_email_auth(self):
        """email+api_token auth has no access_token — should skip entirely."""
        config = {
            'subdomain': 'acme',
            'email': 'agent@acme.com',
            'api_token': 'zd_api_token_123',
            'start_date': '2024-01-01T00:00:00Z',
        }
        result = refresh_credentials(config, '/tmp/fake.json')
        self.assertEqual(result, config)
        self.assertNotIn('access_token', result)
        self.assertNotIn('refresh_token', result)

    def test_empty_config_only_required_keys(self):
        """Minimal config with only required keys."""
        config = {
            'subdomain': 'acme',
            'start_date': '2024-01-01T00:00:00Z',
        }
        result = refresh_credentials(config, '/tmp/fake.json')
        self.assertEqual(result, config)


# ===========================================================================
# 2. DEV MODE
# ===========================================================================
class TestDevMode(unittest.TestCase):
    """--dev flag: reuse existing tokens, no API calls."""

    @patch('tap_zendesk.oauth._write_config')
    @patch('tap_zendesk.oauth.requests.post')
    def test_dev_mode_returns_existing_tokens(self, mock_post, mock_write_config):
        config = {
            'subdomain': 'acme',
            'access_token': 'dev_at',
            'refresh_token': 'dev_rt',
            'client_id': 'cid',
            'client_secret': 'cs',
        }
        result = refresh_credentials(config, '/tmp/fake.json', dev_mode=True)
        self.assertEqual(result['access_token'], 'dev_at')
        self.assertEqual(result['refresh_token'], 'dev_rt')
        mock_post.assert_not_called()
        mock_write_config.assert_not_called()

# ===========================================================================
# 3. _refresh_access_token — API BEHAVIOUR SCENARIOS
# ===========================================================================
class TestRefreshAccessTokenAPI(unittest.TestCase):
    """Test every HTTP response scenario from Zendesk /oauth/tokens."""

    BASE_CONFIG = {
        'subdomain': 'acme',
        'refresh_token': 'old_rt',
        'client_id': 'cid',
        'client_secret': 'cs',
    }

    # --- 200 OK: normal success ---
    @patch('tap_zendesk.oauth.requests.post')
    def test_200_success_returns_token_data(self, mock_post):
        token_resp = {
            'access_token': 'fresh_at',
            'token_type': 'bearer',
            'refresh_token': 'fresh_rt',
        }
        mock_post.return_value = MockResponse(200, token_resp)
        result = _refresh_access_token(self.BASE_CONFIG)
        self.assertEqual(result['access_token'], 'fresh_at')
        self.assertEqual(result['refresh_token'], 'fresh_rt')

        url = mock_post.call_args[0][0]
        self.assertEqual(url, 'https://acme.zendesk.com/oauth/tokens')

    # --- 200 OK: verify request payload (no expires_in) ---
    @patch('tap_zendesk.oauth.requests.post')
    def test_200_correct_payload(self, mock_post):
        mock_post.return_value = MockResponse(200, {
            'access_token': 'a', 'refresh_token': 'r', 'token_type': 'bearer'
        })
        _refresh_access_token(self.BASE_CONFIG)
        payload = mock_post.call_args[1]['json']
        self.assertEqual(payload['grant_type'], 'refresh_token')
        self.assertEqual(payload['refresh_token'], 'old_rt')
        self.assertEqual(payload['client_id'], 'cid')
        self.assertEqual(payload['client_secret'], 'cs')
        self.assertNotIn('expires_in', payload)

    # --- 400 Bad Request ---
    @patch('tap_zendesk.oauth.requests.post')
    def test_400_bad_request(self, mock_post):
        mock_post.return_value = MockResponse(400, text='{"error": "invalid_grant"}')
        with self.assertRaises(ZendeskError) as ctx:
            _refresh_access_token(self.BASE_CONFIG)
        self.assertIn('400', str(ctx.exception))


# ===========================================================================
# 4. _is_token_expired — TOKEN INFO API CALL
# ===========================================================================
class TestIsTokenExpired(unittest.TestCase):
    """Token expiry check via GET /api/v2/oauth/tokens/current.json."""

    BASE_CONFIG = {
        'subdomain': 'acme',
        'access_token': 'some_at',
    }

    @patch('tap_zendesk.oauth.time.time', return_value=1_000_000)
    @patch('tap_zendesk.oauth.requests.get')
    def test_token_valid_beyond_buffer(self, mock_get, mock_time):
        """Token with plenty of remaining life should return False."""
        # expires_at=1_100_000, remaining=100_000 > buffer(10_800) → valid
        mock_get.return_value = MockResponse(200, {
            'token': {'expires_at': 1_100_000}
        })
        self.assertFalse(_is_token_expired(self.BASE_CONFIG))
        mock_get.assert_called_once()
        url = mock_get.call_args[0][0]
        self.assertEqual(url, 'https://acme.zendesk.com/api/v2/oauth/tokens/current.json')

    @patch('tap_zendesk.oauth.time.time', return_value=1_000_000)
    @patch('tap_zendesk.oauth.requests.get')
    def test_token_expiring_within_buffer(self, mock_get, mock_time):
        """Token expiring within the 3-hour buffer should return True."""
        # expires_at=1_005_000, remaining=5_000 < buffer(10_800) → expired
        mock_get.return_value = MockResponse(200, {
            'token': {'expires_at': 1_005_000}
        })
        self.assertTrue(_is_token_expired(self.BASE_CONFIG))

    @patch('tap_zendesk.oauth.time.time', return_value=1_000_000)
    @patch('tap_zendesk.oauth.requests.get')
    def test_token_already_expired(self, mock_get, mock_time):
        """Token already past expiry time should return True."""
        # expires_at=950_000, remaining=-50_000 < buffer → expired
        mock_get.return_value = MockResponse(200, {
            'token': {'expires_at': 950_000}
        })
        self.assertTrue(_is_token_expired(self.BASE_CONFIG))

    @patch('tap_zendesk.oauth.requests.get')
    def test_401_token_expired(self, mock_get):
        """401 means token is expired — should return True."""
        mock_get.return_value = MockResponse(401, text='Unauthorized')
        self.assertTrue(_is_token_expired(self.BASE_CONFIG))

    @patch('tap_zendesk.oauth.requests.get')
    def test_403_triggers_refresh(self, mock_get):
        """403 (unexpected) should trigger refresh to be safe."""
        mock_get.return_value = MockResponse(403, text='Forbidden')
        self.assertTrue(_is_token_expired(self.BASE_CONFIG))

    @patch('tap_zendesk.oauth.requests.get')
    def test_500_triggers_refresh(self, mock_get):
        """500 (unexpected) should trigger refresh to be safe."""
        mock_get.return_value = MockResponse(500, text='Internal Server Error')
        self.assertTrue(_is_token_expired(self.BASE_CONFIG))

    @patch('tap_zendesk.oauth.requests.get')
    def test_request_exception_triggers_refresh(self, mock_get):
        """Network failure should trigger refresh."""
        import requests as req
        mock_get.side_effect = req.ConnectionError("Network unreachable")
        self.assertTrue(_is_token_expired(self.BASE_CONFIG))

    @patch('tap_zendesk.oauth.time.time', return_value=1_000_000)
    @patch('tap_zendesk.oauth.requests.get')
    def test_correct_headers_sent(self, mock_get, mock_time):
        """Verify Authorization header is sent correctly."""
        mock_get.return_value = MockResponse(200, {
            'token': {'expires_at': 1_100_000}
        })
        _is_token_expired(self.BASE_CONFIG)
        headers = mock_get.call_args[1]['headers']
        self.assertEqual(headers['Authorization'], 'Bearer some_at')
        self.assertEqual(headers['Accept'], 'application/json')


# ===========================================================================
# 5. refresh_credentials — END-TO-END FLOW
# ===========================================================================
class TestRefreshCredentialsE2E(unittest.TestCase):
    """End-to-end tests for the main refresh_credentials function."""

    BASE_CONFIG = {
        'subdomain': 'acme',
        'access_token': 'old_at',
        'refresh_token': 'old_rt',
        'client_id': 'cid',
        'client_secret': 'cs',
    }

    @patch('tap_zendesk.oauth._write_config')
    @patch('tap_zendesk.oauth._refresh_access_token')
    @patch('tap_zendesk.oauth._is_token_expired', return_value=True)
    def test_expired_token_triggers_refresh_and_write(self, mock_expired, mock_refresh, mock_write):
        mock_refresh.return_value = {
            'access_token': 'new_at',
            'refresh_token': 'new_rt',
        }
        config = dict(self.BASE_CONFIG)
        result = refresh_credentials(config, '/tmp/fake.json')

        self.assertEqual(result['access_token'], 'new_at')
        self.assertEqual(result['refresh_token'], 'new_rt')
        mock_refresh.assert_called_once()
        mock_write.assert_called_once_with('/tmp/fake.json', result)
        # expires_at should NOT be set
        self.assertNotIn('access_token_expires_at', result)

    @patch('tap_zendesk.oauth._write_config')
    @patch('tap_zendesk.oauth._refresh_access_token')
    @patch('tap_zendesk.oauth._is_token_expired', return_value=False)
    def test_valid_token_skips_refresh(self, mock_expired, mock_refresh, mock_write):
        config = dict(self.BASE_CONFIG)
        result = refresh_credentials(config, '/tmp/fake.json')

        self.assertEqual(result['access_token'], 'old_at')
        self.assertEqual(result['refresh_token'], 'old_rt')
        mock_refresh.assert_not_called()
        mock_write.assert_not_called()
