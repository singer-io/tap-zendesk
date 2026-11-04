import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch

from tap_zendesk import get_session, http, oauth
from tap_zendesk.streams import Organizations, Tickets, Users


ORIGIN = "https://acme.zendesk.com"
URL = ORIGIN + "/api/v2/users"
CONFIG = {
    "subdomain": "acme",
    "access_token": "test-access-token",
    "refresh_token": "test-refresh-token",
    "client_id": "test-client",
    "client_secret": "test-secret",
}
INVALID_SUBDOMAINS = (
    "attacker.example/", "attacker.example?", "attacker.example#",
    "acme@attacker.example/", "acme.zendesk.com", "acme:443",
    "acme\\attacker", "acme%2f", " acme", "acme\n", "-acme",
    "acme-", "a" * 64, "", None, 123,
)
INVALID_URLS = (
    "https://attacker.example/api/v2/users",
    "https://other.zendesk.com/api/v2/users",
    "http://acme.zendesk.com/api/v2/users",
    "https://acme.zendesk.com:444/api/v2/users",
    "https://acme.zendesk.com.evil.example/api/v2/users",
    "https://user:password@acme.zendesk.com/api/v2/users",
    "https://acme.zendesk.com./api/v2/users",
    "https://acme.zendesk.com:/api/v2/users",
    "https://acme.zendesk.com/api/v2/users#fragment",
    "https://acme.zendesk.com\\@attacker.example/api/v2/users",
    "https://acme.zendesk.com/\nusers",
    "//attacker.example/api/v2/users",
)


class TestRequestOrigins(unittest.TestCase):
    def test_subdomain_rejected_before_stream_url_is_used(self):
        for stream_class in (Users, Organizations, Tickets):
            for subdomain in INVALID_SUBDOMAINS:
                with self.subTest(stream=stream_class.name, subdomain=subdomain):
                    with self.assertRaises(ValueError):
                        stream_class(config={**CONFIG, "subdomain": subdomain}).get_stream_endpoint()

    def test_valid_dns_labels_keep_existing_endpoints(self):
        for subdomain in ("acme", "acme-123", "123", "ACME", "a" * 63):
            with self.subTest(subdomain=subdomain):
                stream = Users(config={**CONFIG, "subdomain": subdomain})
                self.assertEqual(
                    stream.get_stream_endpoint(),
                    f"https://{subdomain}.zendesk.com/api/v2/users",
                )

    def test_final_stream_endpoint_cannot_override_origin(self):
        stream = Users(config=CONFIG)
        stream.endpoint = "https://other.zendesk.com/api/v2/users"
        with self.assertRaises(ValueError):
            stream.get_stream_endpoint()

    @patch("tap_zendesk.http.requests.get")
    def test_sync_request_rejects_untrusted_urls_before_sending(self, request):
        for url in INVALID_URLS:
            if url.startswith("https://other.zendesk.com/"):
                continue
            with self.subTest(url=url), self.assertRaises(ValueError):
                http.call_api(url, 10, {}, http.build_headers("test-token"))
        request.assert_not_called()

    def test_async_request_rejects_untrusted_urls_before_sending(self):
        session = Mock()
        for url in INVALID_URLS:
            if url.startswith("https://other.zendesk.com/"):
                continue
            with self.subTest(url=url), self.assertRaises(ValueError):
                asyncio.run(http.call_api_async(session, url, 10, {}, http.build_headers("test-token")))
        session.get.assert_not_called()

    @patch("tap_zendesk.http.requests.get")
    def test_absolute_pagination_stays_on_initial_origin(self, request):
        for helper in (http.get_offset_based, http.get_incremental_export_offset):
            for field in ("next_page", "after_url"):
                for url in INVALID_URLS:
                    with self.subTest(helper=helper.__name__, field=field, url=url):
                        request.reset_mock()
                        request.return_value = Mock(
                            status_code=200, json=Mock(return_value={field: url}))
                        args = (URL, "test-token", 10, 100)
                        if helper is http.get_incremental_export_offset:
                            args += (0,)
                        with self.assertRaises(ValueError):
                            list(helper(*args))
                        request.assert_called_once()

    @patch("tap_zendesk.http.requests.get")
    def test_same_origin_absolute_pagination_preserves_authentication(self, request):
        for credential in ("test-token", http.get_credential(
                {"email": "test@example.com", "api_token": "test-api-token"})):
            request.reset_mock()
            next_url = ORIGIN + ":443/api/v2/users?page=2"
            request.side_effect = [
                Mock(status_code=200, json=Mock(return_value={"next_page": next_url})),
                Mock(status_code=200, json=Mock(return_value={})),
            ]
            self.assertEqual(len(list(http.get_offset_based(URL, credential, 10, 100))), 2)
            self.assertEqual(request.call_args.args[0], next_url)
            self.assertEqual(
                request.call_args.kwargs["headers"], http.build_headers(credential))
            self.assertFalse(request.call_args.kwargs["allow_redirects"])

    @patch("tap_zendesk.http.requests.get")
    def test_sync_redirects_fail_without_following(self, request):
        for status in (301, 302, 303, 307, 308):
            request.return_value = Mock(status_code=status)
            with self.subTest(status=status), self.assertRaises(ValueError):
                http.call_api(URL, 10, {}, http.build_headers("test-token"))
            self.assertFalse(request.call_args.kwargs["allow_redirects"])

    def test_async_redirects_fail_without_following(self):
        for status in (301, 302, 303, 307, 308):
            session = Mock()
            session.get.return_value.__aenter__ = AsyncMock(
                return_value=Mock(status=status))
            session.get.return_value.__aexit__ = AsyncMock(return_value=False)
            with self.subTest(status=status), self.assertRaises(ValueError):
                asyncio.run(http.call_api_async(session, URL, 10, {}, {}))
            self.assertFalse(session.get.call_args.kwargs["allow_redirects"])

    @patch("tap_zendesk.oauth.requests.post")
    @patch("tap_zendesk.oauth.requests.get")
    def test_oauth_rejects_invalid_subdomains_without_sending_secrets(self, get, post):
        for subdomain in INVALID_SUBDOMAINS:
            config = {**CONFIG, "subdomain": subdomain}
            for helper in (oauth.is_token_valid, oauth._refresh_access_token):
                with self.subTest(helper=helper.__name__, subdomain=subdomain):
                    with self.assertRaises(ValueError):
                        helper(config)
        get.assert_not_called()
        post.assert_not_called()

    @patch("tap_zendesk.oauth.requests.post")
    @patch("tap_zendesk.oauth.requests.get")
    def test_oauth_redirects_do_not_forward_or_rotate_secrets(self, get, post):
        get.return_value = Mock(status_code=302)
        post.return_value = Mock(status_code=307)
        with self.assertRaises(ValueError):
            oauth.refresh_credentials(CONFIG.copy(), "/unused/config.json")
        get.assert_called_once()
        post.assert_not_called()
        self.assertFalse(get.call_args.kwargs["allow_redirects"])
        with self.assertRaises(ValueError):
            oauth._refresh_access_token(CONFIG)
        self.assertFalse(post.call_args.kwargs["allow_redirects"])

    @patch("tap_zendesk.oauth.requests.get")
    def test_oauth_final_origin_is_checked_before_request(self, request):
        with patch.object(oauth, "TOKEN_INFO_URL", "https://other.zendesk.com/current"):
            with self.assertRaises(ValueError):
                oauth.is_token_valid(CONFIG)
        request.assert_not_called()

    @patch("tap_zendesk.oauth.requests.post")
    def test_oauth_refresh_final_origin_is_checked_before_request(self, request):
        with patch.object(oauth, "TOKEN_REFRESH_URL", "https://other.zendesk.com/tokens"):
            with self.assertRaises(ValueError):
                oauth._refresh_access_token(CONFIG)
        request.assert_not_called()

    def test_startup_validates_subdomain_even_without_refresh_or_in_dev_mode(self):
        for config in (
                {"subdomain": "attacker.example/", "access_token": "test"},
                {"subdomain": "attacker.example/", "email": "test", "api_token": "test"}):
            for dev_mode in (False, True):
                with self.subTest(config=config, dev_mode=dev_mode), self.assertRaises(ValueError):
                    oauth.refresh_credentials(config, "/unused/config.json", dev_mode=dev_mode)

    def test_zenpy_session_pins_origin_with_or_without_partner_headers(self):
        for extra in ({}, {"marketplace_name": "test", "marketplace_organization_id": 1,
                          "marketplace_app_id": 2}):
            with self.subTest(extra=extra):
                session = get_session({**CONFIG, **extra})
                try:
                    with patch("requests.Session.send") as request:
                        for url in INVALID_URLS:
                            with self.subTest(url=url), self.assertRaises(ValueError):
                                session.send(Mock(url=url))
                        request.assert_not_called()
                        request.return_value = Mock(status_code=200)
                        session.get(URL, headers=http.build_headers("test-token"), allow_redirects=True)
                        request.assert_called_once()
                        self.assertFalse(request.call_args.kwargs["allow_redirects"])
                        request.return_value = Mock(status_code=302)
                        with self.assertRaises(ValueError):
                            session.post(ORIGIN + "/oauth/tokens", json={"secret": "test"})
                finally:
                    session.close()
