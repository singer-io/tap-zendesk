import unittest
from tap_zendesk import get_session, validate_dependencies, DependencyException
from tap_zendesk.http import ZendeskSession

class TestGetSession(unittest.TestCase):
    """
    Confirm that partner information is added to session headers when
    present in config.
    """
    def test_no_partner_info_returns_origin_pinned_session(self):
        with get_session({"subdomain": "acme"}) as test_session:
            self.assertIsInstance(test_session, ZendeskSession)
            self.assertNotIn("X-Zendesk-Marketplace-Name", test_session.headers)

    def test_incomplete_partner_info_returns_origin_pinned_session(self):
        with get_session({"subdomain": "acme", "marketplace_name": "Hithere"}) as test_session:
            self.assertIsInstance(test_session, ZendeskSession)
            self.assertNotIn("X-Zendesk-Marketplace-Name", test_session.headers)

    def test_adds_headers_when_all_present_in_config(self):
        test_session = get_session({"subdomain": "acme", "marketplace_name": "Hithere",
                                    "marketplace_organization_id": 1234,
                                    "marketplace_app_id": 12345})
        self.assertEqual("Hithere", test_session.headers.get("X-Zendesk-Marketplace-Name"))
        self.assertEqual("1234", test_session.headers.get("X-Zendesk-Marketplace-Organization-Id"))
        self.assertEqual("12345", test_session.headers.get("X-Zendesk-Marketplace-App-Id"))
