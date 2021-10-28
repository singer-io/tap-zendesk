import unittest
from tap_zendesk import get_session

class TestGetSession(unittest.TestCase):
    """
    Confirm that partner information is added to session headers when
    present in config.
    """
    def test_no_partner_info_returns_none(self):
        test_session = get_session({})
        #Verify test_session is None when no partner info passed
        self.assertEqual(test_session, None)

    def test_incomplete_partner_info_returns_none(self):
        test_session = get_session({"marketplace_name": "Hithere"})
        #Verify test_session is None when incomplete partner info passed
        self.assertEqual(test_session, None)

    def test_adds_headers_when_all_present_in_config(self):
        test_session = get_session({"marketplace_name": "Hithere",
                                    "marketplace_organization_id": 1234,
                                    "marketplace_app_id": 12345})
        #Verify marketplace_name is as expected return from get_session dict.
        self.assertEqual("Hithere", test_session.headers.get("X-Zendesk-Marketplace-Name"))
        #Verify marketplace_organization_id as expected return from get_session dict.
        self.assertEqual("1234", test_session.headers.get("X-Zendesk-Marketplace-Organization-Id"))
        #Verify marketplace_app_id as expected return from get_session dict.
        self.assertEqual("12345", test_session.headers.get("X-Zendesk-Marketplace-App-Id"))
