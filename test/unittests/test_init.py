import unittest
from tap_zendesk import get_session, validate_dependencies, DependencyException

class TestGetSession(unittest.TestCase):
    """
    Confirm that partner information is added to session headers when
    present in config.
    """
    def test_no_partner_info_returns_none(self):
        test_session = get_session({})
        self.assertEqual(test_session, None)

    def test_incomplete_partner_info_returns_none(self):
        test_session = get_session({"marketplace_name": "Hithere"})
        self.assertEqual(test_session, None)

    def test_adds_headers_when_all_present_in_config(self):
        test_session = get_session({"marketplace_name": "Hithere",
                                    "marketplace_organization_id": 1234,
                                    "marketplace_app_id": 12345})
        self.assertEqual("Hithere", test_session.headers.get("X-Zendesk-Marketplace-Name"))
        self.assertEqual("1234", test_session.headers.get("X-Zendesk-Marketplace-Organization-Id"))
        self.assertEqual("12345", test_session.headers.get("X-Zendesk-Marketplace-App-Id"))

    def test_parent_stream_not_selected(self):
        """
        Test that validate_dependencies does raise an exception when the parent stream is not selected but the sub stream is.
        """
        selected_stream_ids = ["ticket_audits", "ticket_metrics", "ticket_comments"]
        with self.assertRaises(DependencyException) as e:
            validate_dependencies(selected_stream_ids)

        self.assertEqual(
            str(e.exception),
            "Unable to extract ticket_audits data. To receive ticket_audits data, you also need to select tickets. Unable to extract ticket_metrics data. To receive ticket_metrics data, you also need to select tickets. Unable to extract ticket_comments data. To receive ticket_comments data, you also need to select tickets.",
        )
