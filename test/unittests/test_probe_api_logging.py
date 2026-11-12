import logging
import unittest
from unittest.mock import Mock, patch

from tap_zendesk import http
from tap_zendesk.exceptions import (
    ZendeskForbiddenError,
    ZendeskInternalServerError,
    ZendeskNotFoundError,
    ZendeskUnauthorizedError,
)

URL = "https://test.zendesk.com/api/v2/satisfaction_reasons"


def mocked_response(status_code):
    response = Mock()
    response.status_code = status_code
    response.json.return_value = {}
    response.headers = {}
    if status_code >= 400:
        response.raise_for_status.side_effect = http.HTTPError(response=response)
    else:
        response.raise_for_status.return_value = None
    return response


@patch("backoff._sync.time.sleep", return_value=None)
class TestGiveupLogLevel(unittest.TestCase):
    """Only the 403/404 a probe expects is logged at WARNING."""

    def _giveup_levels(self, func, status_code, exc_class, **kwargs):
        with patch("requests.get", return_value=mocked_response(status_code)), \
                self.assertLogs("backoff", level="DEBUG") as logs:
            with self.assertRaises(exc_class):
                func(URL, 300, params={}, headers={}, **kwargs)
        return [(r.levelno, r.getMessage()) for r in logs.records if r.getMessage().startswith("Giving up")]

    def test_probe_403_is_warning(self, _):
        records = self._giveup_levels(http.probe_api, 403, ZendeskForbiddenError)
        self.assertEqual(records, [(logging.WARNING, records[0][1])])
        self.assertIn("probe_api", records[0][1])
        self.assertIn("ZendeskForbiddenError", records[0][1])

    def test_probe_404_is_warning_when_not_found_ok(self, _):
        records = self._giveup_levels(http.probe_api, 404, ZendeskNotFoundError, not_found_ok=True)
        self.assertEqual([level for level, _ in records], [logging.WARNING])

    def test_probe_404_is_error_by_default(self, _):
        records = self._giveup_levels(http.probe_api, 404, ZendeskNotFoundError)
        self.assertEqual([level for level, _ in records], [logging.ERROR])

    def test_probe_401_is_error(self, _):
        records = self._giveup_levels(http.probe_api, 401, ZendeskUnauthorizedError, not_found_ok=True)
        self.assertEqual([level for level, _ in records], [logging.ERROR])

    def test_probe_500_after_retries_is_error(self, _):
        records = self._giveup_levels(http.probe_api, 500, ZendeskInternalServerError, not_found_ok=True)
        self.assertEqual([level for level, _ in records], [logging.ERROR])

    def test_call_api_unchanged(self, _):
        for status_code, exc_class in ((403, ZendeskForbiddenError), (404, ZendeskNotFoundError)):
            with self.subTest(status_code=status_code):
                records = self._giveup_levels(http.call_api, status_code, exc_class)
                self.assertEqual([level for level, _ in records], [logging.ERROR])
                self.assertIn("Giving up call_api(...)", records[0][1])


@patch("backoff._sync.time.sleep", return_value=None)
class TestProbeApiRetries(unittest.TestCase):
    """probe_api retries the same way as call_api."""

    def test_fatal_4xx_not_retried(self, _):
        for func in (http.call_api, http.probe_api):
            with self.subTest(func=func.__name__), \
                    patch("requests.get", return_value=mocked_response(403)) as mock_get:
                with self.assertRaises(ZendeskForbiddenError):
                    func(URL, 300, params={}, headers={})
                self.assertEqual(mock_get.call_count, 1)

    def test_500_retried_ten_times(self, _):
        for func in (http.call_api, http.probe_api):
            with self.subTest(func=func.__name__), \
                    patch("requests.get", return_value=mocked_response(500)) as mock_get:
                with self.assertRaises(ZendeskInternalServerError):
                    func(URL, 300, params={}, headers={})
                self.assertEqual(mock_get.call_count, 10)

    def test_retryable_409_then_success(self, _):
        for func in (http.call_api, http.probe_api):
            with self.subTest(func=func.__name__), \
                    patch("requests.get", side_effect=[mocked_response(409), mocked_response(200)]) as mock_get:
                self.assertEqual(func(URL, 300, params={}, headers={}).status_code, 200)
                self.assertEqual(mock_get.call_count, 2)

    def test_connection_error_retried_five_times(self, _):
        for func in (http.call_api, http.probe_api):
            with self.subTest(func=func.__name__), \
                    patch("requests.get", side_effect=http.ConnectionError()) as mock_get:
                with self.assertRaises(http.ConnectionError):
                    func(URL, 300, params={}, headers={})
                self.assertEqual(mock_get.call_count, 5)

    def test_probe_retry_logged_at_info(self, _):
        with patch("requests.get", side_effect=[mocked_response(409), mocked_response(200)]), \
                self.assertLogs("backoff", level="DEBUG") as logs:
            http.probe_api(URL, 300, params={}, headers={})
        self.assertEqual([(r.levelno, r.getMessage()[:22]) for r in logs.records],
                         [(logging.INFO, "Backing off probe_api(")])


class TestCheckAccessUsesProbeApi(unittest.TestCase):
    """HTTP-based check_access implementations use probe_api."""

    CONFIG = {"subdomain": "test", "access_token": "token", "start_date": "2026-01-01T00:00:00Z"}

    def _probe_kwargs(self, stream_cls):
        stream = stream_cls(client=Mock(), config=self.CONFIG)
        with patch("tap_zendesk.http.probe_api") as mock_probe, \
                patch("tap_zendesk.http.call_api") as mock_call:
            stream.check_access()
        mock_probe.assert_called_once()
        mock_call.assert_not_called()
        return mock_probe.call_args.kwargs

    def test_tickets(self):
        from tap_zendesk.streams import Tickets
        self.assertNotIn("not_found_ok", self._probe_kwargs(Tickets))

    def test_base_stream(self):
        from tap_zendesk.streams import SatisfactionRatings
        self.assertNotIn("not_found_ok", self._probe_kwargs(SatisfactionRatings))

    def test_ticket_audits_expects_404(self):
        from tap_zendesk.streams import TicketAudits
        self.assertTrue(self._probe_kwargs(TicketAudits)["not_found_ok"])

    def test_ticket_audits_still_ignores_404(self):
        from tap_zendesk.streams import TicketAudits
        stream = TicketAudits(client=Mock(), config=self.CONFIG)
        with patch("tap_zendesk.http.probe_api", side_effect=ZendeskNotFoundError("not found")):
            stream.check_access()
