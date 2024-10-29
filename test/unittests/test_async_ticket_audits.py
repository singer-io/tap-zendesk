import unittest
from unittest.mock import patch, MagicMock
from aioresponses import aioresponses
import asyncio
from aiohttp import ClientSession

from tap_zendesk import http, streams


class TestASyncTicketAudits(unittest.TestCase):

    @aioresponses()
    @patch("tap_zendesk.streams.zendesk_metrics.capture")
    @patch("tap_zendesk.streams.LOGGER.warning")
    def test_sync_audit_comment_both_selected(self, mocked, mock_capture, mock_warning):
        """
        Test that tap sync both ticket_audits and ticket_comments when both streams are selected.
        """
        # Mock the necessary data
        ticket_id = 1
        comments_stream = MagicMock()
        comments_stream.is_selected.return_value = True

        # Mock the responses for get_objects
        async def mock_get_objects(session, ticket_id):
            return [
                {
                    "id": ticket_id,
                    "events": [{"type": "Comment", "id": f"comment_{ticket_id}"}],
                    "created_at": "2023-01-01T00:00:00Z",
                    "via": "web",
                    "metadata": {},
                }
            ]

        instance = streams.TicketAudits(None, {})
        instance.stream = "ticket_audits"

        # Run the sync method
        async def run_test():
            with patch.object(
                streams.TicketAudits, "get_objects", side_effect=mock_get_objects
            ):
                async with ClientSession() as session:
                    audit_records, comment_records = await instance.sync(
                        session, ticket_id, comments_stream
                    )

                    # Assertions
                    self.assertEqual(len(audit_records), 1)
                    self.assertEqual(len(comment_records), 1)
                    self.assertEqual(audit_records[0][1]["id"], 1)
                    self.assertEqual(
                        comment_records[0][1]["id"], f"comment_{ticket_id}"
                    )
                    self.assertEqual(
                        comment_records[0][1]["created_at"], "2023-01-01T00:00:00Z"
                    )
                    self.assertEqual(comment_records[0][1]["via"], "web")
                    self.assertEqual(comment_records[0][1]["metadata"], {})
                    self.assertEqual(
                        comment_records[0][1]["ticket_id"], ticket_id)

        asyncio.run(run_test())

    @aioresponses()
    @patch("tap_zendesk.streams.zendesk_metrics.capture")
    @patch("tap_zendesk.streams.LOGGER.warning")
    def test_sync_comment_only_selected(self, mocked, mock_capture, mock_warning):
        """
        Test that tap sync just ticket_comments when only the comment stream is selected.
        """
        # Mock the necessary data
        ticket_id = 1
        comments_stream = MagicMock()
        comments_stream.is_selected.return_value = True

        # Mock the responses for get_objects
        async def mock_get_objects(session, ticket_id):
            return [
                {
                    "id": ticket_id,
                    "events": [{"type": "Comment", "id": f"comment_{ticket_id}"}],
                    "created_at": "2023-01-01T00:00:00Z",
                    "via": "web",
                    "metadata": {},
                }
            ]

        instance = streams.TicketAudits(None, {})

        # Run the sync method
        async def run_test():
            with patch.object(
                streams.TicketAudits, "get_objects", side_effect=mock_get_objects
            ):
                async with ClientSession() as session:
                    audit_records, comment_records = await instance.sync(
                        session, ticket_id, comments_stream
                    )

                    # Assertions
                    self.assertEqual(len(audit_records), 0)
                    self.assertEqual(len(comment_records), 1)

        asyncio.run(run_test())

    @aioresponses()
    @patch("tap_zendesk.streams.zendesk_metrics.capture")
    @patch("tap_zendesk.streams.LOGGER.warning")
    def test_sync_audit_only_selected(self, mocked, mock_capture, mock_warning):
        """
        Test that tap sync just ticket_audits when only the audit stream is selected.
        """
        # Mock the necessary data
        ticket_id = 1
        comments_stream = MagicMock()
        comments_stream.is_selected.return_value = False

        # Mock the responses for get_objects
        async def mock_get_objects(session, ticket_id):
            return [
                {
                    "id": ticket_id,
                    "events": [{"type": "Comment", "id": f"comment_{ticket_id}"}],
                    "created_at": "2023-01-01T00:00:00Z",
                    "via": "web",
                    "metadata": {},
                }
            ]

        instance = streams.TicketAudits(None, {})
        instance.stream = "ticket_audits"

        # Run the sync method
        async def run_test():
            with patch.object(
                streams.TicketAudits, "get_objects", side_effect=mock_get_objects
            ):
                async with ClientSession() as session:
                    audit_records, comment_records = await instance.sync(
                        session, ticket_id, comments_stream
                    )

                    # Assertions
                    self.assertEqual(len(audit_records), 1)
                    self.assertEqual(len(comment_records), 0)
                    self.assertEqual(audit_records[0][1]["id"], 1)

        asyncio.run(run_test())

    @aioresponses()
    @patch("tap_zendesk.streams.zendesk_metrics.capture")
    @patch("tap_zendesk.streams.LOGGER.warning")
    @patch(
        "tap_zendesk.streams.TicketAudits.get_objects",
        side_effect=http.ZendeskNotFoundError,
    )
    def test_audit_not_found(
        self, mocked, mock_capture, mock_warning, mock_get_objects
    ):
        """
        Test that sync handles the case where the ticket is not found.
        """
        
        # Mock the necessary data
        ticket_id = 1

        comments_stream = MagicMock()
        comments_stream.is_selected.return_value = True

        instance = streams.TicketAudits("client", {})

        async def run_test():
            # Run the sync method
            async with ClientSession() as session:
                audit_records, comment_records = await instance.sync(
                    session, ticket_id, comments_stream
                )

                # Assertions
                self.assertEqual(len(audit_records), 0)
                self.assertEqual(len(comment_records), 0)
                mock_warning.assert_called_once_with(
                    "Unable to retrieve metrics for ticket (ID: %s), record not found",
                    ticket_id,
                )

        asyncio.run(run_test())

    @aioresponses()
    @patch("tap_zendesk.streams.zendesk_metrics.capture")
    @patch("tap_zendesk.streams.LOGGER.warning")
    @patch(
        "tap_zendesk.streams.TicketAudits.get_objects",
        side_effect=http.ZendeskInternalServerError(
            "The server encountered an unexpected condition which prevented it from fulfilling the request."
        ),
    )
    def test_paginate_ticket_audits_exception(
        self, mocked, mock_capture, mock_warning, mock_get_objects
    ):
        """
        Test that sync handles generic exceptions thrown by paginate_ticket_audits method.
        """
        # Mock the necessary data
        ticket_id = 1

        comments_stream = MagicMock()
        comments_stream.is_selected.return_value = True

        instance = streams.TicketAudits("client", {})

        async def run_test():
            # Run the sync method
            async with ClientSession() as session:
                with self.assertRaises(http.ZendeskError) as context:
                    audit_records, comment_records = await instance.sync(
                        session, ticket_id, comments_stream
                    )

            self.assertEqual(str(
                context.exception), "The server encountered an unexpected condition which prevented it from fulfilling the request.")

        asyncio.run(run_test())
