import unittest
from unittest.mock import patch, MagicMock
import asyncio
from aiohttp import ClientSession

from tap_zendesk import http, streams


class TestASyncTicketAudits(unittest.TestCase):

    @patch("tap_zendesk.streams.zendesk_metrics.capture")
    @patch("tap_zendesk.streams.LOGGER.warning")
    def test_sync_audit_comment_both_selected(self, mock_capture, mock_warning):
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
                    self.assertEqual(comment_records[0][1]["ticket_id"], ticket_id)

        asyncio.run(run_test())

    @patch("tap_zendesk.streams.zendesk_metrics.capture")
    @patch("tap_zendesk.streams.LOGGER.warning")
    def test_sync_comment_only_selected(self, mock_capture, mock_warning):
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

    @patch("tap_zendesk.streams.zendesk_metrics.capture")
    @patch("tap_zendesk.streams.LOGGER.warning")
    def test_sync_audit_only_selected(self, mock_capture, mock_warning):
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

    @patch("tap_zendesk.streams.Tickets.update_bookmark")
    @patch("tap_zendesk.streams.Tickets.get_bookmark")
    @patch("tap_zendesk.streams.Tickets.get_objects")
    @patch("tap_zendesk.streams.Tickets.check_access")
    @patch("tap_zendesk.streams.singer.write_state")
    @patch("tap_zendesk.streams.zendesk_metrics.capture")
    @patch("tap_zendesk.streams.LOGGER.info")
    def test_sync_audits_comments_stream__both_not_selected(
        self,
        mock_info,
        mock_capture,
        mock_write_state,
        mock_check_access,
        mock_get_objects,
        mock_get_bookmark,
        mock_update_bookmark,
    ):
        """
        Test that sync does not extract records for audits and comments when both of them are not selected.
        """
        # Mock the necessary data
        state = {}
        bookmark = "2023-01-01T00:00:00Z"
        tickets = [
            {"id": 1, "generated_timestamp": 1672531200, "fields": "duplicate"},
            {"id": 2, "generated_timestamp": 1672531300, "fields": "duplicate"},
        ]
        mock_get_bookmark.return_value = bookmark
        mock_get_objects.return_value = tickets

        # Create an instance of the Tickets class
        instance = streams.Tickets(None, {})
        audits_stream = streams.TicketAudits(None, {})
        comments_stream = streams.TicketComments(None, {})
        instance.audits_stream = audits_stream
        instance.comments_stream = comments_stream

        # Run the sync method
        result = list(instance.sync(state))

        # Assertions
        self.assertEqual(len(result), 2)

    @patch("tap_zendesk.streams.zendesk_metrics.capture")
    @patch("tap_zendesk.streams.LOGGER.warning")
    @patch(
        "tap_zendesk.streams.TicketAudits.get_objects",
        side_effect=http.ZendeskNotFoundError,
    )
    def test_audit_not_found(self, mock_capture, mock_warning, mock_get_objects):
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

        asyncio.run(run_test())

    @patch("tap_zendesk.streams.zendesk_metrics.capture")
    @patch("tap_zendesk.streams.LOGGER.warning")
    @patch(
        "tap_zendesk.streams.TicketAudits.get_objects",
        side_effect=http.ZendeskInternalServerError(
            "The server encountered an unexpected condition which prevented it from fulfilling the request."
        ),
    )
    def test_paginate_ticket_audits_exception(
        self, mock_capture, mock_warning, mock_get_objects
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

            self.assertEqual(
                str(context.exception),
                "The server encountered an unexpected condition which prevented it from fulfilling the request.",
            )

        asyncio.run(run_test())
