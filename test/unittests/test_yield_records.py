from tap_zendesk import LOGGER, oauth_auth
import unittest
from unittest import mock
from unittest.mock import patch
from tap_zendesk.streams import Stream, Tickets, zendesk_metrics
from tap_zendesk.sync import sync_stream
from tap_zendesk import Zenpy
import json
     
class Zenpy():
    def __init__(self) -> None:
        pass

def mocked_sync_audits(ticket_id=None):
    ticket_audits = [
    {
        "author_id":387494208358,
        "created_at":"2021-10-11T12:23:20.000000Z",
        "id":910518732098,
        "ticket_id":2
    },
    {
        "author_id":387494208358,
        "created_at":"2021-10-11T12:24:05.000000Z",
        "id":910519204898,
        "ticket_id":2,
    }
    ]
    for audit in ticket_audits:
        yield ('ticket_audits', audit)

def mocked_sync_metrics(ticket_id=None):
    ticket_metrics = [
    {
        "author_id":387494208358,
        "created_at":"2021-10-11T12:23:20.000000Z",
        "id":910518732090,
        "ticket_id":2
    },
    {
        "author_id":387494208358,
        "created_at":"2021-10-11T12:24:05.000000Z",
        "id":910519204892,
        "ticket_id":2,
    }
    ]
    for metric in ticket_metrics:
        yield ('ticket_metrics', metric)

def mocked_sync_comments(ticket_id=None):
    ticket_comments = [
    {
        "author_id":387494208356,
        "created_at":"2021-10-11T12:23:20.000000Z",
        "id":910518732090,
        "ticket_id":2
    },
    {
        "author_id":387494208354,
        "created_at":"2021-10-11T12:24:05.000000Z",
        "id":910519204892,
        "ticket_id":2,
    }
    ]
    for comment in ticket_comments:
        yield ('ticket_comments', comment)

@mock.patch('tap_zendesk.streams.Stream.update_bookmark')
@mock.patch('tap_zendesk.streams.Stream.get_bookmark')
@mock.patch('tap_zendesk.streams.TicketAudits.is_selected')
@mock.patch('tap_zendesk.streams.TicketMetrics.is_selected')
@mock.patch('tap_zendesk.streams.TicketComments.is_selected')
@mock.patch('tap_zendesk.streams.TicketAudits.sync')
@mock.patch('tap_zendesk.streams.TicketMetrics.sync')
@mock.patch('tap_zendesk.streams.TicketComments.sync')
@mock.patch('tap_zendesk.streams.CursorBasedExportStream.get_objects')
def test_yield_records(mock_objects, mock_comments_sync, mock_metrics_sync, mock_audits_sync, mock_comments, mock_metrics, mock_audits, mock_get_bookmark, mock_update_bookmark):
    ticket_stream = Tickets(Zenpy(), {})
    tickets = [{
        "url":"https://talend1234.zendesk.com/api/v2/tickets/1.json",
        "id":2,
        "external_id":"None",
        "created_at":"2021-10-11T12:12:31Z",
        "updated_at":"2021-10-12T08:37:28Z",
        "requester_id":387331462257,
        "submitter_id":387494208358,
        "assignee_id":387494208358,
        "organization_id":"None",
        "group_id":360010350357,
        "due_at":"None",
        "ticket_form_id":360003740737,
        "brand_id":360004806057,
        "generated_timestamp":1634027848,
        "fields": []
    }]
    mock_objects.return_value = tickets
    expected_audits = [
        {
            "author_id":387494208358,
            "created_at":"2021-10-11T12:23:20.000000Z",
            "id":910518732098,
            "ticket_id":2
        },
        {
            "author_id":387494208358,
            "created_at":"2021-10-11T12:24:05.000000Z",
            "id":910519204898,
            "ticket_id":2,
        }
    ]
    expected_metrics = [
    {
        "author_id":387494208358,
        "created_at":"2021-10-11T12:23:20.000000Z",
        "id":910518732090,
        "ticket_id":2
    },
    {
        "author_id":387494208358,
        "created_at":"2021-10-11T12:24:05.000000Z",
        "id":910519204892,
        "ticket_id":2,
    }
    ]
    expected_comments = [
    {
        "author_id":387494208356,
        "created_at":"2021-10-11T12:23:20.000000Z",
        "id":910518732090,
        "ticket_id":2
    },
    {
        "author_id":387494208354,
        "created_at":"2021-10-11T12:24:05.000000Z",
        "id":910519204892,
        "ticket_id":2,
    }
    ]
    mock_metrics.return_value = True
    mock_audits.return_value = True
    mock_comments.return_value = True
    mock_update_bookmark.side_effect = None
    mock_metrics_sync.side_effect = mocked_sync_metrics
    mock_audits_sync.side_effect = mocked_sync_audits
    mock_comments_sync.side_effect = mocked_sync_comments

    expected_tickets = list(ticket_stream.sync(state={}))
    audits = []
    metrics = []
    comments = []

    for count, each in enumerate(expected_tickets):
        if count == 0:
            continue
        if each[0] == 'ticket_audits':
            audits.append(each[1])
        if each[0] == 'ticket_metrics':
            metrics.append(each[1])
        if each[0] == 'ticket_comments':
            comments.append(each[1])
    assert expected_audits == audits
    assert expected_metrics == metrics
    assert expected_comments == comments
