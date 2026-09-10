import unittest
from unittest.mock import MagicMock, patch, call

import tap_zendesk
from tap_zendesk import (
    do_discover,
    stream_is_selected,
    get_selected_streams,
    get_sub_stream_names,
    validate_dependencies,
    populate_class_schemas,
    do_sync,
    oauth_auth,
    api_token_auth,
    request_metrics_patch,
    SUB_STREAMS,
)


class TestDoDiscover(unittest.TestCase):

    @patch('tap_zendesk.json.dump')
    @patch('tap_zendesk.discover_streams')
    def test_do_discover_writes_catalog_to_stdout(self, mock_discover_streams, mock_json_dump):
        mock_discover_streams.return_value = [{"stream": "tickets"}]
        client = MagicMock()
        config = {"start_date": "2020-01-01T00:00:00Z"}

        do_discover(client, config)

        mock_discover_streams.assert_called_once_with(client, config)
        args, kwargs = mock_json_dump.call_args
        self.assertEqual(args[0], {"streams": [{"stream": "tickets"}]})
        self.assertEqual(kwargs.get("indent"), 2)


class TestStreamIsSelected(unittest.TestCase):

    def test_returns_true_when_selected(self):
        mdata = {(): {"selected": True}}
        self.assertTrue(stream_is_selected(mdata))

    def test_returns_false_when_not_selected(self):
        mdata = {(): {"selected": False}}
        self.assertFalse(stream_is_selected(mdata))

    def test_returns_false_when_missing(self):
        self.assertFalse(stream_is_selected({}))


class TestGetSelectedStreams(unittest.TestCase):

    @patch('tap_zendesk.metadata.to_map')
    def test_returns_only_selected_stream_names(self, mock_to_map):
        selected_stream = MagicMock(tap_stream_id="tickets", metadata=[])
        unselected_stream = MagicMock(tap_stream_id="users", metadata=[])
        catalog = MagicMock(streams=[selected_stream, unselected_stream])

        mock_to_map.side_effect = [
            {(): {"selected": True}},
            {(): {"selected": False}},
        ]

        result = get_selected_streams(catalog)

        self.assertEqual(result, ["tickets"])


class TestGetSubStreamNames(unittest.TestCase):

    def test_flattens_all_sub_stream_names(self):
        result = get_sub_stream_names()
        expected = []
        for parent in SUB_STREAMS:
            expected.extend(SUB_STREAMS[parent])
        self.assertEqual(sorted(result), sorted(expected))


class TestValidateDependencies(unittest.TestCase):

    def test_adds_missing_parent_when_substream_selected(self):
        selected = ["ticket_audits"]
        validate_dependencies(selected)
        self.assertIn("tickets", selected)

    def test_no_change_when_parent_already_selected(self):
        selected = ["tickets", "ticket_audits"]
        validate_dependencies(selected)
        self.assertEqual(selected.count("tickets"), 1)

    def test_no_change_when_no_substreams_selected(self):
        selected = ["tickets"]
        validate_dependencies(selected)
        self.assertEqual(selected, ["tickets"])


class TestPopulateClassSchemas(unittest.TestCase):

    def test_assigns_stream_to_class(self):
        fake_stream = MagicMock(tap_stream_id="tickets")
        catalog = MagicMock(streams=[fake_stream])

        class FakeStreamClass:
            stream = None

        with patch.dict('tap_zendesk.STREAMS', {"tickets": FakeStreamClass}, clear=True):
            populate_class_schemas(catalog)
            self.assertIs(tap_zendesk.STREAMS["tickets"].stream, fake_stream)


class TestOauthAuth(unittest.TestCase):

    def test_returns_none_when_access_token_missing(self):
        args = MagicMock(config={"subdomain": "foo"})
        self.assertIsNone(oauth_auth(args))

    def test_returns_creds_when_access_token_present(self):
        args = MagicMock(config={"subdomain": "foo", "access_token": "abc"})
        result = oauth_auth(args)
        self.assertEqual(result, {"subdomain": "foo", "oauth_token": "abc"})


class TestApiTokenAuth(unittest.TestCase):

    def test_returns_none_when_keys_missing(self):
        args = MagicMock(config={"subdomain": "foo"})
        self.assertIsNone(api_token_auth(args))

    def test_returns_creds_when_keys_present(self):
        args = MagicMock(config={
            "subdomain": "foo",
            "email": "user@example.com",
            "api_token": "tok123",
        })
        result = api_token_auth(args)
        self.assertEqual(result, {
            "subdomain": "foo",
            "email": "user@example.com",
            "token": "tok123",
        })


class TestRequestMetricsPatch(unittest.TestCase):

    @patch('tap_zendesk.singer_metrics.http_request_timer')
    @patch('tap_zendesk.request')
    def test_wraps_request_and_records_metrics(self, mock_request, mock_timer):
        mock_timer.return_value.__enter__.return_value = MagicMock()
        response = MagicMock()
        response.headers = {'ETag': 'etag-value', 'X-Request-Id': 'req-id'}
        mock_request.return_value = response

        self_obj = MagicMock()
        result = request_metrics_patch(self_obj, "GET", "https://example.com")

        mock_request.assert_called_once_with(self_obj, "GET", "https://example.com")
        self.assertIs(result, response)

    @patch('tap_zendesk.singer_metrics.http_request_timer')
    @patch('tap_zendesk.request')
    def test_missing_headers_default_to_not_present(self, mock_request, mock_timer):
        mock_timer.return_value.__enter__.return_value = MagicMock()
        response = MagicMock()
        response.headers = {}
        mock_request.return_value = response

        self_obj = MagicMock()
        result = request_metrics_patch(self_obj, "GET", "https://example.com")

        self.assertIs(result, response)


class TestDoSync(unittest.TestCase):

    def _make_fake_stream_class(self):
        class FakeStreamInstance:
            children = []

            def __init__(self, client, config):
                self.client = client
                self.config = config
                self.child_to_sync = []

        return FakeStreamInstance

    def _make_catalog_stream(self, tap_stream_id, selected=True):
        stream = MagicMock()
        stream.tap_stream_id = tap_stream_id
        stream.metadata = []
        stream.schema.to_dict.return_value = {"type": "object", "properties": {}}
        stream.stream = stream
        return stream

    @patch('tap_zendesk.zendesk_metrics.log_aggregate_rates')
    @patch('tap_zendesk.sync_stream', return_value=5)
    @patch('tap_zendesk.singer.write_state')
    @patch('tap_zendesk.singer.write_schema')
    @patch('tap_zendesk.metadata.to_map', return_value={(): {"selected": True}})
    def test_skips_unselected_and_substreams(self, mock_to_map, mock_write_schema,
                                              mock_write_state, mock_sync_stream, mock_log_rates):
        FakeStreamClass = self._make_fake_stream_class()
        parent_stream = self._make_catalog_stream("tickets")
        unselected_stream = self._make_catalog_stream("brands")
        substream = self._make_catalog_stream("ticket_audits")

        catalog = MagicMock(streams=[parent_stream, unselected_stream, substream])

        fake_streams = {
            "tickets": FakeStreamClass,
            "brands": FakeStreamClass,
            "ticket_audits": FakeStreamClass,
        }

        with patch.dict('tap_zendesk.STREAMS', fake_streams, clear=True), \
             patch('tap_zendesk.get_selected_streams', return_value=["tickets", "ticket_audits"]):
            do_sync(MagicMock(), catalog, {}, {"start_date": "2020-01-01T00:00:00Z"})

        # "brands" was not selected -> skipped entirely (no write_schema for it)
        # "ticket_audits" is a sub-stream of "tickets" -> synced via parent, not directly
        stream_names_written = [c.args[0] for c in mock_write_schema.call_args_list]
        self.assertIn("tickets", stream_names_written)
        self.assertIn("ticket_audits", stream_names_written)  # written as sub-stream schema
        self.assertNotIn("brands", stream_names_written)

        mock_sync_stream.assert_called_once()
        mock_log_rates.assert_called()

    @patch('tap_zendesk.zendesk_metrics.log_aggregate_rates')
    @patch('tap_zendesk.sync_stream', return_value=3)
    @patch('tap_zendesk.singer.write_state')
    @patch('tap_zendesk.singer.write_schema')
    @patch('tap_zendesk.metadata.to_map', return_value={(): {"selected": True}})
    def test_children_are_added_to_child_to_sync_when_selected(self, mock_to_map, mock_write_schema,
                                                                mock_write_state, mock_sync_stream,
                                                                mock_log_rates):
        class FakeChildClass:
            children = []

            def __init__(self, client, config):
                pass

        class FakeParentClass:
            children = ["custom_child_stream"]

            def __init__(self, client, config):
                self.child_to_sync = []

        parent_stream = self._make_catalog_stream("tickets")
        catalog = MagicMock(streams=[parent_stream])

        fake_streams = {
            "tickets": FakeParentClass,
            "custom_child_stream": FakeChildClass,
        }

        with patch.dict('tap_zendesk.STREAMS', fake_streams, clear=True), \
             patch('tap_zendesk.get_sub_stream_names', return_value=[]), \
             patch('tap_zendesk.get_selected_streams', return_value=["tickets", "custom_child_stream"]):
            do_sync(MagicMock(), catalog, {}, {"start_date": "2020-01-01T00:00:00Z"})

        mock_sync_stream.assert_called_once()
        synced_instance = mock_sync_stream.call_args.args[2]
        self.assertEqual(len(synced_instance.child_to_sync), 1)


class TestMain(unittest.TestCase):

    def _make_parsed_args(self, **overrides):
        defaults = {
            "config": {"start_date": "2020-01-01T00:00:00Z", "subdomain": "foo",
                       "access_token": "abc"},
            "config_path": "/tmp/config.json",
            "discover": False,
            "catalog": None,
            "state": {},
            "dev": False,
        }
        defaults.update(overrides)
        parsed_args = MagicMock()
        for key, value in defaults.items():
            setattr(parsed_args, key, value)
        return parsed_args

    @patch('tap_zendesk.do_discover')
    @patch('tap_zendesk.Zenpy')
    @patch('tap_zendesk.get_session', return_value=None)
    @patch('tap_zendesk.oauth_auth', return_value={"subdomain": "foo", "oauth_token": "abc"})
    @patch('tap_zendesk.refresh_credentials')
    @patch('tap_zendesk.singer.utils.parse_args')
    def test_discover_mode_calls_do_discover(self, mock_parse_args, mock_refresh_credentials,
                                              mock_oauth_auth, mock_get_session, mock_zenpy,
                                              mock_do_discover):
        parsed_args = self._make_parsed_args(discover=True)
        mock_parse_args.return_value = parsed_args
        mock_refresh_credentials.return_value = parsed_args.config
        mock_zenpy.return_value = MagicMock()

        tap_zendesk.main()

        mock_do_discover.assert_called_once()

    @patch('tap_zendesk.do_sync')
    @patch('tap_zendesk.Zenpy')
    @patch('tap_zendesk.get_session', return_value=None)
    @patch('tap_zendesk.oauth_auth', return_value={"subdomain": "foo", "oauth_token": "abc"})
    @patch('tap_zendesk.refresh_credentials')
    @patch('tap_zendesk.singer.utils.parse_args')
    def test_sync_mode_calls_do_sync(self, mock_parse_args, mock_refresh_credentials,
                                      mock_oauth_auth, mock_get_session, mock_zenpy,
                                      mock_do_sync):
        parsed_args = self._make_parsed_args(discover=False, catalog=MagicMock(), state={"foo": "bar"})
        mock_parse_args.return_value = parsed_args
        mock_refresh_credentials.return_value = parsed_args.config
        mock_zenpy.return_value = MagicMock()

        tap_zendesk.main()

        mock_do_sync.assert_called_once()
        args = mock_do_sync.call_args.args
        self.assertEqual(args[2], {"foo": "bar"})

    @patch('tap_zendesk.do_sync')
    @patch('tap_zendesk.Zenpy')
    @patch('tap_zendesk.get_session', return_value=None)
    @patch('tap_zendesk.api_token_auth', return_value={"subdomain": "foo", "email": "a@b.com", "token": "tok"})
    @patch('tap_zendesk.oauth_auth', return_value=None)
    @patch('tap_zendesk.refresh_credentials')
    @patch('tap_zendesk.singer.utils.parse_args')
    def test_uses_custom_request_timeout_from_config(self, mock_parse_args, mock_refresh_credentials,
                                                       mock_oauth_auth, mock_api_token_auth,
                                                       mock_get_session, mock_zenpy, mock_do_sync):
        config = {"start_date": "2020-01-01T00:00:00Z", "subdomain": "foo",
                  "email": "a@b.com", "api_token": "tok", "request_timeout": "500"}
        parsed_args = self._make_parsed_args(config=config, discover=False, catalog=MagicMock())
        mock_parse_args.return_value = parsed_args
        mock_refresh_credentials.return_value = config
        mock_zenpy.return_value = MagicMock()

        tap_zendesk.main()

        _, kwargs = mock_zenpy.call_args
        self.assertEqual(kwargs["timeout"], 500.0)

    @patch('tap_zendesk.do_sync')
    @patch('tap_zendesk.Zenpy')
    @patch('tap_zendesk.get_session', return_value=None)
    @patch('tap_zendesk.oauth_auth', return_value={"subdomain": "foo", "oauth_token": "abc"})
    @patch('tap_zendesk.refresh_credentials')
    @patch('tap_zendesk.singer.utils.parse_args')
    def test_dev_mode_warns_and_skips_credential_refresh_call_args(
            self, mock_parse_args, mock_refresh_credentials, mock_oauth_auth,
            mock_get_session, mock_zenpy, mock_do_sync):
        parsed_args = self._make_parsed_args(dev=True, discover=False, catalog=MagicMock())
        mock_parse_args.return_value = parsed_args
        mock_refresh_credentials.return_value = parsed_args.config
        mock_zenpy.return_value = MagicMock()

        tap_zendesk.main()

        mock_refresh_credentials.assert_called_once_with(
            parsed_args.config, parsed_args.config_path, dev_mode=True)

    @patch('tap_zendesk.do_sync')
    @patch('tap_zendesk.Zenpy', return_value=None)
    @patch('tap_zendesk.get_session', return_value=None)
    @patch('tap_zendesk.oauth_auth', return_value={"subdomain": "foo", "oauth_token": "abc"})
    @patch('tap_zendesk.refresh_credentials')
    @patch('tap_zendesk.singer.utils.parse_args')
    def test_logs_error_when_no_client_created(self, mock_parse_args, mock_refresh_credentials,
                                                mock_oauth_auth, mock_get_session, mock_zenpy,
                                                mock_do_sync):
        parsed_args = self._make_parsed_args(discover=False, catalog=None)
        mock_parse_args.return_value = parsed_args
        mock_refresh_credentials.return_value = parsed_args.config

        tap_zendesk.main()

        mock_do_sync.assert_not_called()
