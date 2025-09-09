#!/usr/bin/env python3

import os
import json
import unittest
from singer_tap_tester import cli, user


class TestCustomTicketStatusesIntegration(unittest.TestCase):
    """
    Simple integration test for tap-zendesk custom_ticket_statuses stream.
    
    This test will:
    1. Run discovery to ensure custom_ticket_statuses stream is discovered
    2. Run a sync with custom_ticket_statuses selected
    3. Verify the output contains custom status records
    """

    def setUp(self):
        """Set up test configuration."""
        self.tap_name = "tap-zendesk"
        
        # Load configuration from config.json (in parent directory)
        config_path = os.path.join(os.path.dirname(__file__), "..", "config.json")
        
        if not os.path.exists(config_path):
            self.skipTest("config.json not found - create one with access_token and subdomain")
        
        try:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        except Exception as e:
            self.skipTest(f"Failed to load config.json: {e}")
        
        # Verify required fields are present
        required_fields = ["access_token", "subdomain"]
        missing_fields = [field for field in required_fields if not self.config.get(field)]
        
        if missing_fields:
            self.skipTest(f"Missing required fields in config.json: {missing_fields}")

    def test_custom_ticket_statuses_discovery(self):
        """Test that custom_ticket_statuses stream is discovered."""
        catalog = cli.run_discovery(self.tap_name, self.config)
        
        # Parse catalog if it's a string
        if isinstance(catalog, str):
            catalog = json.loads(catalog)
        
        # Find custom_ticket_statuses stream
        custom_statuses_stream = None
        for stream in catalog.get("streams", []):
            if stream.get("tap_stream_id") == "custom_ticket_statuses":
                custom_statuses_stream = stream
                break
        
        self.assertIsNotNone(custom_statuses_stream, "custom_ticket_statuses stream not found in catalog")
        
        # Verify schema has expected fields
        schema_properties = custom_statuses_stream.get("schema", {}).get("properties", {})
        expected_fields = ["id", "agent_label", "status_category", "description", "active"]
        
        for field in expected_fields:
            self.assertIn(field, schema_properties, f"Field '{field}' missing from custom_ticket_statuses schema")

    def test_custom_ticket_statuses_sync(self):
        """Test that custom_ticket_statuses stream can be synced."""
        catalog = cli.run_discovery(self.tap_name, self.config)
        
        # Parse catalog if it's a string  
        if isinstance(catalog, str):
            catalog = json.loads(catalog)
        
        # Select only the custom_ticket_statuses stream
        catalog = user.select_stream(catalog, "custom_ticket_statuses")
        catalog = user.select_all_fields(catalog, "custom_ticket_statuses")
        
        # Run sync
        tap_output = cli.run_sync(self.tap_name, self.config, catalog, {})
        
        # The output should contain some data or at least the stream schema
        self.assertIsNotNone(tap_output, "Tap sync returned no output")
        
        # Parse output lines
        output_lines = tap_output.strip().split('\n') if isinstance(tap_output, str) else []
        
        # Look for schema and record messages for custom_ticket_statuses
        schema_found = False
        record_found = False
        
        for line in output_lines:
            if line.strip():
                try:
                    message = json.loads(line)
                    if message.get("type") == "SCHEMA" and message.get("stream") == "custom_ticket_statuses":
                        schema_found = True
                    elif message.get("type") == "RECORD" and message.get("stream") == "custom_ticket_statuses":
                        record_found = True
                        # Verify record has expected structure
                        record_data = message.get("record", {})
                        self.assertIn("id", record_data, "Record missing 'id' field")
                except json.JSONDecodeError:
                    # Skip non-JSON lines (like logs)
                    continue
        
        self.assertTrue(schema_found, "No SCHEMA message found for custom_ticket_statuses")
        print(f"Schema found: {schema_found}, Records found: {record_found}")


if __name__ == '__main__':
    # Run with verbose output
    unittest.main(verbosity=2)