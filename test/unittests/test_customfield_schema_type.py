import types
import unittest
from tap_zendesk.streams import process_custom_field


class TestCustomFieldSchemaDiscovery(unittest.TestCase):

    """
    Validates that all zendesk types are validated and converted to correct singer type formats
    """

    def get_z_field_obj(self, *params):
        field = types.SimpleNamespace()
        field.title, field.key, field.type = params
        return field

    def test_return_field_type_lookup(self):
        expected_singer_type = {"type" : ["integer", "null"]}
        z_field = self.get_z_field_obj("test_lookup_type","user_lookup_1","lookup")
        actual_singer_type = process_custom_field(z_field)
        self.assertEqual(actual_singer_type, expected_singer_type)

    def test_return_field_type_text(self):
        expected_singer_type = {"type" : ["string", "null"]}
        z_field = self.get_z_field_obj("user_name","name_field","text")
        actual_singer_type = process_custom_field(z_field)
        self.assertEqual(actual_singer_type, expected_singer_type)

    def test_return_field_type_textarea(self):
        expected_singer_type = {"type" : ["string", "null"]}
        z_field = self.get_z_field_obj("field_title_textarea","field_key_textarea","textarea")
        actual_singer_type = process_custom_field(z_field)
        self.assertEqual(actual_singer_type, expected_singer_type)

    def test_return_field_type_regexp(self):
        expected_singer_type = {"type" : ["string", "null"]}
        z_field = self.get_z_field_obj("field_title_regexp","field_key_regexp","regexp")
        actual_singer_type = process_custom_field(z_field)
        self.assertEqual(actual_singer_type, expected_singer_type)

    def test_return_field_type_dropdown(self):
        expected_singer_type = {"type" : ["string", "null"]}
        z_field = self.get_z_field_obj("field_title_dropdown","field_key_dropdown","regexp")
        actual_singer_type = process_custom_field(z_field)
        self.assertEqual(actual_singer_type, expected_singer_type)

    def test_return_field_type_decimal(self):
        expected_singer_type = {"type" : ["number", "null"]}
        z_field = self.get_z_field_obj("field_title_decimal","field_key_decimal","decimal")
        actual_singer_type = process_custom_field(z_field)
        self.assertEqual(actual_singer_type, expected_singer_type)

    def test_return_field_type_integer(self):
        expected_singer_type = {"type" : ["integer", "null"]}
        z_field = self.get_z_field_obj("field_title_integer","field_key_integer","integer")
        actual_singer_type = process_custom_field(z_field)
        self.assertEqual(actual_singer_type, expected_singer_type)

    def test_return_field_type_checkbox(self):
        expected_singer_type = {"type" : ["boolean", "null"]}
        z_field = self.get_z_field_obj("field_title_checkbox","field_key_checkbox","checkbox")
        actual_singer_type = process_custom_field(z_field)
        self.assertEqual(actual_singer_type, expected_singer_type)


    def test_return_field_type_date(self):
        expected_singer_type = {"type" : ["string", "null"], 'format': 'datetime'}
        z_field = self.get_z_field_obj("field_title_date","field_key_date","date")
        actual_singer_type = process_custom_field(z_field)
        self.assertEqual(actual_singer_type, expected_singer_type)

    def test_return_field_type_unidentified(self):
        expected_singer_type = {"type" : ["string", "null"]}
        z_field = self.get_z_field_obj("test_UNINDETIFIED_TYPE","UNINDETIFIED_TYPE","UNINDETIFIED_TYPE")
        actual_singer_type = process_custom_field(z_field)
        self.assertEqual(actual_singer_type, expected_singer_type)
