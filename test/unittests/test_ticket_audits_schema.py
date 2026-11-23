import json
import os
import unittest

from singer import Transformer
from singer.schema import Schema

SCHEMAS_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "tap_zendesk", "schemas")


def load_schema(name):
    with open(os.path.join(SCHEMAS_DIR, name), encoding="utf-8") as f:
        return json.load(f)


def transform(record, schema=None):
    schema = schema or load_schema("ticket_audits.json")
    with Transformer() as transformer:
        return transformer.transform(record, schema)


def audit_with_event(**event):
    return {"id": 1, "ticket_id": 2, "events": [{"id": 3, "type": "Change", **event}]}


class TestTicketAuditsEventValues(unittest.TestCase):
    """ `events[].value` / `previous_value` must be passed through without coercion. """

    def assert_passthrough(self, value):
        for field in ("value", "previous_value"):
            with self.subTest(field=field, value=value):
                out = transform(audit_with_event(**{field: value}))["events"][0][field]
                self.assertEqual(out, value)
                self.assertIs(type(out), type(value))

    def test_string_values(self):
        for value in ("open", "1", "false", ""):
            self.assert_passthrough(value)

    def test_integer_value(self):
        self.assert_passthrough(123)

    def test_float_and_boolean_values(self):
        for value in (1.5, True, False, 0):
            self.assert_passthrough(value)

    def test_null_value(self):
        self.assert_passthrough(None)

    def test_sla_object_is_valid_nested_json(self):
        value = {"minutes": 1440, "seconds": 86400, "in_business_hours": True}
        self.assert_passthrough(value)
        out = transform(audit_with_event(value=value))["events"][0]["value"]
        self.assertIs(out["in_business_hours"], True)
        # Must serialize to real JSON, not Python repr
        self.assertEqual(json.loads(json.dumps(out)), value)

    def test_object_with_undocumented_keys_is_preserved(self):
        self.assert_passthrough({"minutes": 5, "foo": {"bar": [1, {"x": None}]}, "flag": False})

    def test_empty_object_and_array(self):
        self.assert_passthrough({})
        self.assert_passthrough([])

    def test_arrays(self):
        for value in (["a", "b"], [1, 2], [{"id": 1, "ok": True}], ["a", {"b": 1}, None]):
            self.assert_passthrough(value)

    def test_documented_status_and_custom_status_samples(self):
        record = {"id": 1, "events": [
            {"field_name": "status", "id": 1564247, "previous_value": "new",
             "type": "Change", "value": "open"},
            {"field_name": "custom_status_id", "id": 1564248, "previous_value": 1,
             "type": "Change", "value": 123},
        ]}
        self.assertEqual(transform(record), record)

    def test_anyof_survives_catalog_round_trip(self):
        # Sync transforms against the schema stored in the catalog.
        schema = Schema.from_dict(load_schema("ticket_audits.json")).to_dict()
        value = {"minutes": 1440, "in_business_hours": True}
        out = transform(audit_with_event(value=value, previous_value=3), schema)["events"][0]
        self.assertEqual(out["value"], value)
        self.assertEqual(out["previous_value"], 3)


class TestTicketAuditsEventValueNullability(unittest.TestCase):
    """ `null` must be declared explicitly so downstream parsers see the fields as nullable. """

    FIELDS = ("value", "previous_value")

    def event_props(self, schema):
        return schema["properties"]["events"]["items"]["properties"]

    def test_null_branch_is_declared(self):
        for schema in (load_schema("ticket_audits.json"),
                       Schema.from_dict(load_schema("ticket_audits.json")).to_dict()):
            for field in self.FIELDS:
                with self.subTest(field=field):
                    self.assertIn({"type": "null"}, self.event_props(schema)[field]["anyOf"])

    def test_null_branch_is_after_catch_all(self):
        # `null` also matches "", so placing it before `{}` would turn "" into None.
        for field in self.FIELDS:
            with self.subTest(field=field):
                branches = self.event_props(load_schema("ticket_audits.json"))[field]["anyOf"]
                self.assertLess(branches.index({}), branches.index({"type": "null"}))

    def test_empty_string_is_not_converted_to_null(self):
        for field in self.FIELDS:
            with self.subTest(field=field):
                out = transform(audit_with_event(**{field: ""}))["events"][0][field]
                self.assertEqual(out, "")


class TestTicketAuditsAttributes(unittest.TestCase):
    """ Attributes returned by the API must not be dropped. """

    API_AUDIT = {
        "id": 123456789,
        "ticket_id": 98043,
        "author_id": -1,
        "metadata": {"system": {}, "custom": {}, "parent_audit_id": "10928485"},
        "events": [{
            "id": 12333344445,
            "type": "Change",
            "previous_value": None,
            "value": {"minutes": 1440, "seconds": 86400, "in_business_hours": True},
            "field_name": "first_reply_time",
            "via": {
                "source": {"rel": "sla_target_change"},
                "current_sla_policy": "CS Emails - 24 business hours First Reply Time",
            },
        }],
        "via": {"channel": "sla", "source": {"from": {}, "to": {}, "rel": None}},
    }

    def test_customer_reported_audit_is_unchanged(self):
        record = dict(self.API_AUDIT, created_at="2026-08-21T09:26:08Z")
        out = transform(record)
        out.pop("created_at")
        record.pop("created_at")
        self.assertEqual(out, record)

    def test_parent_audit_id_is_kept(self):
        out = transform(self.API_AUDIT)
        self.assertEqual(out["metadata"]["parent_audit_id"], "10928485")

    def test_current_sla_policy_is_kept(self):
        out = transform(self.API_AUDIT)
        self.assertEqual(out["events"][0]["via"]["current_sla_policy"],
                         "CS Emails - 24 business hours First Reply Time")

    def test_ticket_comments_keep_parent_audit_id(self):
        # Comments copy the audit's metadata, so the shared schema must keep it too.
        record = {"id": 1, "metadata": {"parent_audit_id": "42", "system": {}}}
        out = transform(record, resolve_refs(load_schema("ticket_comments.json")))
        self.assertEqual(out["metadata"]["parent_audit_id"], "42")


def resolve_refs(schema):
    """ Inline `shared/*.json` `$ref`s the way the tap's discovery does. """
    def walk(node):
        if isinstance(node, dict):
            if "$ref" in node:
                return walk(load_schema(node["$ref"]))
            return {k: walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(v) for v in node]
        return node
    return walk(schema)
