import unittest
from pathlib import Path

from catalog import ACCOUNT_WEIGHTS, SUBQUERY_TABLE_WEIGHTS, TARGETS, GatewayShapes, apportion, contains_subquery, validate_predicate


class CatalogPlanningTests(unittest.TestCase):
    def test_keyed_subscriptions_distinguish_table_and_calendar(self):
        gateway = GatewayShapes(Path(__file__).with_name("indexed-source.json.gz"))
        bodies = [gateway.keyed_body("record", 0), gateway.keyed_body("graph_edge", 0), gateway.keyed_body("calendar_event", 0, calendar=0), gateway.keyed_body("calendar_event", 0, calendar=1)]
        self.assertEqual(len({body["subscription"] for body in bodies}), len(bodies))

    def test_apportion_keeps_exact_counts(self):
        for day in TARGETS.values():
            self.assertEqual(sum(apportion(day["subquery_shapes"], SUBQUERY_TABLE_WEIGHTS).values()), day["subquery_shapes"])
            self.assertEqual(day["standalone"] + day["family_shapes"] + day["subquery_shapes"], day["shapes"])
        self.assertEqual(len(ACCOUNT_WEIGHTS), 19)

    def test_subquery_detection_is_recursive(self):
        self.assertTrue(contains_subquery({"and": [{"col": "id", "in": {"table": "record", "project": "id"}}]}))
        self.assertFalse(contains_subquery({"col": "id", "op": "eq", "value": "synthetic"}))

    def test_nested_columns_are_checked_against_their_own_table(self):
        columns = {"record": {"id"}, "projection": {"record_id", "day"}}
        validate_predicate({"col": "id", "in": {"table": "projection", "project": "record_id", "where": {"col": "day", "op": "eq", "value": "synthetic"}}}, "record", columns)
        with self.assertRaisesRegex(ValueError, "record.record_id"):
            validate_predicate({"col": "record_id", "op": "eq", "value": "synthetic"}, "record", columns)


if __name__ == "__main__":
    unittest.main()
