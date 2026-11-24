import unittest

from catalog import ACCOUNT_WEIGHTS, SUBQUERY_TABLE_WEIGHTS, TARGETS, apportion, contains_subquery


class CatalogPlanningTests(unittest.TestCase):
    def test_apportion_keeps_exact_counts(self):
        for day in TARGETS.values():
            self.assertEqual(sum(apportion(day["subquery_shapes"], SUBQUERY_TABLE_WEIGHTS).values()), day["subquery_shapes"])
            self.assertEqual(day["standalone"] + day["family_shapes"] + day["subquery_shapes"], day["shapes"])
        self.assertEqual(len(ACCOUNT_WEIGHTS), 19)

    def test_subquery_detection_is_recursive(self):
        self.assertTrue(contains_subquery({"and": [{"col": "id", "in": {"table": "record", "project": "id"}}]}))
        self.assertFalse(contains_subquery({"col": "id", "op": "eq", "value": "synthetic"}))


if __name__ == "__main__":
    unittest.main()
