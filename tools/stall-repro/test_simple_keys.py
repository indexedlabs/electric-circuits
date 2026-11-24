import asyncio
import json
import unittest
from pathlib import Path

from catalog import GatewayShapes, contains_subquery, validate_predicate
from parity import check_fixtures
from simple_keys import HIDDEN_RECORD_TYPES, SIMPLE_KEYED, AccountLists, predicate_sql, simple_bodies, simple_feeds

FOLDER = Path(__file__).parent
LISTS = AccountLists(0, "u", ("s0",), ("s1",), ("g",), ("c0", "c1"), ("r0", "r1"))


class SimpleKeysTests(unittest.TestCase):
    def setUp(self):
        self.gateway = GatewayShapes(FOLDER / "indexed-source.json.gz")

    def test_hidden_types_match_the_gateway(self):
        hidden = self.gateway.loader.module("api.models.record").CLIENT_HIDDEN_RECORD_TYPES
        self.assertEqual({member.value for member in hidden}, set(HIDDEN_RECORD_TYPES))

    def test_every_feed_is_plain_and_names_known_columns(self):
        columns = {table: {column["name"] for column in declared} for table, declared in json.loads((FOLDER / "results/fixture/columns.json").read_text()).items()}
        columns["assignment"].add("space_id")
        for table in SIMPLE_KEYED:
            for edge_shares in (False, True):
                for _, predicate in simple_feeds(self.gateway, table, LISTS, edge_shares=edge_shares):
                    self.assertFalse(contains_subquery(predicate))
                    validate_predicate(predicate, table, columns)
                    self.assertNotIn("audience_key", json.dumps(predicate))
                    predicate_sql(predicate, "q")

    def test_one_shape_per_space_or_calendar_and_shares_only_where_stated(self):
        counts = {table: len(simple_feeds(self.gateway, table, LISTS)) for table in SIMPLE_KEYED}
        self.assertEqual(counts, {**{table: 3 for table in SIMPLE_KEYED}, "graph_edge": 2})
        self.assertEqual(len(simple_feeds(self.gateway, "graph_edge", LISTS, edge_shares=True)), 3)

    def test_subscriptions_are_distinct(self):
        bodies = [body for table in SIMPLE_KEYED for body in asyncio.run(simple_bodies(self.gateway, "S-edge-shares", table, 0, LISTS))]
        self.assertEqual(len({body["subscription"] for body in bodies}), len(bodies))

    def test_fixture_check_reports_surprises_and_missed_drift(self):
        labels = {("record", "x"): {"label": "l", "drift": True, "expect": {(0, "b")}}, ("record", "y"): {"label": "m", "drift": False, "expect": set()}}
        difference = {"account": 0, "table": "record", "side": "b", "key": "x", "reason": "r"}
        report = check_fixtures({"differences": [difference], "drift_check": {"record": {"ids": ["x"]}}}, labels)
        self.assertEqual((report["surprises"], report["drift_rows_found"]), ([], 1))
        report = check_fixtures({"differences": [{**difference, "key": "y"}], "drift_check": {}}, labels)
        self.assertEqual((len(report["surprises"]), report["drift_rows_missed"]), (2, [["record", "x"]]))


if __name__ == "__main__":
    unittest.main()
