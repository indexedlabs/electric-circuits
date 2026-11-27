import asyncio
import json
import unittest
from pathlib import Path

from catalog import GatewayShapes, contains_subquery, validate_predicate
from parity import check_fixtures
from simple_keys import H1P_VARIANTS, HIDDEN_RECORD_TYPES, SIMPLE_KEYED, AccountLists, access_leaf, h1_catalog, h1p_catalog, predicate_sql, simple_bodies, simple_feeds, unwrap_single_or

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


class PlainWordsTests(unittest.TestCase):
    """PLAIN-WORDS-3 run 1: the hypothetical plain-test fix over main's lowering."""

    def setUp(self):
        self.main = GatewayShapes(FOLDER / "indexed-source-52e79b8.json.gz")

    def test_unwrap_only_touches_one_child_ors(self):
        leaf = {"col": "space_id", "op": "eq", "value": "x"}
        self.assertEqual(unwrap_single_or({"and": [{"or": [{"or": [leaf]}]}, {"or": [leaf, leaf]}]}), {"and": [leaf, {"or": [leaf, leaf]}]})

    def test_leaf_is_the_first_equality_in_the_top_level_and(self):
        first, second = {"col": "a", "op": "eq", "value": 1}, {"col": "b", "op": "eq", "value": 2}
        bound = {"col": "c", "op": "gte", "value": 3}
        self.assertEqual(access_leaf({"and": [bound, {"and": [first, second]}]}), ("a", "eq", 1))
        self.assertEqual(access_leaf({"and": [{"or": [first]}, bound]}), ("c", "gte", 3))
        self.assertIsNone(access_leaf({"or": [first, second]}))

    def test_today_no_keyed_record_feed_is_indexed_and_the_fix_indexes_one_family_accounts(self):
        today = asyncio.run(h1_catalog(self.main, 40, False))
        self.assertEqual({access_leaf(request["body"]["where"]) for request in today}, {None})
        control = asyncio.run(h1p_catalog(self.main, *H1P_VARIANTS["S-main3-h1p-40-one-today"]))
        self.assertEqual({access_leaf(request["body"]["where"]) for request in control}, {None})
        fixed = asyncio.run(h1p_catalog(self.main, *H1P_VARIANTS["S-main3-h1p-40-one"]))
        self.assertEqual({access_leaf(request["body"]["where"])[0] for request in fixed}, {"space_id"})
        self.assertEqual(len({json.dumps(request["body"]["where"]) for request in fixed}), 40)
        mix = asyncio.run(h1p_catalog(self.main, *H1P_VARIANTS["S-main3-h1p-40-mix"]))
        self.assertEqual(sum(access_leaf(request["body"]["where"]) is None for request in mix), 2)


if __name__ == "__main__":
    unittest.main()
