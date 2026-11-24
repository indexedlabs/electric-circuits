import unittest

from data import IMPORT_TRANSACTIONS, LARGE_ROWS, row_id


class WorkloadCountsTests(unittest.TestCase):
    def test_transaction_targets(self):
        self.assertEqual(sum(IMPORT_TRANSACTIONS), 13081)
        self.assertEqual(len(IMPORT_TRANSACTIONS), 6)
        self.assertEqual(min(IMPORT_TRANSACTIONS), 2086)
        self.assertEqual(max(IMPORT_TRANSACTIONS), 2461)
        self.assertEqual(sum(LARGE_ROWS.values()), 29793)

    def test_synthetic_accounts_are_disjoint(self):
        self.assertNotEqual(row_id("record", 0), row_id("record", 19))
        self.assertEqual(row_id("user", 0), row_id("user", 0))


if __name__ == "__main__":
    unittest.main()
