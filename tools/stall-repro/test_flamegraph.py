import unittest
import xml.etree.ElementTree as ElementTree

from flamegraph import attribute, fold, render


class PerfAnalysisTests(unittest.TestCase):
    def test_fold_retains_root_to_leaf_order_and_counts(self):
        sample = "engine 123 [000] 1.234: cycles:\n\t123 leaf+0x12 (/engine)\n\t456 root (/engine)\n\n"
        stacks = fold(sample + sample)
        self.assertEqual(stacks, {"root;leaf": 2})
        ElementTree.fromstring(render(stacks, "synthetic & isolated"))

    def test_disjoint_attribution_preserves_sample_count(self):
        stacks = {"root;on_table_delta;dbsp::step": 4, "root;propagate_one": 3, "root;unknown": 2}
        result = attribute(stacks)
        self.assertEqual(sum(row["samples"] for row in result.values()), 9)
        self.assertEqual(result["row_and_membership_evaluation"]["samples"], 4)


if __name__ == "__main__":
    unittest.main()
