import gzip
import json
import tempfile
import unittest
from pathlib import Path

from source_loader import SourceLoader


class SourceLoaderTests(unittest.TestCase):
    def test_original_globals_remain_lazy_in_classes_functions_and_comprehensions(self):
        bundle = {"modules": {"api.synthetic_test": {"imports": {"dataclass": ["dataclasses", "dataclass"]}, "definitions": {"CONSTANT": "CONSTANT = 7", "Example": "@dataclass\nclass Example:\n    value: int = CONSTANT\n    def total(self, increment: int) -> int:\n        return self.value + increment + CONSTANT", "calculate": "def calculate(values):\n    return [value + CONSTANT for value in values]"}, "path": "synthetic.py", "sha256": "synthetic"}}}
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "source.gz"
            path.write_bytes(gzip.compress(json.dumps(bundle).encode()))
            loader = SourceLoader(path)
            module = loader.module("api.synthetic_test")
            self.assertEqual(module.Example().total(1), 15)
            self.assertEqual(module.calculate([1, 2]), [8, 9])


if __name__ == "__main__":
    unittest.main()
