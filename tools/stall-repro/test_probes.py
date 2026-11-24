import unittest

from probes import require_isolated, summarize


class ProbeContractTests(unittest.TestCase):
    def test_refuses_nonlocal_services(self):
        with self.assertRaises(ValueError):
            require_isolated("https://example.invalid", "postgres://localhost/stall_repro")
        with self.assertRaises(ValueError):
            require_isolated("http://127.0.0.1:7010", "postgres://localhost/postgres")

    def test_timeout_is_not_a_successful_latency(self):
        result = summarize([{"kind": "drain", "success": True, "duration_s": 0.1}, {"kind": "drain", "success": False, "duration_s": 2.0}])
        self.assertEqual(result["drain"]["attempts"], 2)
        self.assertEqual(result["drain"]["success_rate"], 0.5)
        self.assertEqual(result["drain"]["successful_max_s"], 0.1)
        self.assertIsNone(result["live"]["success_rate"])


if __name__ == "__main__":
    unittest.main()
