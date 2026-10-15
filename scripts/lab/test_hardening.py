"""Focused regressions for the lab's local resource and HTTP boundaries."""
import http.server as http_server
import threading
import unittest

from hardening import disk_violation, http, MAX_CREATED, MIN_FREE


class Limits(unittest.TestCase):
    def test_stops_before_either_hard_disk_limit(self):
        self.assertFalse(disk_violation(MAX_CREATED-300_000_001, MIN_FREE+300_000_000))
        self.assertTrue(disk_violation(MAX_CREATED-300_000_000, MIN_FREE+300_000_000))
        self.assertTrue(disk_violation(0, MIN_FREE+299_999_999))
        self.assertTrue(disk_violation(MAX_CREATED, MIN_FREE*2))
        self.assertTrue(disk_violation(0, MIN_FREE-1))

    def test_remote_targets_are_rejected_before_opening_a_connection(self):
        for url in ['http://example.invalid/', 'http://localhost/', 'https://127.0.0.1/',
                    'http://0.0.0.0/', 'file:///tmp/a']:
            with self.subTest(url=url), self.assertRaisesRegex(ValueError, 'literal loopback'):
                http(url)

    def test_plain_text_readiness_and_case_insensitive_headers(self):
        # The first smoke failed because DS returns text on its root path. Do
        # not turn that successful transport into a JSON parsing failure.
        class Handler(http_server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header('Stream-Next-Offset', 'next')
                self.end_headers()
                self.wfile.write(b'durable streams')

            def log_message(self, *args):
                pass

        with http_server.HTTPServer(('127.0.0.1', 0), Handler) as server:
            thread = threading.Thread(target=server.handle_request)
            thread.start()
            try:
                status, headers, body = http(f'http://127.0.0.1:{server.server_port}/')
                self.assertEqual((status, body), (200, 'durable streams'))
                self.assertEqual(headers['stream-next-offset'], 'next')
            finally:
                thread.join(timeout=3)
                self.assertFalse(thread.is_alive())


if __name__ == '__main__':
    unittest.main()
