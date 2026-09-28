#!/usr/bin/env python3
"""Unit tests for spark_noabort.post_json's deadline logic, against a local mock HTTP server.

Covers: a reply that lands before the deadline, a reply that lands after the deadline (the
call must return {"abandoned": True} promptly, well before the server's own delay elapses, and
the late reply must still get ledgered once it lands), a non-2xx status, and a refused
connection.
"""
import http.server
import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "python"))
import spark_noabort as sn  # noqa: E402


class _Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):  # silence
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length) if length else b""
        qs = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
        delay = float(qs.get("delay", ["0"])[0])
        status = int(qs.get("status", ["200"])[0])
        if delay:
            time.sleep(delay)
        if status >= 400:
            body = json.dumps({"error": "boom"}).encode()
        else:
            body = json.dumps({"result": "ok", "usage": {"prompt_tokens": 7, "completion_tokens": 3}}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class SparkNoAbortTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def url(self, suffix=""):
        return f"http://127.0.0.1:{self.port}/{suffix}"

    def test_reply_within_deadline(self):
        res = sn.post_json(self.url(), {"q": 1}, deadline_s=3.0)
        self.assertTrue(res.get("ok"))
        self.assertEqual(res["json"]["result"], "ok")

    def test_deadline_abandons_without_waiting_for_the_server(self):
        # Server takes 1.2s; we only allow 0.2s. The call must return in well under 1.2s.
        t0 = time.time()
        res = sn.post_json(self.url("?delay=1.2"), {}, deadline_s=0.2)
        elapsed = time.time() - t0
        self.assertTrue(res.get("abandoned"))
        self.assertLess(elapsed, 0.9, "post_json waited for the slow server instead of abandoning")

    def test_late_reply_is_ledgered_after_abandonment(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = os.path.join(tmp, "usage.jsonl")
            res = sn.post_json(
                self.url("?delay=1.0"),
                {},
                deadline_s=0.2,
                late={"path": ledger, "row": {"tag": "unit-test"}, "tokenKeys": ["p", "c"]},
            )
            self.assertTrue(res.get("abandoned"))
            # Give the detached carrier time to receive the (delayed) reply and write it.
            deadline = time.time() + 3.0
            while time.time() < deadline and not os.path.exists(ledger):
                time.sleep(0.05)
            self.assertTrue(os.path.exists(ledger), "late reply was never ledgered")
            with open(ledger, encoding="utf-8") as fh:
                rows = [json.loads(l) for l in fh if l.strip()]
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["tag"], "unit-test")
            self.assertTrue(row["late"])
            self.assertTrue(row["measured"])
            self.assertEqual(row["p"], 7)
            self.assertEqual(row["c"], 3)

    def test_http_error_status(self):
        res = sn.post_json(self.url("?status=500"), {}, deadline_s=3.0)
        self.assertEqual(res.get("error"), "http_500")

    def test_connection_refused(self):
        # Nothing listens on this port; the connect should fail fast, well within the deadline.
        res = sn.post_json("http://127.0.0.1:1/", {}, deadline_s=5.0)
        self.assertIn("error", res)


if __name__ == "__main__":
    unittest.main()
