from __future__ import annotations

import json
import threading
import unittest
from urllib.request import Request, urlopen

from gateway.mock_server import build_mock_server


class MockServerTests(unittest.TestCase):
    def test_mock_server_returns_formal_scan_contract(self) -> None:
        server = build_mock_server("127.0.0.1", 0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            body = json.dumps(
                {
                    "asset_id": "AS-7001",
                    "raw_text": "AS-7001",
                    "symbology": "QR",
                    "source_id": "demo-image",
                    "frame_time": 1700000000,
                }
            ).encode("utf-8")
            request = Request(
                url=f"http://127.0.0.1:{server.server_address[1]}/scan/result",
                data=body,
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urlopen(request, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertTrue(payload["success"])
        self.assertEqual(payload["code"], "SCAN_ACCEPTED")
        self.assertEqual(payload["asset_id"], "AS-7001")
        self.assertIn("extra", payload)

    def test_mock_server_health_endpoint_is_available(self) -> None:
        server = build_mock_server("127.0.0.1", 0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urlopen(f"http://127.0.0.1:{server.server_address[1]}/health", timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertTrue(payload["success"])
        self.assertEqual(payload["code"], "MOCK_SERVER_HEALTHY")
        self.assertEqual(payload["asset_id"], "MOCK_SERVER")


if __name__ == "__main__":
    unittest.main()
