from __future__ import annotations

import json
import unittest
from urllib.error import URLError

from app.config import GatewayConfig
from gateway.api_client import APIClient, TransportResponse
from models.scan_result import ScanResult


class APIClientTests(unittest.TestCase):
    def build_client(self, transport):
        return APIClient(GatewayConfig(), transport=transport)

    def test_request_payload_mapping_and_forbidden_field_filtering(self) -> None:
        captured: dict[str, object] = {}

        def transport(url, payload, timeout_sec, headers):
            captured["url"] = url
            captured["payload"] = dict(payload)
            captured["timeout_sec"] = timeout_sec
            captured["headers"] = dict(headers)
            return TransportResponse(
                status_code=200,
                body=json.dumps(
                    {
                        "success": True,
                        "code": "OK",
                        "message": "accepted",
                        "asset_id": "AS-1001",
                        "extra": {"server": "mock"},
                    }
                ).encode("utf-8"),
            )

        client = self.build_client(transport)
        request = ScanResult(
            asset_id="AS-1001",
            raw_text="AS-1001",
            symbology="QR",
            source_id="webcam-0",
            frame_time=1700000000,
            frame_id="frame-1",
            confidence=0.98,
            bbox=(1, 2, 3, 4),
            extra={"request_seq": 101, "trace_id": "trace-1", "user_id": "U-1"},
        )

        result = client.submit(request)

        self.assertTrue(result.business_success)
        self.assertTrue(result.transport_success)
        self.assertTrue(result.response_valid)
        payload = captured["payload"]
        self.assertEqual(
            set(payload.keys()),
            {"asset_id", "raw_text", "symbology", "source_id", "frame_time", "frame_id", "confidence", "bbox", "extra"},
        )
        self.assertEqual(payload["asset_id"], "AS-1001")
        self.assertEqual(payload["bbox"], [1, 2, 3, 4])
        self.assertEqual(payload["extra"], {"trace_id": "trace-1"})
        self.assertNotIn("request_seq", payload)
        self.assertNotIn("user_id", payload)

    def test_formal_success_response_requires_asset_id_and_extra(self) -> None:
        client = self.build_client(
            lambda url, payload, timeout_sec, headers: TransportResponse(
                status_code=200,
                body=json.dumps(
                    {
                        "success": True,
                        "code": "SCAN_ACCEPTED",
                        "message": "scan accepted",
                        "asset_id": "AS-1100",
                        "extra": {"trace_id": "trace-1100"},
                    }
                ).encode("utf-8"),
            )
        )
        request = ScanResult(
            asset_id="AS-1100",
            raw_text="AS-1100",
            symbology="QR",
            source_id="webcam-0",
            frame_time=1700000010,
        )

        result = client.submit(request)

        self.assertEqual(result.status, "business_success")
        self.assertTrue(result.transport_success)
        self.assertTrue(result.http_ok)
        self.assertTrue(result.response_valid)
        self.assertTrue(result.business_success)
        self.assertEqual(result.response_payload["asset_id"], "AS-1100")
        self.assertEqual(result.response_payload["extra"], {"trace_id": "trace-1100"})

    def test_non_json_response_is_reported_as_invalid_response(self) -> None:
        client = self.build_client(
            lambda url, payload, timeout_sec, headers: TransportResponse(status_code=200, body=b"<html>oops</html>")
        )
        request = ScanResult(
            asset_id="AS-1002",
            raw_text="AS-1002",
            symbology="QR",
            source_id="webcam-0",
            frame_time=1700000001,
        )

        result = client.submit(request)

        self.assertFalse(result.business_success)
        self.assertEqual(result.status, "invalid_response")
        self.assertEqual(result.code, "INVALID_RESPONSE")
        self.assertTrue(result.transport_success)
        self.assertFalse(result.response_valid)

    def test_success_false_response_is_reported_as_business_error(self) -> None:
        client = self.build_client(
            lambda url, payload, timeout_sec, headers: TransportResponse(
                status_code=200,
                body=json.dumps(
                    {
                        "success": False,
                        "code": "BUSY",
                        "message": "scanner busy",
                        "asset_id": "AS-1003",
                        "extra": {"queue_depth": 1},
                    }
                ).encode("utf-8"),
            )
        )
        request = ScanResult(
            asset_id="AS-1003",
            raw_text="AS-1003",
            symbology="QR",
            source_id="webcam-0",
            frame_time=1700000002,
        )

        result = client.submit(request)

        self.assertFalse(result.business_success)
        self.assertEqual(result.status, "business_error")
        self.assertEqual(result.code, "BUSY")
        self.assertEqual(result.message, "scanner busy")
        self.assertTrue(result.transport_success)
        self.assertTrue(result.response_valid)

    def test_missing_contract_fields_are_reported_as_invalid_response(self) -> None:
        client = self.build_client(
            lambda url, payload, timeout_sec, headers: TransportResponse(
                status_code=200,
                body=json.dumps({"asset_id": "AS-1005", "exists": True}).encode("utf-8"),
            )
        )
        request = ScanResult(
            asset_id="AS-1005",
            raw_text="AS-1005",
            symbology="QR",
            source_id="webcam-0",
            frame_time=1700000005,
        )

        result = client.submit(request)

        self.assertEqual(result.status, "invalid_response")
        self.assertEqual(result.code, "INVALID_RESPONSE")
        self.assertTrue(result.transport_success)
        self.assertFalse(result.response_valid)

    def test_non_2xx_response_is_reported_as_http_error(self) -> None:
        client = self.build_client(
            lambda url, payload, timeout_sec, headers: TransportResponse(
                status_code=503,
                body=json.dumps(
                    {
                        "success": False,
                        "code": "UPSTREAM_UNAVAILABLE",
                        "message": "scan service unavailable",
                        "asset_id": "AS-1006",
                        "extra": {"retryable": True},
                    }
                ).encode("utf-8"),
            )
        )
        request = ScanResult(
            asset_id="AS-1006",
            raw_text="AS-1006",
            symbology="QR",
            source_id="webcam-0",
            frame_time=1700000006,
        )

        result = client.submit(request)

        self.assertEqual(result.status, "http_error")
        self.assertTrue(result.transport_success)
        self.assertFalse(result.http_ok)
        self.assertTrue(result.response_valid)
        self.assertFalse(result.business_success)
        self.assertEqual(result.code, "UPSTREAM_UNAVAILABLE")

    def test_network_timeout_is_reported(self) -> None:
        def transport(url, payload, timeout_sec, headers):
            raise TimeoutError("socket timed out")

        client = self.build_client(transport)
        request = ScanResult(
            asset_id="AS-1004",
            raw_text="AS-1004",
            symbology="QR",
            source_id="webcam-0",
            frame_time=1700000003,
        )

        result = client.submit(request)

        self.assertEqual(result.status, "network_error")
        self.assertEqual(result.code, "NETWORK_TIMEOUT")
        self.assertFalse(result.transport_success)
        self.assertFalse(result.http_ok)
        self.assertFalse(result.response_valid)

    def test_connection_error_is_reported(self) -> None:
        def transport(url, payload, timeout_sec, headers):
            raise URLError("connection refused")

        client = self.build_client(transport)
        request = ScanResult(
            asset_id="AS-1007",
            raw_text="AS-1007",
            symbology="QR",
            source_id="webcam-0",
            frame_time=1700000007,
        )

        result = client.submit(request)

        self.assertEqual(result.status, "network_error")
        self.assertEqual(result.code, "NETWORK_ERROR")
        self.assertFalse(result.transport_success)

    def test_invalid_request_object_is_rejected_before_submit(self) -> None:
        client = self.build_client(
            lambda url, payload, timeout_sec, headers: TransportResponse(
                status_code=200,
                body=b"{}",
            )
        )

        with self.assertRaises(TypeError):
            client.submit(object())


if __name__ == "__main__":
    unittest.main()
