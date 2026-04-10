from __future__ import annotations

import unittest

from models.decode_result import DecodeResult
from models.frame import FrameData
from models.scan_result import ScanResult, ScanSubmitRequest
from models.submit_request import ScanSubmitRequest as CompatScanSubmitRequest


class ModelValidationTests(unittest.TestCase):
    def test_frame_data_rejects_blank_frame_id(self) -> None:
        with self.assertRaises(ValueError):
            FrameData(frame_id=" ", image=b"x", timestamp=1.0, source_id="cam-1")

    def test_decode_result_rejects_empty_raw_text(self) -> None:
        with self.assertRaises(ValueError):
            DecodeResult(raw_text=" ", symbology="QR")

    def test_scan_result_duplicate_cannot_build_submit_request(self) -> None:
        scan_result = ScanResult(
            asset_id="AS-1001",
            raw_text="AS-1001",
            symbology="QR",
            source_id="cam-1",
            frame_time=1700000000,
            is_duplicate=True,
            duplicate_reason="within_dedup_window",
        )
        with self.assertRaises(ValueError):
            scan_result.to_submit_request()

    def test_scan_result_filters_forbidden_extra_fields(self) -> None:
        scan_result = ScanResult(
            asset_id="AS-1001",
            raw_text="AS-1001",
            symbology="QR",
            source_id="cam-1",
            frame_time=1700000000,
            extra={"request_seq": 1, "trace_id": "trace-1"},
        )
        self.assertEqual(scan_result.extra, {"trace_id": "trace-1"})

    def test_frame_time_must_be_unix_integer_seconds(self) -> None:
        with self.assertRaises(TypeError):
            ScanResult(
                asset_id="AS-1002",
                raw_text="AS-1002",
                symbology="QR",
                source_id="cam-1",
                frame_time=1700000000.5,
            )
        with self.assertRaises(ValueError):
            ScanSubmitRequest(
                asset_id="AS-1002",
                raw_text="AS-1002",
                symbology="QR",
                source_id="cam-1",
                frame_time=1700000000123,
            )

    def test_extra_filters_forbidden_nested_fields(self) -> None:
        request = ScanSubmitRequest(
            asset_id="AS-1003",
            raw_text="AS-1003",
            symbology="QR",
            source_id="cam-1",
            frame_time=1700000003,
            extra={
                "trace": {"request_id": "req-1", "safe": True},
                "items": [{"hw_seq": 1}, {"ok": 2}],
            },
        )
        self.assertEqual(
            request.extra,
            {"trace": {"safe": True}, "items": [{}, {"ok": 2}]},
        )

    def test_scan_submit_request_formal_source_is_scan_result_module(self) -> None:
        self.assertIs(CompatScanSubmitRequest, ScanSubmitRequest)


if __name__ == "__main__":
    unittest.main()
