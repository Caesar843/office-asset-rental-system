from __future__ import annotations

import unittest

from app.config import DedupConfig
from models.decode_result import DecodeResult
from models.error_result import VisionErrorResult
from models.frame import FrameData
from parser.asset_id_parser import AssetIdParser
from parser.deduplicator import ScanResultDeduplicator
from parser.normalizer import FormalScanResultBuilder, ScanResultNormalizer
from parser.stub import MockScanResultBuilder


class ParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.frame = FrameData(frame_id="frame-1", image=b"x", timestamp=1700000000, source_id="webcam-0")

    def test_mock_builder_creates_scan_result(self) -> None:
        builder = MockScanResultBuilder(asset_id="AS-3001")

        result = builder.build(self.frame, [DecodeResult(raw_text="AS-3001", symbology="QR", confidence=0.95)])

        self.assertEqual(result.asset_id, "AS-3001")
        self.assertEqual(result.source_id, "webcam-0")
        self.assertEqual(result.frame_time, 1700000000)

    def test_mock_builder_returns_error_when_no_decode_results(self) -> None:
        builder = MockScanResultBuilder()

        result = builder.build(self.frame, [])

        self.assertIsInstance(result, VisionErrorResult)
        self.assertEqual(result.error_code, "NO_CODE")

    def test_asset_id_parser_extracts_direct_asset_id(self) -> None:
        parser = AssetIdParser()
        self.assertEqual(parser.parse("AS-3001"), "AS-3001")

    def test_asset_id_parser_cleans_whitespace_newlines_and_prefix(self) -> None:
        parser = AssetIdParser()
        self.assertEqual(parser.parse("  asset_id:  as-3002 \n"), "AS-3002")

    def test_asset_id_parser_rejects_illegal_raw_text(self) -> None:
        parser = AssetIdParser()
        self.assertIsNone(parser.parse("borrow asset 3003 please"))

    def test_normalizer_preserves_formal_fields(self) -> None:
        normalizer = ScanResultNormalizer()

        result = normalizer.normalize(
            self.frame,
            DecodeResult(raw_text="AS-3004", symbology="QR", confidence=0.95, bbox=(1, 2, 30, 40)),
            "AS-3004",
        )

        self.assertEqual(result.asset_id, "AS-3004")
        self.assertEqual(result.raw_text, "AS-3004")
        self.assertEqual(result.symbology, "QR")
        self.assertEqual(result.source_id, "webcam-0")
        self.assertEqual(result.frame_time, 1700000000)
        self.assertEqual(result.frame_id, "frame-1")
        self.assertEqual(result.bbox, (1, 2, 30, 40))

    def test_deduplicator_marks_same_frame_same_asset(self) -> None:
        deduplicator = ScanResultDeduplicator(DedupConfig())
        normalizer = ScanResultNormalizer()
        decode_result = DecodeResult(raw_text="AS-3005", symbology="QR")
        first = deduplicator.apply(normalizer.normalize(self.frame, decode_result, "AS-3005"))
        second = deduplicator.apply(normalizer.normalize(self.frame, decode_result, "AS-3005"))

        self.assertFalse(first.is_duplicate)
        self.assertTrue(second.is_duplicate)
        self.assertEqual(second.duplicate_reason, "same_frame_same_asset")

    def test_deduplicator_marks_within_time_window_duplicate(self) -> None:
        deduplicator = ScanResultDeduplicator(DedupConfig())
        normalizer = ScanResultNormalizer()
        first_frame = FrameData(frame_id="frame-a", image=b"x", timestamp=1700000000, source_id="webcam-0")
        second_frame = FrameData(frame_id="frame-b", image=b"x", timestamp=1700000001, source_id="webcam-0")
        decode_result = DecodeResult(raw_text="AS-3006", symbology="QR")
        first = deduplicator.apply(normalizer.normalize(first_frame, decode_result, "AS-3006"))
        second = deduplicator.apply(normalizer.normalize(second_frame, decode_result, "AS-3006"))

        self.assertFalse(first.is_duplicate)
        self.assertTrue(second.is_duplicate)
        self.assertEqual(second.duplicate_reason, "within_dedup_window")
        with self.assertRaises(ValueError):
            second.to_submit_request()

    def test_formal_scan_result_builder_reports_parse_failure(self) -> None:
        builder = FormalScanResultBuilder(
            asset_id_parser=AssetIdParser(),
            normalizer=ScanResultNormalizer(),
            deduplicator=ScanResultDeduplicator(DedupConfig()),
        )

        result = builder.build(self.frame, [DecodeResult(raw_text="invalid payload", symbology="QR")])

        self.assertIsInstance(result, VisionErrorResult)
        self.assertEqual(result.error_code, "ASSET_ID_PARSE_FAILED")

    def test_formal_scan_result_builder_reports_multi_asset_conflict(self) -> None:
        builder = FormalScanResultBuilder(
            asset_id_parser=AssetIdParser(),
            normalizer=ScanResultNormalizer(),
            deduplicator=ScanResultDeduplicator(DedupConfig()),
        )

        result = builder.build(
            self.frame,
            [
                DecodeResult(raw_text="AS-3007", symbology="QR", confidence=0.9),
                DecodeResult(raw_text="AS-3008", symbology="CODE128", confidence=0.8),
            ],
        )

        self.assertIsInstance(result, VisionErrorResult)
        self.assertEqual(result.error_code, "MULTI_RESULT_CONFLICT")

    def test_formal_scan_result_builder_accepts_multiple_raw_texts_normalized_to_same_asset(self) -> None:
        builder = FormalScanResultBuilder(
            asset_id_parser=AssetIdParser(),
            normalizer=ScanResultNormalizer(),
            deduplicator=ScanResultDeduplicator(DedupConfig()),
        )

        result = builder.build(
            self.frame,
            [
                DecodeResult(raw_text="asset_id: AS-3009", symbology="QR", confidence=0.7),
                DecodeResult(raw_text="AS-3009\n", symbology="CODE128", confidence=0.8),
            ],
        )

        self.assertEqual(result.asset_id, "AS-3009")
        self.assertFalse(result.is_duplicate)


if __name__ == "__main__":
    unittest.main()
