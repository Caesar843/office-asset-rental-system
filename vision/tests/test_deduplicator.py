from __future__ import annotations

import unittest

from app.config import DedupConfig
from models.decode_result import DecodeResult
from models.frame import FrameData
from parser.deduplicator import ScanResultDeduplicator
from parser.normalizer import ScanResultNormalizer


class DeduplicatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.deduplicator = ScanResultDeduplicator(DedupConfig())
        self.normalizer = ScanResultNormalizer()

    def _scan_result(self, *, frame_id: str, frame_time: int, asset_id: str):
        frame = FrameData(frame_id=frame_id, image=b"x", timestamp=frame_time, source_id="webcam-0")
        return self.normalizer.normalize(frame, DecodeResult(raw_text=asset_id, symbology="QR"), asset_id)

    def test_time_window_duplicate_is_blocked(self) -> None:
        first = self.deduplicator.apply(self._scan_result(frame_id="frame-a", frame_time=1700000000, asset_id="AS-8801"))
        second = self.deduplicator.apply(self._scan_result(frame_id="frame-b", frame_time=1700000001, asset_id="AS-8801"))

        self.assertFalse(first.is_duplicate)
        self.assertTrue(second.is_duplicate)
        self.assertEqual(second.duplicate_reason, "within_dedup_window")

    def test_result_recovers_after_dedup_window(self) -> None:
        self.deduplicator.apply(self._scan_result(frame_id="frame-a", frame_time=1700000000, asset_id="AS-8802"))
        third = self.deduplicator.apply(self._scan_result(frame_id="frame-c", frame_time=1700000003, asset_id="AS-8802"))

        self.assertFalse(third.is_duplicate)
        self.assertIsNone(third.duplicate_reason)

    def test_long_running_cache_is_pruned(self) -> None:
        self.deduplicator.apply(self._scan_result(frame_id="frame-old", frame_time=1700000000, asset_id="AS-OLD"))
        self.deduplicator.apply(self._scan_result(frame_id="frame-new", frame_time=1700000003, asset_id="AS-NEW"))

        sizes = self.deduplicator.cache_sizes()

        self.assertEqual(sizes["recent_by_key"], 1)
        self.assertEqual(sizes["seen_assets_by_frame"], 1)

    def test_same_frame_same_asset_and_different_asset_are_not_confused(self) -> None:
        first = self.deduplicator.apply(self._scan_result(frame_id="frame-shared", frame_time=1700000010, asset_id="AS-8803"))
        same_asset = self.deduplicator.apply(
            self._scan_result(frame_id="frame-shared", frame_time=1700000010, asset_id="AS-8803")
        )
        different_asset = self.deduplicator.apply(
            self._scan_result(frame_id="frame-shared", frame_time=1700000010, asset_id="AS-8804")
        )

        self.assertFalse(first.is_duplicate)
        self.assertTrue(same_asset.is_duplicate)
        self.assertEqual(same_asset.duplicate_reason, "same_frame_same_asset")
        self.assertFalse(different_asset.is_duplicate)


if __name__ == "__main__":
    unittest.main()
