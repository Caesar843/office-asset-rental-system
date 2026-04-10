from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from models.decode_result import DecodeResult
from models.error_result import VisionErrorResult
from models.frame import FrameData
from models.scan_result import ScanResult


class AssetIdExtractor(Protocol):
    def parse(self, raw_text: str) -> str | None:
        """Extract one formal asset_id from raw decoder text."""


class Deduplicator(Protocol):
    def apply(self, scan_result: ScanResult) -> ScanResult:
        """Mark a ScanResult as duplicate when needed."""


class ScanResultBuilder(Protocol):
    def build(
        self,
        frame: FrameData,
        decode_results: Sequence[DecodeResult],
    ) -> ScanResult | VisionErrorResult:
        """Convert decode results into one formal ScanResult."""
