from __future__ import annotations

from collections.abc import Sequence

from models.decode_result import DecodeResult
from models.error_result import VisionErrorResult
from models.frame import FrameData
from models.scan_result import ScanResult
from parser.base import ScanResultBuilder


class MockScanResultBuilder(ScanResultBuilder):
    """Round-1 parser stub.

    This builder only exists so the formal pipeline can run end-to-end before the
    real asset_id parser / normalizer / deduplicator are implemented.
    """

    def __init__(self, *, asset_id: str | None = None, is_duplicate: bool = False) -> None:
        self._asset_id = asset_id
        self._is_duplicate = is_duplicate

    def build(
        self,
        frame: FrameData,
        decode_results: Sequence[DecodeResult],
    ) -> ScanResult | VisionErrorResult:
        if not decode_results:
            return VisionErrorResult(
                stage="decoder",
                error_code="NO_CODE",
                message="decoder returned no results",
                frame_id=frame.frame_id,
                source_id=frame.source_id,
            )

        first = decode_results[0]
        asset_id = self._asset_id or first.raw_text.strip()
        if not asset_id:
            return VisionErrorResult(
                stage="parser",
                error_code="ASSET_ID_PARSE_FAILED",
                message="mock parser could not derive asset_id",
                frame_id=frame.frame_id,
                source_id=frame.source_id,
            )

        return ScanResult(
            asset_id=asset_id,
            raw_text=first.raw_text,
            symbology=first.symbology,
            source_id=frame.source_id,
            frame_time=int(frame.timestamp),
            frame_id=frame.frame_id,
            bbox=first.bbox,
            confidence=first.confidence,
            is_duplicate=self._is_duplicate,
            duplicate_reason="mock_duplicate" if self._is_duplicate else None,
        )
