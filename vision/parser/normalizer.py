from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from models.decode_result import DecodeResult
from models.error_result import VisionErrorResult
from models.frame import FrameData
from models.scan_result import ScanResult
from parser.asset_id_parser import AssetIdParser
from parser.base import ScanResultBuilder
from parser.deduplicator import ScanResultDeduplicator


class ScanResultNormalizer:
    def normalize(self, frame: FrameData, decode_result: DecodeResult, asset_id: str) -> ScanResult:
        extra = {
            "decoder_name": decode_result.decoder_name,
            **decode_result.extra,
        }
        extra = {key: value for key, value in extra.items() if value is not None}
        return ScanResult(
            asset_id=asset_id,
            raw_text=decode_result.raw_text,
            symbology=decode_result.symbology,
            source_id=frame.source_id,
            frame_time=int(frame.timestamp),
            frame_id=frame.frame_id,
            bbox=decode_result.bbox,
            confidence=decode_result.confidence,
            extra=extra,
        )


class FormalScanResultBuilder(ScanResultBuilder):
    def __init__(
        self,
        *,
        asset_id_parser: AssetIdParser,
        normalizer: ScanResultNormalizer,
        deduplicator: ScanResultDeduplicator,
    ) -> None:
        self._asset_id_parser = asset_id_parser
        self._normalizer = normalizer
        self._deduplicator = deduplicator

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

        normalized_candidates: list[tuple[str, DecodeResult, ScanResult]] = []
        for decode_result in decode_results:
            asset_id = self._asset_id_parser.parse(decode_result.raw_text)
            if asset_id is None:
                continue
            normalized = self._normalizer.normalize(frame, decode_result, asset_id)
            normalized_candidates.append((asset_id, decode_result, normalized))

        if not normalized_candidates:
            return VisionErrorResult(
                stage="parser",
                error_code="ASSET_ID_PARSE_FAILED",
                message="could not extract a formal asset_id from decoder output",
                frame_id=frame.frame_id,
                source_id=frame.source_id,
                extra={"raw_texts": [item.raw_text for item in decode_results]},
            )

        asset_ids = {asset_id for asset_id, _, _ in normalized_candidates}
        if len(asset_ids) > 1:
            return VisionErrorResult(
                stage="parser",
                error_code="MULTI_RESULT_CONFLICT",
                message="multiple different asset_id values were decoded from the same frame",
                frame_id=frame.frame_id,
                source_id=frame.source_id,
                extra={"asset_ids": sorted(asset_ids)},
            )

        chosen = max(
            normalized_candidates,
            key=lambda item: ((item[1].confidence or 0.0), (item[1].bbox[2] * item[1].bbox[3]) if item[1].bbox else 0.0),
        )[2]
        return self._deduplicator.apply(chosen)
