from __future__ import annotations

from dataclasses import replace
from collections.abc import Sequence
from typing import Any

from app.config import DecodeConfig
from decoder.barcode_decoder import BarcodeDecoder
from decoder.base import Decoder, DecoderConfigurationError
from decoder.qr_decoder import QRCodeDecoder
from models.decode_result import DecodeResult
from models.frame import DECODE_CANDIDATES_EXTRA_KEY, PRIMARY_DECODE_CANDIDATE_EXTRA_KEY, FrameData


class HybridDecoder(Decoder):
    def __init__(
        self,
        config: DecodeConfig,
        *,
        qr_decoder: Decoder | None = None,
        barcode_decoder: Decoder | None = None,
    ) -> None:
        self._config = config
        self._qr_decoder = qr_decoder or QRCodeDecoder(config)
        self._barcode_decoder = barcode_decoder or BarcodeDecoder(config)

    def decode(self, frame: FrameData) -> list[DecodeResult]:
        if not self._config.enable_qr and not self._config.enable_barcode:
            raise DecoderConfigurationError("hybrid decoder requires at least one enabled decode path")

        ordered_decoders = self._ordered_decoders()
        stage_frames = self._build_stage_frames(frame)
        for stage_rank, stage_frame in enumerate(stage_frames):
            merged: list[DecodeResult] = []
            seen: set[tuple[str, str, tuple[int, int, int, int] | None]] = set()
            stage_meta = dict(stage_frame.extra)
            for decoder in ordered_decoders:
                for result in decoder.decode(stage_frame):
                    normalized = self._decorate_result(result, stage_meta=stage_meta, stage_rank=stage_rank)
                    fingerprint = (normalized.raw_text, normalized.symbology, normalized.bbox)
                    if fingerprint in seen:
                        continue
                    merged.append(normalized)
                    seen.add(fingerprint)
                if merged and not self._config.allow_multi_decode:
                    return merged[:1]
            if merged:
                return merged if self._config.allow_multi_decode else merged[:1]
        return []

    def _ordered_decoders(self) -> list[Decoder]:
        ordered_decoders: list[Decoder] = []
        if self._config.prefer_qr_first:
            if self._config.enable_qr:
                ordered_decoders.append(self._qr_decoder)
            if self._config.enable_barcode:
                ordered_decoders.append(self._barcode_decoder)
            return ordered_decoders
        if self._config.enable_barcode:
            ordered_decoders.append(self._barcode_decoder)
        if self._config.enable_qr:
            ordered_decoders.append(self._qr_decoder)
        return ordered_decoders

    def _build_stage_frames(self, frame: FrameData) -> list[FrameData]:
        candidates = frame.extra.get(DECODE_CANDIDATES_EXTRA_KEY)
        if not isinstance(candidates, list) or not candidates:
            return [frame]

        primary_name = frame.extra.get(PRIMARY_DECODE_CANDIDATE_EXTRA_KEY)
        ordered_candidates = [dict(candidate) for candidate in candidates]
        if isinstance(primary_name, str):
            ordered_candidates.sort(key=lambda item: 0 if item.get("name") == primary_name else 1)

        stage_frames: list[FrameData] = []
        for candidate in ordered_candidates:
            image = candidate["image"]
            height, width = image.shape[:2]
            channel_count = 1 if image.ndim == 2 else int(image.shape[2])
            stage_frames.append(
                replace(
                    frame,
                    image=image,
                    width=int(width),
                    height=int(height),
                    channel_count=channel_count,
                    extra={
                        "decode_stage": str(candidate.get("name", "full_original")),
                        "decode_stage_origin": str(candidate.get("origin", "full")),
                        "decode_stage_variant": str(candidate.get("variant", "original")),
                        "decode_bbox_offset": tuple(candidate.get("bbox_offset", (0, 0))),
                        "decode_preprocess_steps": tuple(candidate.get("preprocess_steps", ())),
                    },
                )
            )
        return stage_frames

    def _decorate_result(
        self,
        result: DecodeResult,
        *,
        stage_meta: dict[str, Any],
        stage_rank: int,
    ) -> DecodeResult:
        extra = dict(result.extra)
        extra.update(
            {
                "decode_stage": stage_meta.get("decode_stage"),
                "decode_stage_rank": stage_rank,
                "decode_stage_origin": stage_meta.get("decode_stage_origin"),
                "decode_stage_variant": stage_meta.get("decode_stage_variant"),
                "decode_preprocess_steps": stage_meta.get("decode_preprocess_steps"),
                "decode_used_fallback": stage_rank > 0,
            }
        )
        bbox_offset = stage_meta.get("decode_bbox_offset", (0, 0))
        return replace(
            result,
            bbox=self._translate_bbox(result.bbox, bbox_offset=bbox_offset),
            extra=extra,
        )

    def _translate_bbox(
        self,
        bbox: tuple[int, int, int, int] | None,
        *,
        bbox_offset: Any,
    ) -> tuple[int, int, int, int] | None:
        if bbox is None:
            return None
        if not isinstance(bbox_offset, Sequence) or len(bbox_offset) != 2:
            return bbox
        offset_x = int(bbox_offset[0])
        offset_y = int(bbox_offset[1])
        return (
            bbox[0] + offset_x,
            bbox[1] + offset_y,
            bbox[2],
            bbox[3],
        )
