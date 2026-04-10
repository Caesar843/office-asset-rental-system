from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from models._validation import (
    ensure_non_empty_text,
    ensure_probability,
    ensure_unix_timestamp_seconds,
    normalize_bbox,
    normalize_extra,
)


@dataclass(frozen=True, slots=True)
class ScanSubmitRequest:
    """Formal single source of truth for the submit contract object.

    Per the frozen vision module mapping, ScanSubmitRequest is formally defined in
    models/scan_result.py together with ScanResult.
    """

    asset_id: str
    raw_text: str
    symbology: str
    source_id: str
    frame_time: int
    frame_id: str | None = None
    confidence: float | None = None
    bbox: tuple[int, int, int, int] | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "asset_id", ensure_non_empty_text(self.asset_id, "asset_id"))
        object.__setattr__(self, "raw_text", ensure_non_empty_text(self.raw_text, "raw_text"))
        object.__setattr__(self, "symbology", ensure_non_empty_text(self.symbology, "symbology"))
        object.__setattr__(self, "source_id", ensure_non_empty_text(self.source_id, "source_id"))
        object.__setattr__(self, "frame_time", ensure_unix_timestamp_seconds(self.frame_time, "frame_time"))
        if self.frame_id is not None:
            object.__setattr__(self, "frame_id", ensure_non_empty_text(self.frame_id, "frame_id"))
        object.__setattr__(self, "confidence", ensure_probability(self.confidence, "confidence"))
        object.__setattr__(self, "bbox", normalize_bbox(self.bbox))
        object.__setattr__(self, "extra", normalize_extra(self.extra))

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "asset_id": self.asset_id,
            "raw_text": self.raw_text,
            "symbology": self.symbology,
            "source_id": self.source_id,
            "frame_time": self.frame_time,
        }
        if self.frame_id is not None:
            payload["frame_id"] = self.frame_id
        if self.confidence is not None:
            payload["confidence"] = self.confidence
        if self.bbox is not None:
            payload["bbox"] = list(self.bbox)
        if self.extra:
            payload["extra"] = dict(self.extra)
        return payload


@dataclass(frozen=True, slots=True)
class ScanResult:
    asset_id: str
    raw_text: str
    symbology: str
    source_id: str
    frame_time: int
    frame_id: str | None = None
    bbox: tuple[int, int, int, int] | None = None
    confidence: float | None = None
    is_duplicate: bool = False
    duplicate_reason: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "asset_id", ensure_non_empty_text(self.asset_id, "asset_id"))
        object.__setattr__(self, "raw_text", ensure_non_empty_text(self.raw_text, "raw_text"))
        object.__setattr__(self, "symbology", ensure_non_empty_text(self.symbology, "symbology"))
        object.__setattr__(self, "source_id", ensure_non_empty_text(self.source_id, "source_id"))
        object.__setattr__(self, "frame_time", ensure_unix_timestamp_seconds(self.frame_time, "frame_time"))
        if self.frame_id is not None:
            object.__setattr__(self, "frame_id", ensure_non_empty_text(self.frame_id, "frame_id"))
        object.__setattr__(self, "bbox", normalize_bbox(self.bbox))
        object.__setattr__(self, "confidence", ensure_probability(self.confidence, "confidence"))
        if self.duplicate_reason is not None:
            object.__setattr__(
                self,
                "duplicate_reason",
                ensure_non_empty_text(self.duplicate_reason, "duplicate_reason"),
            )
        object.__setattr__(self, "extra", normalize_extra(self.extra))

    def to_submit_request(self) -> ScanSubmitRequest:
        if self.is_duplicate:
            raise ValueError("duplicate scan results must not enter the HTTP submit chain")
        return ScanSubmitRequest(
            asset_id=self.asset_id,
            raw_text=self.raw_text,
            symbology=self.symbology,
            source_id=self.source_id,
            frame_time=self.frame_time,
            frame_id=self.frame_id,
            confidence=self.confidence,
            bbox=self.bbox,
            extra=self.extra,
        )

    def to_submit_payload(self) -> dict[str, Any]:
        return self.to_submit_request().to_payload()
