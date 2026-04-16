from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

from models._validation import (
    ensure_non_empty_text,
    ensure_positive_number,
    ensure_positive_int,
    normalize_extra,
)

DECODE_CANDIDATES_EXTRA_KEY = "_decode_candidates"
PRIMARY_DECODE_CANDIDATE_EXTRA_KEY = "_primary_decode_candidate"
QUALITY_REPORT_EXTRA_KEY = "_quality_report"
QUALITY_RETRY_EXTRA_KEY = "_quality_retry_applied"
DECODE_CANDIDATE_SUMMARY_EXTRA_KEY = "_decode_candidate_summary"


@dataclass(frozen=True, slots=True)
class FrameData:
    frame_id: str
    image: Any
    timestamp: float
    source_id: str
    width: int | None = None
    height: int | None = None
    channel_count: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "frame_id", ensure_non_empty_text(self.frame_id, "frame_id"))
        if self.image is None:
            raise ValueError("image cannot be None")
        object.__setattr__(self, "timestamp", ensure_positive_number(self.timestamp, "timestamp"))
        object.__setattr__(self, "source_id", ensure_non_empty_text(self.source_id, "source_id"))

        if self.width is not None:
            object.__setattr__(self, "width", ensure_positive_int(self.width, "width"))
        if self.height is not None:
            object.__setattr__(self, "height", ensure_positive_int(self.height, "height"))
        if self.channel_count is not None:
            object.__setattr__(self, "channel_count", ensure_positive_int(self.channel_count, "channel_count"))

        object.__setattr__(self, "extra", normalize_extra(self.extra))


def strip_transient_frame_data(frame: FrameData) -> FrameData:
    compacted_extra = compact_frame_extra(frame.extra)
    if compacted_extra == frame.extra:
        return frame
    return replace(frame, extra=compacted_extra)


def compact_frame_extra(extra: dict[str, Any]) -> dict[str, Any]:
    compacted = dict(extra)
    candidates = compacted.pop(DECODE_CANDIDATES_EXTRA_KEY, None)
    if isinstance(candidates, list) and candidates:
        compacted[DECODE_CANDIDATE_SUMMARY_EXTRA_KEY] = tuple(
            _summarize_decode_candidate(candidate)
            for candidate in candidates
            if isinstance(candidate, dict)
        )
    return compacted


def _summarize_decode_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    image = candidate.get("image")
    image_shape: tuple[int, ...] | None = None
    if hasattr(image, "shape"):
        try:
            image_shape = tuple(int(value) for value in image.shape)
        except Exception:
            image_shape = None
    return {
        "name": candidate.get("name"),
        "origin": candidate.get("origin"),
        "variant": candidate.get("variant"),
        "bbox_offset": tuple(candidate.get("bbox_offset", (0, 0))),
        "preprocess_steps": tuple(candidate.get("preprocess_steps", ())),
        "image_shape": image_shape,
    }
