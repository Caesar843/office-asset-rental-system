from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from models._validation import ensure_non_empty_text, ensure_probability, normalize_bbox, normalize_extra


@dataclass(frozen=True, slots=True)
class DecodeResult:
    raw_text: str
    symbology: str
    bbox: tuple[int, int, int, int] | None = None
    confidence: float | None = None
    decoder_name: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "raw_text", ensure_non_empty_text(self.raw_text, "raw_text"))
        object.__setattr__(self, "symbology", ensure_non_empty_text(self.symbology, "symbology"))
        object.__setattr__(self, "bbox", normalize_bbox(self.bbox))
        object.__setattr__(self, "confidence", ensure_probability(self.confidence, "confidence"))
        if self.decoder_name is not None:
            object.__setattr__(self, "decoder_name", ensure_non_empty_text(self.decoder_name, "decoder_name"))
        object.__setattr__(self, "extra", normalize_extra(self.extra))
