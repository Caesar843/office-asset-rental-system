from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from models._validation import ensure_non_empty_text, normalize_extra


@dataclass(frozen=True, slots=True)
class VisionErrorResult:
    stage: str
    error_code: str
    message: str
    frame_id: str | None = None
    source_id: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "stage", ensure_non_empty_text(self.stage, "stage"))
        object.__setattr__(self, "error_code", ensure_non_empty_text(self.error_code, "error_code"))
        object.__setattr__(self, "message", ensure_non_empty_text(self.message, "message"))
        if self.frame_id is not None:
            object.__setattr__(self, "frame_id", ensure_non_empty_text(self.frame_id, "frame_id"))
        if self.source_id is not None:
            object.__setattr__(self, "source_id", ensure_non_empty_text(self.source_id, "source_id"))
        object.__setattr__(self, "extra", normalize_extra(self.extra))
