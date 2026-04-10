from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from models._validation import (
    ensure_non_empty_text,
    ensure_positive_number,
    ensure_positive_int,
    normalize_extra,
)


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
