from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Callable

from app.config import CaptureConfig


@dataclass(frozen=True, slots=True)
class ReconnectBackoffPolicy:
    base_delay_sec: float
    mode: str
    max_delay_sec: float
    jitter_enabled: bool
    jitter_ratio: float

    @classmethod
    def from_config(cls, config: CaptureConfig) -> ReconnectBackoffPolicy:
        return cls(
            base_delay_sec=float(config.reconnect_backoff_sec),
            mode=config.reconnect_backoff_mode,
            max_delay_sec=float(config.reconnect_backoff_max_sec),
            jitter_enabled=config.reconnect_jitter_enabled,
            jitter_ratio=float(config.reconnect_jitter_ratio),
        )

    def delay_for_attempt(self, attempt: int, *, random_fn: Callable[[], float] | None = None) -> float:
        if attempt < 1:
            raise ValueError("attempt must be >= 1")

        delay = self.base_delay_sec
        if self.mode == "exponential":
            delay *= 2 ** (attempt - 1)
        delay = min(delay, self.max_delay_sec)

        if not self.jitter_enabled or self.jitter_ratio == 0.0:
            return delay

        jitter_source = random_fn or random.random
        jitter_span = delay * self.jitter_ratio
        normalized = max(0.0, min(1.0, float(jitter_source())))
        jitter_offset = (normalized * 2.0 - 1.0) * jitter_span
        return max(0.0, delay + jitter_offset)
