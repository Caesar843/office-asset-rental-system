from __future__ import annotations

import time
from dataclasses import dataclass, field


class HealthState:
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    RECONNECTING = "RECONNECTING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"

    ALL = frozenset({STARTING, RUNNING, DEGRADED, RECONNECTING, STOPPING, STOPPED})


@dataclass(frozen=True, slots=True)
class HealthTransition:
    from_state: str | None
    to_state: str
    reason: str
    timestamp: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if self.from_state is not None and self.from_state not in HealthState.ALL:
            raise ValueError(f"unsupported health state: {self.from_state}")
        if self.to_state not in HealthState.ALL:
            raise ValueError(f"unsupported health state: {self.to_state}")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("reason cannot be blank")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "timestamp": self.timestamp,
            "to_state": self.to_state,
            "reason": self.reason,
        }
        if self.from_state is not None:
            payload["from_state"] = self.from_state
        return payload
