from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

FORBIDDEN_REQUEST_FIELDS = frozenset(
    {
        "request_seq",
        "request_id",
        "hw_seq",
        "hw_result",
        "transaction_state",
        "device_status",
        "user_id",
        "action_type",
    }
)

FORMAL_SUBMIT_FIELDS = frozenset(
    {
        "asset_id",
        "raw_text",
        "symbology",
        "source_id",
        "frame_time",
        "frame_id",
        "confidence",
        "bbox",
        "extra",
    }
)


def ensure_non_empty_text(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} cannot be blank")
    return stripped


def ensure_positive_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return value


def ensure_unix_timestamp_seconds(value: int, field_name: str = "frame_time") -> int:
    timestamp = ensure_positive_int(value, field_name)
    if timestamp >= 10_000_000_000:
        raise ValueError(f"{field_name} must be a Unix timestamp integer in seconds, not milliseconds")
    return timestamp


def ensure_non_negative_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must be >= 0")
    return value


def ensure_positive_number(value: int | float, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a number")
    if value <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return float(value)


def ensure_probability(value: float | None, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a float between 0 and 1")
    numeric = float(value)
    if numeric < 0.0 or numeric > 1.0:
        raise ValueError(f"{field_name} must be between 0 and 1")
    return numeric


def normalize_bbox(
    value: Sequence[int | float] | None,
    field_name: str = "bbox",
) -> tuple[int, int, int, int] | None:
    if value is None:
        return None
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"{field_name} must be a sequence of four numbers")
    if len(value) != 4:
        raise ValueError(f"{field_name} must contain exactly four items")
    x, y, width, height = value
    normalized = (
        int(ensure_non_negative_int(int(x), f"{field_name}[0]")),
        int(ensure_non_negative_int(int(y), f"{field_name}[1]")),
        int(ensure_positive_int(int(width), f"{field_name}[2]")),
        int(ensure_positive_int(int(height), f"{field_name}[3]")),
    )
    return normalized


def normalize_extra(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError("extra must be a mapping")
    cleaned: dict[str, Any] = {}
    for key, item in value.items():
        key_name = ensure_non_empty_text(str(key), "extra key")
        if key_name in FORBIDDEN_REQUEST_FIELDS or key_name in FORMAL_SUBMIT_FIELDS:
            continue
        cleaned[key_name] = _sanitize_extra_value(item)
    return cleaned


def _sanitize_extra_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            key_name = ensure_non_empty_text(str(key), "extra key")
            if key_name in FORBIDDEN_REQUEST_FIELDS or key_name in FORMAL_SUBMIT_FIELDS:
                continue
            cleaned[key_name] = _sanitize_extra_value(item)
        return cleaned
    if isinstance(value, list):
        return [_sanitize_extra_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_extra_value(item) for item in value)
    return value
