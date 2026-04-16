from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any
from urllib.parse import urlparse


def _require_bool(value: bool, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{field_name} must be a bool")
    return value


def _require_int(value: int, field_name: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{field_name} must be >= {minimum}")
    return value


def _require_optional_int(value: int | None, field_name: str, *, minimum: int | None = None) -> int | None:
    if value is None:
        return None
    return _require_int(value, field_name, minimum=minimum)


def _require_probability(value: float, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a number")
    numeric = float(value)
    if numeric < 0.0 or numeric > 1.0:
        raise ValueError(f"{field_name} must be between 0 and 1")
    return numeric


def _require_positive_number(value: int | float, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a number")
    numeric = float(value)
    if numeric <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return numeric


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"{field_name} cannot be blank")
    return stripped


def _require_optional_text(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name)


def _normalize_section_overrides(
    section_name: str,
    values: dict[str, Any] | None,
    *,
    aliases: dict[str, str] | None = None,
) -> dict[str, Any]:
    if not values:
        return {}
    normalized = dict(values)
    alias_map = aliases or {}
    for alias_name, canonical_name in alias_map.items():
        if alias_name not in normalized:
            continue
        alias_value = normalized.pop(alias_name)
        if canonical_name in normalized and normalized[canonical_name] != alias_value:
            raise ValueError(
                f"{section_name}.{alias_name} conflicts with {section_name}.{canonical_name}"
            )
        normalized[canonical_name] = alias_value
    return normalized


@dataclass(frozen=True, slots=True)
class CaptureConfig:
    source_type: str = "webcam"
    source_value: int | str = 0
    source_id: str = "webcam-0"
    frame_width: int | None = 1280
    frame_height: int | None = 720
    fps_limit: int = 5
    connect_timeout_sec: float = 3.0
    reconnect_enabled: bool = True
    reconnect_max_attempts: int = 3
    reconnect_backoff_sec: float = 0.5
    reconnect_backoff_mode: str = "fixed"
    reconnect_backoff_max_sec: float = 8.0
    reconnect_jitter_enabled: bool = False
    reconnect_jitter_ratio: float = 0.15
    read_failure_tolerance: int = 2

    def __post_init__(self) -> None:
        source_type = _require_text(self.source_type, "source_type")
        if source_type not in {"webcam", "ip_camera", "image_file", "video_file", "mock"}:
            raise ValueError(f"unsupported source_type: {source_type}")
        object.__setattr__(self, "source_type", source_type)
        if not isinstance(self.source_value, (int, str)):
            raise TypeError("source_value must be an int or string")
        if isinstance(self.source_value, str):
            object.__setattr__(self, "source_value", _require_text(self.source_value, "source_value"))
        object.__setattr__(self, "source_id", _require_text(self.source_id, "source_id"))
        object.__setattr__(self, "frame_width", _require_optional_int(self.frame_width, "frame_width", minimum=1))
        object.__setattr__(self, "frame_height", _require_optional_int(self.frame_height, "frame_height", minimum=1))
        object.__setattr__(self, "fps_limit", _require_int(self.fps_limit, "fps_limit", minimum=1))
        object.__setattr__(
            self,
            "connect_timeout_sec",
            _require_positive_number(self.connect_timeout_sec, "connect_timeout_sec"),
        )
        object.__setattr__(self, "reconnect_enabled", _require_bool(self.reconnect_enabled, "reconnect_enabled"))
        object.__setattr__(
            self,
            "reconnect_max_attempts",
            _require_int(self.reconnect_max_attempts, "reconnect_max_attempts", minimum=0),
        )
        object.__setattr__(
            self,
            "reconnect_backoff_sec",
            _require_positive_number(self.reconnect_backoff_sec, "reconnect_backoff_sec"),
        )
        reconnect_backoff_mode = _require_text(self.reconnect_backoff_mode, "reconnect_backoff_mode").lower()
        if reconnect_backoff_mode not in {"fixed", "exponential"}:
            raise ValueError("reconnect_backoff_mode must be 'fixed' or 'exponential'")
        object.__setattr__(self, "reconnect_backoff_mode", reconnect_backoff_mode)
        reconnect_backoff_max_sec = _require_positive_number(
            self.reconnect_backoff_max_sec,
            "reconnect_backoff_max_sec",
        )
        if reconnect_backoff_max_sec < self.reconnect_backoff_sec:
            raise ValueError("reconnect_backoff_max_sec must be >= reconnect_backoff_sec")
        object.__setattr__(self, "reconnect_backoff_max_sec", reconnect_backoff_max_sec)
        object.__setattr__(
            self,
            "reconnect_jitter_enabled",
            _require_bool(self.reconnect_jitter_enabled, "reconnect_jitter_enabled"),
        )
        object.__setattr__(
            self,
            "reconnect_jitter_ratio",
            _require_probability(self.reconnect_jitter_ratio, "reconnect_jitter_ratio"),
        )
        object.__setattr__(
            self,
            "read_failure_tolerance",
            _require_int(self.read_failure_tolerance, "read_failure_tolerance", minimum=0),
        )


@dataclass(frozen=True, slots=True)
class PreprocessConfig:
    enable_quality_gate: bool = True
    min_quality_score: float = 0.0
    max_retry_count: int = 1
    enable_grayscale: bool = True
    enable_contrast_enhance: bool = True
    contrast_alpha: float = 1.15
    contrast_beta: float = 0.0
    retry_with_enhancement: bool = True
    retry_contrast_alpha: float = 1.35
    enable_roi: bool = False
    roi: tuple[float, float, float, float] | None = None
    roi_fallback_to_full_frame: bool = True
    laplacian_variance_threshold: float = 40.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "enable_quality_gate", _require_bool(self.enable_quality_gate, "enable_quality_gate"))
        object.__setattr__(self, "min_quality_score", _require_probability(self.min_quality_score, "min_quality_score"))
        object.__setattr__(self, "max_retry_count", _require_int(self.max_retry_count, "max_retry_count", minimum=0))
        if self.max_retry_count > 1:
            raise ValueError("max_retry_count must remain <= 1 in the staged single-retry pipeline")
        object.__setattr__(self, "enable_grayscale", _require_bool(self.enable_grayscale, "enable_grayscale"))
        object.__setattr__(
            self,
            "enable_contrast_enhance",
            _require_bool(self.enable_contrast_enhance, "enable_contrast_enhance"),
        )
        object.__setattr__(self, "contrast_alpha", _require_positive_number(self.contrast_alpha, "contrast_alpha"))
        if isinstance(self.contrast_beta, bool) or not isinstance(self.contrast_beta, (int, float)):
            raise TypeError("contrast_beta must be a number")
        object.__setattr__(self, "contrast_beta", float(self.contrast_beta))
        object.__setattr__(
            self,
            "retry_with_enhancement",
            _require_bool(self.retry_with_enhancement, "retry_with_enhancement"),
        )
        object.__setattr__(
            self,
            "retry_contrast_alpha",
            _require_positive_number(self.retry_contrast_alpha, "retry_contrast_alpha"),
        )
        object.__setattr__(self, "enable_roi", _require_bool(self.enable_roi, "enable_roi"))
        if self.roi is not None:
            if len(self.roi) != 4:
                raise ValueError("roi must contain exactly four normalized values")
            x, y, width, height = (float(value) for value in self.roi)
            for value, field_name in zip((x, y, width, height), ("roi.x", "roi.y", "roi.width", "roi.height"), strict=True):
                if value < 0.0 or value > 1.0:
                    raise ValueError(f"{field_name} must be between 0 and 1")
            if width <= 0.0 or height <= 0.0:
                raise ValueError("roi width and height must be > 0")
            if x + width > 1.0 or y + height > 1.0:
                raise ValueError("roi must remain within normalized frame bounds")
            object.__setattr__(self, "roi", (x, y, width, height))
        object.__setattr__(
            self,
            "roi_fallback_to_full_frame",
            _require_bool(self.roi_fallback_to_full_frame, "roi_fallback_to_full_frame"),
        )
        object.__setattr__(
            self,
            "laplacian_variance_threshold",
            _require_positive_number(self.laplacian_variance_threshold, "laplacian_variance_threshold"),
        )

    @property
    def enable_roi_crop(self) -> bool:
        return self.enable_roi

    @property
    def roi_ratio(self) -> tuple[float, float, float, float] | None:
        return self.roi

    @property
    def min_sharpness_score(self) -> float:
        return self.laplacian_variance_threshold

    @property
    def allow_one_retry_enhance(self) -> bool:
        return self.retry_with_enhancement and self.max_retry_count > 0


@dataclass(frozen=True, slots=True)
class DecodeConfig:
    decoder_backend: str = "zxingcpp"
    allowed_symbologies: tuple[str, ...] = ("QR", "BARCODE")
    enable_qr: bool = True
    enable_barcode: bool = True
    prefer_qr_first: bool = True
    allow_multi_decode: bool = False
    try_rotate: bool = True
    try_downscale: bool = True
    try_invert: bool = True

    def __post_init__(self) -> None:
        decoder_backend = _require_text(self.decoder_backend, "decoder_backend").lower()
        if decoder_backend not in {"stub", "zxingcpp"}:
            raise ValueError("decoder_backend must be 'stub' or 'zxingcpp'")
        object.__setattr__(self, "decoder_backend", decoder_backend)
        if not self.allowed_symbologies:
            raise ValueError("allowed_symbologies cannot be empty")
        cleaned = tuple(_require_text(item, "allowed_symbologies item") for item in self.allowed_symbologies)
        object.__setattr__(self, "allowed_symbologies", cleaned)
        object.__setattr__(self, "enable_qr", _require_bool(self.enable_qr, "enable_qr"))
        object.__setattr__(self, "enable_barcode", _require_bool(self.enable_barcode, "enable_barcode"))
        if not self.enable_qr and not self.enable_barcode:
            raise ValueError("at least one decoder path must be enabled")
        object.__setattr__(self, "prefer_qr_first", _require_bool(self.prefer_qr_first, "prefer_qr_first"))
        object.__setattr__(self, "allow_multi_decode", _require_bool(self.allow_multi_decode, "allow_multi_decode"))
        object.__setattr__(self, "try_rotate", _require_bool(self.try_rotate, "try_rotate"))
        object.__setattr__(self, "try_downscale", _require_bool(self.try_downscale, "try_downscale"))
        object.__setattr__(self, "try_invert", _require_bool(self.try_invert, "try_invert"))


@dataclass(frozen=True, slots=True)
class DedupConfig:
    enable_same_frame_dedup: bool = True
    enable_time_window_dedup: bool = True
    dedup_window_sec: int = 2
    window_sec: int | None = None
    key_fields: tuple[str, str] = ("asset_id", "source_id")
    time_field: str = "frame_time"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "enable_same_frame_dedup",
            _require_bool(self.enable_same_frame_dedup, "enable_same_frame_dedup"),
        )
        object.__setattr__(
            self,
            "enable_time_window_dedup",
            _require_bool(self.enable_time_window_dedup, "enable_time_window_dedup"),
        )
        if self.window_sec is not None and self.dedup_window_sec != 2 and self.window_sec != self.dedup_window_sec:
            raise ValueError("window_sec conflicts with dedup_window_sec")
        window_value = self.window_sec if self.window_sec is not None else self.dedup_window_sec
        object.__setattr__(self, "dedup_window_sec", _require_int(window_value, "dedup_window_sec", minimum=1))
        object.__setattr__(self, "window_sec", self.dedup_window_sec)
        if tuple(self.key_fields) != ("asset_id", "source_id"):
            raise ValueError("key_fields is a frozen V1 contract field and must remain ('asset_id', 'source_id')")
        if self.time_field != "frame_time":
            raise ValueError("time_field is a frozen V1 contract field and must remain 'frame_time'")


@dataclass(frozen=True, slots=True)
class GatewayConfig:
    base_url: str = "http://127.0.0.1:8000"
    scan_result_path: str = "/scan/result"
    request_timeout_sec: float = 5.0
    strict_response_validation: bool = True
    user_agent: str = "vision-client/0.1"

    def __post_init__(self) -> None:
        base_url = _require_text(self.base_url, "base_url")
        parsed = urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("base_url must be a valid http or https URL")
        object.__setattr__(self, "base_url", base_url.rstrip("/"))

        path = _require_text(self.scan_result_path, "scan_result_path")
        if path != "/scan/result":
            raise ValueError("scan_result_path is a frozen V1 contract field and must remain '/scan/result'")
        object.__setattr__(self, "scan_result_path", path)
        object.__setattr__(
            self,
            "request_timeout_sec",
            _require_positive_number(self.request_timeout_sec, "request_timeout_sec"),
        )
        object.__setattr__(
            self,
            "strict_response_validation",
            _require_bool(self.strict_response_validation, "strict_response_validation"),
        )
        object.__setattr__(self, "user_agent", _require_text(self.user_agent, "user_agent"))


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    run_mode: str = "mock"
    single_run: bool = True
    log_level: str = "INFO"
    stop_on_error: bool = True
    summary_on_exit: bool = True
    show_preview: bool = False
    debug_mode: bool = False
    preview_exit_key: str = "q"
    event_history_size: int = 50
    preview_graceful_degrade: bool = True
    summary_include_recent_events: bool = True
    soak_enabled: bool = False
    max_duration_sec: int | None = None
    max_frames: int | None = None
    soak_duration_sec: int | None = None
    soak_max_frames: int | None = None
    summary_json_path: str | None = None
    event_export_path: str | None = None
    health_logging_enabled: bool = True
    summary_verbosity: str = "standard"
    preview_overlay_enabled: bool = True

    def __post_init__(self) -> None:
        run_mode = _require_text(self.run_mode, "run_mode").lower()
        if run_mode not in {"mock", "live"}:
            raise ValueError("run_mode must be 'mock' or 'live'")
        object.__setattr__(self, "run_mode", run_mode)
        object.__setattr__(self, "single_run", _require_bool(self.single_run, "single_run"))
        log_level = _require_text(self.log_level, "log_level").upper()
        if log_level not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
            raise ValueError(f"unsupported log_level: {log_level}")
        object.__setattr__(self, "log_level", log_level)
        object.__setattr__(self, "stop_on_error", _require_bool(self.stop_on_error, "stop_on_error"))
        object.__setattr__(self, "summary_on_exit", _require_bool(self.summary_on_exit, "summary_on_exit"))
        object.__setattr__(self, "show_preview", _require_bool(self.show_preview, "show_preview"))
        object.__setattr__(self, "debug_mode", _require_bool(self.debug_mode, "debug_mode"))
        preview_exit_key = _require_text(self.preview_exit_key, "preview_exit_key")
        if len(preview_exit_key) != 1:
            raise ValueError("preview_exit_key must be exactly one character")
        object.__setattr__(self, "preview_exit_key", preview_exit_key)
        object.__setattr__(
            self,
            "event_history_size",
            _require_int(self.event_history_size, "event_history_size", minimum=1),
        )
        object.__setattr__(
            self,
            "preview_graceful_degrade",
            _require_bool(self.preview_graceful_degrade, "preview_graceful_degrade"),
        )
        object.__setattr__(
            self,
            "summary_include_recent_events",
            _require_bool(self.summary_include_recent_events, "summary_include_recent_events"),
        )
        object.__setattr__(self, "soak_enabled", _require_bool(self.soak_enabled, "soak_enabled"))
        object.__setattr__(
            self,
            "max_duration_sec",
            _require_optional_int(self.max_duration_sec, "max_duration_sec", minimum=1),
        )
        object.__setattr__(
            self,
            "max_frames",
            _require_optional_int(self.max_frames, "max_frames", minimum=1),
        )
        soak_duration_sec = _require_optional_int(self.soak_duration_sec, "soak_duration_sec", minimum=1)
        soak_max_frames = _require_optional_int(self.soak_max_frames, "soak_max_frames", minimum=1)
        if soak_duration_sec is not None and self.max_duration_sec is not None and soak_duration_sec != self.max_duration_sec:
            raise ValueError("soak_duration_sec conflicts with max_duration_sec")
        if soak_max_frames is not None and self.max_frames is not None and soak_max_frames != self.max_frames:
            raise ValueError("soak_max_frames conflicts with max_frames")
        effective_duration = self.max_duration_sec if self.max_duration_sec is not None else soak_duration_sec
        effective_frames = self.max_frames if self.max_frames is not None else soak_max_frames
        object.__setattr__(self, "max_duration_sec", effective_duration)
        object.__setattr__(self, "max_frames", effective_frames)
        object.__setattr__(self, "soak_duration_sec", effective_duration)
        object.__setattr__(self, "soak_max_frames", effective_frames)
        if not self.soak_enabled and (effective_duration is not None or effective_frames is not None):
            raise ValueError("max_duration_sec/max_frames require soak_enabled=True")
        if self.soak_enabled and self.single_run:
            raise ValueError("soak_enabled requires single_run=False")
        if self.soak_enabled and effective_duration is None and effective_frames is None:
            raise ValueError("soak_enabled requires max_duration_sec or max_frames")
        object.__setattr__(
            self,
            "summary_json_path",
            _require_optional_text(self.summary_json_path, "summary_json_path"),
        )
        object.__setattr__(
            self,
            "event_export_path",
            _require_optional_text(self.event_export_path, "event_export_path"),
        )
        object.__setattr__(
            self,
            "health_logging_enabled",
            _require_bool(self.health_logging_enabled, "health_logging_enabled"),
        )
        summary_verbosity = _require_text(self.summary_verbosity, "summary_verbosity").lower()
        if summary_verbosity not in {"compact", "standard", "detailed"}:
            raise ValueError("summary_verbosity must be 'compact', 'standard', or 'detailed'")
        object.__setattr__(self, "summary_verbosity", summary_verbosity)
        object.__setattr__(
            self,
            "preview_overlay_enabled",
            _require_bool(self.preview_overlay_enabled, "preview_overlay_enabled"),
        )

    @property
    def debug(self) -> bool:
        return self.debug_mode

    @property
    def dry_run(self) -> bool:
        return self.run_mode == "mock"


@dataclass(frozen=True, slots=True)
class VisionConfig:
    capture: CaptureConfig = field(default_factory=CaptureConfig)
    preprocess: PreprocessConfig = field(default_factory=PreprocessConfig)
    decode: DecodeConfig = field(default_factory=DecodeConfig)
    dedup: DedupConfig = field(default_factory=DedupConfig)
    gateway: GatewayConfig = field(default_factory=GatewayConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)

    @classmethod
    def from_overrides(
        cls,
        *,
        capture: dict[str, Any] | None = None,
        preprocess: dict[str, Any] | None = None,
        decode: dict[str, Any] | None = None,
        dedup: dict[str, Any] | None = None,
        gateway: dict[str, Any] | None = None,
        runtime: dict[str, Any] | None = None,
    ) -> VisionConfig:
        normalized_preprocess = _normalize_section_overrides(
            "preprocess",
            preprocess,
            aliases={
                "enable_roi_crop": "enable_roi",
                "roi_ratio": "roi",
                "min_sharpness_score": "laplacian_variance_threshold",
                "allow_one_retry_enhance": "retry_with_enhancement",
            },
        )
        normalized_runtime = _normalize_section_overrides(
            "runtime",
            runtime,
            aliases={"debug": "debug_mode"},
        )
        if "dry_run" in (runtime or {}):
            dry_run = normalized_runtime.pop("dry_run")
            if not isinstance(dry_run, bool):
                raise TypeError("runtime.dry_run must be a bool")
            run_mode = "mock" if dry_run else "live"
            if "run_mode" in normalized_runtime and normalized_runtime["run_mode"] != run_mode:
                raise ValueError("runtime.dry_run conflicts with runtime.run_mode")
            normalized_runtime["run_mode"] = run_mode
        normalized_dedup = _normalize_section_overrides(
            "dedup",
            dedup,
            aliases={"window_sec": "dedup_window_sec"},
        )
        return cls(
            capture=CaptureConfig(**(capture or {})),
            preprocess=PreprocessConfig(**normalized_preprocess),
            decode=DecodeConfig(**(decode or {})),
            dedup=DedupConfig(**normalized_dedup),
            gateway=GatewayConfig(**(gateway or {})),
            runtime=RuntimeConfig(**normalized_runtime),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
