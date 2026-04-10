from __future__ import annotations

import logging
import random
import time
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Callable, Protocol

from app.backoff import ReconnectBackoffPolicy
from app.config import VisionConfig
from app.health_state import HealthState, HealthTransition
from app.pipeline import FramePreprocessor, PipelineRunOutput, VisionPipeline
from app.run_report import build_event_export_payload, build_summary_payload, export_json
from capture.base import CaptureDependencyError, CaptureError, CaptureOpenError, CaptureReadError, CaptureReconnectError, FrameSource
from capture.image_file import ImageFileFrameSource
from capture.ip_camera import IPCameraFrameSource
from capture.mock import StaticFrameSource
from capture.video_file import VideoFileFrameSource
from capture.webcam import WebcamFrameSource
from decoder.barcode_decoder import BarcodeDecoder
from decoder.base import Decoder
from decoder.hybrid_decoder import HybridDecoder
from decoder.qr_decoder import QRCodeDecoder
from decoder.stub import StaticDecoder
from gateway.api_client import APIClient
from gateway.mock_transport import build_contract_mock_transport
from models.decode_result import DecodeResult
from models.error_result import VisionErrorResult
from models.frame import FrameData
from parser.asset_id_parser import AssetIdParser
from parser.base import ScanResultBuilder
from parser.deduplicator import ScanResultDeduplicator
from parser.normalizer import FormalScanResultBuilder, ScanResultNormalizer
from parser.stub import MockScanResultBuilder
from preprocess.image_enhance import ImageEnhancer
from preprocess.quality_check import QualityChecker, QualityGateError
from preprocess.roi import ROIProcessor

LOGGER = logging.getLogger(__name__)


class LiveModeConfigurationError(ValueError):
    pass


class LiveModeNotReadyError(RuntimeError):
    pass


class PreviewUnavailableError(RuntimeError):
    pass


class PreviewRenderer(Protocol):
    def render(self, output: PipelineRunOutput) -> bool: ...
    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PreviewStartupNotice:
    message: str
    code: str


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    event_type: str
    message: str
    frame_id: str | None = None
    source_id: str | None = None
    asset_id: str | None = None
    code: str | None = None
    duplicate_reason: str | None = None
    timestamp: float = field(default_factory=time.time)
    extra: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "timestamp": self.timestamp,
            "event_type": self.event_type,
            "message": self.message,
        }
        if self.frame_id is not None:
            payload["frame_id"] = self.frame_id
        if self.source_id is not None:
            payload["source_id"] = self.source_id
        if self.asset_id is not None:
            payload["asset_id"] = self.asset_id
        if self.code is not None:
            payload["code"] = self.code
        if self.duplicate_reason is not None:
            payload["duplicate_reason"] = self.duplicate_reason
        if self.extra:
            payload["extra"] = dict(self.extra)
        return payload


@dataclass(slots=True)
class _RunCounters:
    processed_frames: int = 0
    submitted_frames: int = 0
    skipped_frames: int = 0
    failed_frames: int = 0
    low_quality_count: int = 0
    no_code_count: int = 0
    parse_fail_count: int = 0
    duplicate_count: int = 0
    conflict_count: int = 0
    submit_success_count: int = 0
    submit_fail_count: int = 0
    reconnect_attempt_count: int = 0
    reconnect_success_count: int = 0
    reconnect_fail_count: int = 0


class CV2PreviewRenderer:
    def __init__(self, *, window_name: str, exit_key: str, debug_mode: bool, overlay_enabled: bool) -> None:
        try:
            import cv2  # type: ignore
            import numpy as np
        except ImportError as exc:  # pragma: no cover
            raise PreviewUnavailableError("preview requires opencv-python/opencv-python-headless and numpy") from exc
        self._cv2 = cv2
        self._np = np
        self._window_name = window_name
        self._exit_key = exit_key
        self._debug_mode = debug_mode
        self._overlay_enabled = overlay_enabled
        self._ensure_gui_available()

    def _ensure_gui_available(self) -> None:
        try:  # pragma: no cover
            self._cv2.namedWindow(self._window_name, getattr(self._cv2, "WINDOW_NORMAL", 0))
            self._cv2.destroyWindow(self._window_name)
        except Exception as exc:
            raise PreviewUnavailableError("preview GUI is unavailable in the current environment") from exc

    def render(self, output: PipelineRunOutput) -> bool:
        frame = output.frame
        if frame is None or not isinstance(frame.image, self._np.ndarray):
            return False
        image = frame.image.copy()
        if image.ndim == 2:
            image = self._cv2.cvtColor(image, self._cv2.COLOR_GRAY2BGR)
        if self._overlay_enabled:
            for decode_result in output.decode_results:
                if decode_result.bbox is None:
                    continue
                x, y, width, height = decode_result.bbox
                self._cv2.rectangle(image, (x, y), (x + width, y + height), (0, 255, 0), 2)

            lines = [
                f"frame_id={frame.frame_id}",
                f"source_id={frame.source_id}",
                f"status={output.status}",
            ]
            if output.health_state is not None:
                lines.append(f"health={output.health_state}")
            if output.scan_result is not None:
                lines.append(f"asset_id={output.scan_result.asset_id}")
                lines.append(f"symbology={output.scan_result.symbology}")
                if output.scan_result.duplicate_reason is not None:
                    lines.append(f"duplicate_reason={output.scan_result.duplicate_reason}")
            if output.submit_result is not None:
                lines.append(f"submit_code={output.submit_result.code}")
            if output.error is not None:
                lines.append(f"error_code={output.error.error_code}")
            if self._debug_mode and output.decode_results:
                lines.append(f"raw_texts={[item.raw_text for item in output.decode_results]}")

            y = 24
            for line in lines:
                self._cv2.putText(
                    image,
                    line,
                    (8, y),
                    self._cv2.FONT_HERSHEY_SIMPLEX,
                    0.6,
                    (32, 32, 255) if line.startswith("error_code=") else (32, 220, 32),
                    2,
                    self._cv2.LINE_AA,
                )
                y += 22
        self._cv2.imshow(self._window_name, image)
        key = self._cv2.waitKey(1) & 0xFF
        return key == ord(self._exit_key)

    def close(self) -> None:
        try:  # pragma: no cover
            self._cv2.destroyWindow(self._window_name)
        except Exception:
            return None


@dataclass(slots=True)
class VisionRunner:
    config: VisionConfig
    source: FrameSource
    pipeline: VisionPipeline
    preview_renderer: PreviewRenderer | None = None
    preview_notice: PreviewStartupNotice | None = None
    sleep_fn: Callable[[float], None] = time.sleep
    time_fn: Callable[[], float] = time.time
    monotonic_fn: Callable[[], float] = time.monotonic
    random_fn: Callable[[], float] = random.random
    _stop_requested: bool = field(default=False, init=False, repr=False)
    _event_history: deque[RuntimeEvent] = field(init=False, repr=False)
    _counters: _RunCounters = field(init=False, repr=False)
    _backoff_policy: ReconnectBackoffPolicy = field(init=False, repr=False)
    _consecutive_capture_failures: int = field(default=0, init=False, repr=False)
    _health_state: str = field(default=HealthState.STOPPED, init=False, repr=False)
    _health_transitions: list[HealthTransition] = field(default_factory=list, init=False, repr=False)
    _run_started_at: float | None = field(default=None, init=False, repr=False)
    _run_started_monotonic: float | None = field(default=None, init=False, repr=False)
    _run_finished_at: float | None = field(default=None, init=False, repr=False)
    _run_finished_monotonic: float | None = field(default=None, init=False, repr=False)
    _exported_paths: dict[str, str] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        self._event_history = deque(maxlen=self.config.runtime.event_history_size)
        self._counters = _RunCounters()
        self._backoff_policy = ReconnectBackoffPolicy.from_config(self.config.capture)

    def stop(self) -> None:
        self._stop_requested = True

    def run(self) -> PipelineRunOutput:
        self._stop_requested = False
        self._reset_runtime_state()
        self._run_started_at = self.time_fn()
        self._run_started_monotonic = self.monotonic_fn()
        result = PipelineRunOutput(status="stopped", ended_by="not_started")
        self._transition_health(HealthState.STARTING, "runner_start")
        self._record_event(
            "runner_started",
            message="vision runner started",
            source_id=self.config.capture.source_id,
            extra={
                "run_mode": self.config.runtime.run_mode,
                "source_type": self.config.capture.source_type,
                "single_run": self.config.runtime.single_run,
                "soak_enabled": self.config.runtime.soak_enabled,
            },
        )
        if self.preview_notice is not None:
            self._record_event(
                "preview_startup_unavailable",
                message=self.preview_notice.message,
                source_id=self.config.capture.source_id,
                code=self.preview_notice.code,
            )
        try:
            self._open_source_with_recovery()
            run_single = self.config.runtime.single_run and not self.config.runtime.soak_enabled
            result = self._run_single() if run_single else self._run_loop()
        except CaptureReconnectError as exc:
            result = self._build_capture_failure("CAPTURE_RECONNECT_FAILED", str(exc))
        except CaptureDependencyError as exc:
            result = self._build_capture_failure("CAPTURE_DEPENDENCY_MISSING", str(exc))
        except CaptureOpenError as exc:
            result = self._build_capture_failure("CAPTURE_OPEN_FAILED", str(exc))
        except CaptureReadError as exc:
            result = self._build_capture_failure("CAPTURE_READ_FAILED", str(exc))
        except CaptureError as exc:
            result = self._build_capture_failure("CAPTURE_ERROR", str(exc))
        except KeyboardInterrupt:
            self.stop()
            self._transition_health(HealthState.STOPPING, "keyboard_interrupt")
            self._record_event("runner_stopped", message="runner interrupted by keyboard", source_id=self.config.capture.source_id)
            result = PipelineRunOutput(status="stopped", ended_by="keyboard_interrupt")
        finally:
            self._transition_health(HealthState.STOPPING, result.ended_by or "runner_finalizing")
            self._safe_close_source()
            self._close_preview()
            self._run_finished_at = self.time_fn()
            self._run_finished_monotonic = self.monotonic_fn()
            self._transition_health(HealthState.STOPPED, result.ended_by or "runner_stopped")

        result = self._attach_runtime_summary(result)
        self._maybe_export_runtime_artifacts(result)
        result = self._attach_runtime_summary(result)
        LOGGER.info(
            "vision runner finished status=%s processed_frames=%s submitted_frames=%s skipped_frames=%s failed_frames=%s "
            "low_quality_count=%s no_code_count=%s parse_fail_count=%s duplicate_count=%s conflict_count=%s "
            "submit_success_count=%s submit_fail_count=%s reconnect_attempt_count=%s reconnect_success_count=%s "
            "reconnect_fail_count=%s health_state=%s uptime_sec=%.3f ended_by=%s",
            result.status,
            result.processed_frames,
            result.submitted_frames,
            result.skipped_frames,
            result.failed_frames,
            result.low_quality_count,
            result.no_code_count,
            result.parse_fail_count,
            result.duplicate_count,
            result.conflict_count,
            result.submit_success_count,
            result.submit_fail_count,
            result.reconnect_attempt_count,
            result.reconnect_success_count,
            result.reconnect_fail_count,
            result.health_state,
            result.uptime_sec,
            result.ended_by,
        )
        return result

    def _reset_runtime_state(self) -> None:
        self._event_history = deque(maxlen=self.config.runtime.event_history_size)
        self._counters = _RunCounters()
        self._consecutive_capture_failures = 0
        self._health_state = HealthState.STOPPED
        self._health_transitions = []
        self._run_finished_at = None
        self._run_finished_monotonic = None
        self._exported_paths = {}

    def _open_source_with_recovery(self) -> None:
        try:
            self.source.open()
            self._record_event("capture_opened", message="capture source opened", source_id=self.config.capture.source_id)
            self._transition_health(HealthState.RUNNING, "capture_opened")
        except CaptureOpenError as exc:
            self._record_event(
                "capture_open_failed",
                message=str(exc),
                source_id=self.config.capture.source_id,
                code="CAPTURE_OPEN_FAILED",
            )
            if self._can_reconnect():
                reconnect_error = self._attempt_reconnect(reason="initial_open_failed", source_id=self.config.capture.source_id)
                if reconnect_error is None:
                    return
                raise reconnect_error
            raise

    def _run_single(self) -> PipelineRunOutput:
        output = self.pipeline.run_once()
        self._record_pipeline_output(output)
        self._render_preview(self._decorate_runtime_output(output))
        ended_by = "end_of_stream" if output.status == "eof" else "single_run"
        return replace(output, ended_by=ended_by)

    def _run_loop(self) -> PipelineRunOutput:
        ended_by = "stop_requested"
        last_meaningful = PipelineRunOutput(status="stopped")
        last_submitted: PipelineRunOutput | None = None

        while not self._stop_requested:
            soak_end = self._should_end_for_soak_before_iteration()
            if soak_end is not None:
                ended_by = soak_end
                break

            iteration_started = self.monotonic_fn()
            output = self.pipeline.run_once()
            if output.status == "eof":
                self._record_event("capture_end_of_stream", message="finite source reached end of stream", source_id=self.config.capture.source_id)
                ended_by = "end_of_stream"
                break

            self._record_pipeline_output(output)
            last_meaningful = output
            if output.status == "submitted":
                last_submitted = output
                self._consecutive_capture_failures = 0

            capture_action, capture_output = self._handle_capture_failure(output)
            if capture_output is not None:
                last_meaningful = capture_output

            if self._render_preview(self._decorate_runtime_output(last_meaningful)):
                self.stop()
                ended_by = "preview_exit"
            if self._stop_requested:
                break

            soak_end = self._should_end_for_soak_after_output()
            if soak_end is not None:
                ended_by = soak_end
                break

            if capture_action == "continue":
                self._sleep_for_iteration(iteration_started)
                continue
            if capture_action == "fatal":
                ended_by = "capture_failure"
                break

            self._consecutive_capture_failures = 0
            if self._should_stop_after_output(last_meaningful):
                ended_by = "fatal_error"
                break
            self._sleep_for_iteration(iteration_started)

        if self._stop_requested and ended_by == "stop_requested":
            ended_by = "graceful_stop"

        summary_base = last_meaningful
        if ended_by not in {"fatal_error", "capture_failure"} and last_submitted is not None:
            summary_base = last_submitted
        return replace(summary_base, ended_by=ended_by)

    def _handle_capture_failure(self, output: PipelineRunOutput) -> tuple[str | None, PipelineRunOutput | None]:
        if output.error is None or output.error.stage != "capture":
            return (None, None)

        error = output.error
        if error.error_code == "READ_FRAME_TEMPORARY_FAILURE":
            self._consecutive_capture_failures += 1
            self._record_event(
                "capture_temporary_failure",
                message=error.message,
                source_id=self.config.capture.source_id,
                code=error.error_code,
                extra={"consecutive_failures": self._consecutive_capture_failures},
            )
            if self._consecutive_capture_failures <= self.config.capture.read_failure_tolerance:
                self._transition_health(HealthState.DEGRADED, "temporary_capture_failure")
                LOGGER.info(
                    "capture temporary_failure tolerated consecutive_failures=%s tolerance=%s",
                    self._consecutive_capture_failures,
                    self.config.capture.read_failure_tolerance,
                )
                return ("continue", output)
            if self._can_reconnect():
                reconnect_error = self._attempt_reconnect(
                    reason="read_failure_tolerance_exceeded",
                    source_id=self.config.capture.source_id,
                )
                if reconnect_error is None:
                    self._consecutive_capture_failures = 0
                    return ("continue", output)
                return ("fatal", self._replace_capture_error(output, reconnect_error))
            return ("fatal", output)

        if error.error_code == "CAPTURE_CONNECTION_LOST":
            self._consecutive_capture_failures += 1
            self._record_event(
                "capture_connection_lost",
                message=error.message,
                source_id=self.config.capture.source_id,
                code=error.error_code,
            )
            if self._can_reconnect():
                reconnect_error = self._attempt_reconnect(reason="connection_lost", source_id=self.config.capture.source_id)
                if reconnect_error is None:
                    self._consecutive_capture_failures = 0
                    return ("continue", output)
                return ("fatal", self._replace_capture_error(output, reconnect_error))
            return ("fatal", output)

        return ("fatal", output)

    def _can_reconnect(self) -> bool:
        return (
            self.config.runtime.run_mode == "live"
            and self.config.capture.reconnect_enabled
            and self.source.supports_reconnect()
            and not self.source.is_finite()
            and self.config.capture.reconnect_max_attempts > 0
        )

    def _attempt_reconnect(self, *, reason: str, source_id: str) -> CaptureReconnectError | None:
        last_error: Exception | None = None
        self._transition_health(HealthState.RECONNECTING, reason)
        for attempt in range(1, self.config.capture.reconnect_max_attempts + 1):
            self._counters.reconnect_attempt_count += 1
            backoff_delay_sec = self._backoff_policy.delay_for_attempt(attempt, random_fn=self.random_fn)
            self._record_event(
                "reconnect_attempt",
                message=f"reconnect attempt {attempt} for reason={reason}",
                source_id=source_id,
                code=reason,
                extra={
                    "attempt": attempt,
                    "backoff_delay_sec": backoff_delay_sec,
                    "backoff_mode": self._backoff_policy.mode,
                    "jitter_enabled": self._backoff_policy.jitter_enabled,
                },
            )
            LOGGER.warning(
                "capture reconnect_attempt source_id=%s reason=%s attempt=%s/%s backoff_delay_sec=%.3f mode=%s jitter=%s",
                source_id,
                reason,
                attempt,
                self.config.capture.reconnect_max_attempts,
                backoff_delay_sec,
                self._backoff_policy.mode,
                self._backoff_policy.jitter_enabled,
            )
            try:
                self.source.reconnect()
            except CaptureError as exc:
                last_error = exc
                self._counters.reconnect_fail_count += 1
                self._record_event(
                    "reconnect_failed",
                    message=str(exc),
                    source_id=source_id,
                    code="RECONNECT_FAILED",
                    extra={"attempt": attempt, "backoff_delay_sec": backoff_delay_sec},
                )
                LOGGER.warning("capture reconnect_failed source_id=%s attempt=%s message=%s", source_id, attempt, str(exc))
                if attempt < self.config.capture.reconnect_max_attempts:
                    self.sleep_fn(backoff_delay_sec)
                continue

            self._counters.reconnect_success_count += 1
            self._record_event(
                "reconnect_success",
                message=f"reconnect succeeded for reason={reason}",
                source_id=source_id,
                code="RECONNECT_OK",
                extra={"attempt": attempt, "backoff_mode": self._backoff_policy.mode},
            )
            LOGGER.info("capture reconnect_success source_id=%s attempt=%s", source_id, attempt)
            self._transition_health(HealthState.RUNNING, f"reconnect_success:{reason}")
            return None

        return CaptureReconnectError(
            f"reconnect attempts exhausted for source_id={source_id}; "
            f"reason={reason}; last_error={last_error or 'unknown'}"
        )

    def _record_pipeline_output(self, output: PipelineRunOutput) -> None:
        error = output.error
        frame = output.frame

        if frame is not None and (error is None or error.stage != "capture") and self._health_state in {
            HealthState.STARTING,
            HealthState.DEGRADED,
            HealthState.RECONNECTING,
        }:
            self._transition_health(HealthState.RUNNING, "frame_flowing")

        if frame is not None:
            self._counters.processed_frames += 1
            self._record_event("frame_captured", message="frame captured", frame_id=frame.frame_id, source_id=frame.source_id)

        if output.decode_results:
            self._record_event(
                "decode_success",
                message=f"decoder returned {len(output.decode_results)} result(s)",
                frame_id=frame.frame_id if frame else None,
                source_id=frame.source_id if frame else self.config.capture.source_id,
                code="DECODE_OK",
                extra={
                    "raw_texts": [item.raw_text for item in output.decode_results],
                    "symbologies": [item.symbology for item in output.decode_results],
                },
            )

        if output.scan_result is not None and not output.scan_result.is_duplicate:
            self._record_event(
                "parse_success",
                message="scan result normalized successfully",
                frame_id=output.scan_result.frame_id,
                source_id=output.scan_result.source_id,
                asset_id=output.scan_result.asset_id,
                extra={"raw_text": output.scan_result.raw_text, "symbology": output.scan_result.symbology},
            )

        if output.status == "submitted" and output.submit_result is not None and output.submit_request is not None:
            self._counters.submitted_frames += 1
            self._counters.submit_success_count += 1
            self._record_event(
                "submit_success",
                message=output.submit_result.message,
                frame_id=output.submit_request.frame_id,
                source_id=output.submit_request.source_id,
                asset_id=output.submit_request.asset_id,
                code=output.submit_result.code,
                extra={"raw_text": output.submit_request.raw_text, "symbology": output.submit_request.symbology},
            )
            return

        if output.status == "skipped_duplicate" and output.scan_result is not None:
            self._counters.skipped_frames += 1
            self._counters.duplicate_count += 1
            self._record_event(
                "dedup_duplicate",
                message="scan result skipped as duplicate",
                frame_id=output.scan_result.frame_id,
                source_id=output.scan_result.source_id,
                asset_id=output.scan_result.asset_id,
                duplicate_reason=output.scan_result.duplicate_reason,
                extra={"raw_text": output.scan_result.raw_text, "symbology": output.scan_result.symbology},
            )
            return

        if output.status != "failed" or error is None:
            return

        self._counters.failed_frames += 1
        if error.error_code == "LOW_QUALITY":
            self._counters.low_quality_count += 1
            self._record_event("preprocess_failed", message=error.message, frame_id=error.frame_id, source_id=error.source_id, code=error.error_code)
            return
        if error.error_code == "NO_CODE":
            self._counters.no_code_count += 1
            self._record_event("decode_no_result", message=error.message, frame_id=error.frame_id, source_id=error.source_id, code=error.error_code)
            return
        if error.error_code == "MULTI_RESULT_CONFLICT":
            self._counters.conflict_count += 1
            self._record_event("dedup_conflict", message=error.message, frame_id=error.frame_id, source_id=error.source_id, code=error.error_code)
            return
        if error.stage == "parser":
            self._counters.parse_fail_count += 1
            self._record_event("parse_failed", message=error.message, frame_id=error.frame_id, source_id=error.source_id, code=error.error_code)
            return
        if error.stage == "gateway":
            self._counters.submit_fail_count += 1
            self._record_event(
                "submit_failed",
                message=error.message,
                frame_id=error.frame_id,
                source_id=error.source_id,
                asset_id=output.submit_request.asset_id if output.submit_request is not None else None,
                code=error.error_code,
                extra={
                    "raw_text": output.submit_request.raw_text if output.submit_request is not None else None,
                    "symbology": output.submit_request.symbology if output.submit_request is not None else None,
                },
            )
            return
        if error.stage == "decoder":
            self._record_event("decode_error", message=error.message, frame_id=error.frame_id, source_id=error.source_id, code=error.error_code)
            return
        if error.stage == "capture":
            self._record_event(
                "capture_failed",
                message=error.message,
                frame_id=error.frame_id,
                source_id=error.source_id or self.config.capture.source_id,
                code=error.error_code,
            )
            return
        self._record_event("pipeline_failed", message=error.message, frame_id=error.frame_id, source_id=error.source_id, code=error.error_code)

    def _record_event(
        self,
        event_type: str,
        *,
        message: str,
        frame_id: str | None = None,
        source_id: str | None = None,
        asset_id: str | None = None,
        code: str | None = None,
        duplicate_reason: str | None = None,
        extra: dict[str, object] | None = None,
    ) -> None:
        self._event_history.append(
            RuntimeEvent(
                event_type=event_type,
                message=message,
                frame_id=frame_id,
                source_id=source_id,
                asset_id=asset_id,
                code=code,
                duplicate_reason=duplicate_reason,
                timestamp=self.time_fn(),
                extra=extra or {},
            )
        )

    def _transition_health(self, next_state: str, reason: str) -> None:
        if next_state == self._health_state:
            return
        previous = self._health_state
        transition = HealthTransition(
            from_state=previous,
            to_state=next_state,
            reason=reason,
            timestamp=self.time_fn(),
        )
        self._health_state = next_state
        self._health_transitions.append(transition)
        self._record_event(
            "health_transition",
            message=f"health {previous} -> {next_state}",
            source_id=self.config.capture.source_id,
            code="HEALTH_STATE",
            extra={"from_state": previous, "to_state": next_state, "reason": reason},
        )
        if self.config.runtime.health_logging_enabled:
            LOGGER.info("health transition from_state=%s to_state=%s reason=%s", previous, next_state, reason)

    def _render_preview(self, output: PipelineRunOutput) -> bool:
        if not self.config.runtime.show_preview or self.preview_renderer is None:
            return False
        preview_output = self._decorate_runtime_output(output)
        try:
            stop_requested = self.preview_renderer.render(preview_output)
        except Exception as exc:
            LOGGER.warning("preview runtime_failed message=%s", str(exc))
            self._record_event(
                "preview_runtime_failed",
                message=str(exc),
                source_id=self.config.capture.source_id,
                code="PREVIEW_RUNTIME_FAILED",
            )
            if self.config.runtime.preview_graceful_degrade:
                self._disable_preview(f"preview degraded after runtime failure: {exc}")
                return False
            raise
        if stop_requested:
            self._record_event(
                "preview_exit_requested",
                message=f"preview exit requested via key={self.config.runtime.preview_exit_key}",
                source_id=self.config.capture.source_id,
            )
            LOGGER.info("preview stop_requested exit_key=%s", self.config.runtime.preview_exit_key)
        return stop_requested

    def _disable_preview(self, message: str) -> None:
        LOGGER.warning("%s", message)
        self._record_event("preview_degraded", message=message, source_id=self.config.capture.source_id, code="PREVIEW_DISABLED")
        if self.preview_renderer is not None:
            try:
                self.preview_renderer.close()
            except Exception:
                pass
        self.preview_renderer = None

    def _close_preview(self) -> None:
        if self.preview_renderer is None:
            return
        try:
            self.preview_renderer.close()
        except Exception as exc:
            LOGGER.warning("preview close_failed message=%s", str(exc))
            self._record_event("preview_close_failed", message=str(exc), source_id=self.config.capture.source_id, code="PREVIEW_CLOSE_FAILED")
        finally:
            self.preview_renderer = None

    def _sleep_for_iteration(self, iteration_started: float) -> None:
        interval_sec = 1.0 / float(self.config.capture.fps_limit)
        elapsed = self.monotonic_fn() - iteration_started
        remaining = interval_sec - elapsed
        if remaining > 0:
            self.sleep_fn(remaining)

    def _safe_close_source(self) -> None:
        try:
            self.source.close()
        except Exception as exc:
            LOGGER.warning("capture close_failed message=%s", str(exc))
            self._record_event("capture_close_failed", message=str(exc), source_id=self.config.capture.source_id, code="CAPTURE_CLOSE_FAILED")

    def _should_stop_after_output(self, output: PipelineRunOutput) -> bool:
        if output.error is None:
            return False
        fatal_codes = {
            "CAPTURE_DEPENDENCY_MISSING",
            "CAPTURE_OPEN_FAILED",
            "CAPTURE_RECONNECT_FAILED",
            "PREPROCESS_DEPENDENCY_MISSING",
            "DECODER_DEPENDENCY_MISSING",
            "INVALID_SCAN_RESULT",
            "SCAN_RESULT_BUILD_FAILED",
            "SUBMIT_REQUEST_BUILD_FAILED",
        }
        if output.error.error_code in fatal_codes:
            return True
        if not self.config.runtime.stop_on_error:
            return False
        return output.error.stage == "gateway"

    def _replace_capture_error(self, output: PipelineRunOutput, reconnect_error: CaptureReconnectError) -> PipelineRunOutput:
        error = VisionErrorResult(
            stage="capture",
            error_code="CAPTURE_RECONNECT_FAILED",
            message=str(reconnect_error),
            source_id=self.config.capture.source_id,
        )
        self._record_event(
            "capture_reconnect_exhausted",
            message=str(reconnect_error),
            source_id=self.config.capture.source_id,
            code="CAPTURE_RECONNECT_FAILED",
        )
        return replace(output, error=error)

    def _build_capture_failure(self, error_code: str, message: str) -> PipelineRunOutput:
        self._counters.failed_frames += 1
        self._record_event("capture_fatal", message=message, source_id=self.config.capture.source_id, code=error_code)
        return PipelineRunOutput(
            status="failed",
            error=VisionErrorResult(stage="capture", error_code=error_code, message=message, source_id=self.config.capture.source_id),
            ended_by="capture_failure",
        )

    def _should_end_for_soak_before_iteration(self) -> str | None:
        if not self.config.runtime.soak_enabled:
            return None
        duration_limit = self.config.runtime.soak_duration_sec
        if duration_limit is not None and self._current_uptime_sec() >= float(duration_limit):
            self._record_event(
                "soak_limit_reached",
                message=f"soak duration reached {duration_limit}s",
                source_id=self.config.capture.source_id,
                code="SOAK_DURATION_REACHED",
                extra={"uptime_sec": self._current_uptime_sec()},
            )
            return "soak_duration_reached"
        return None

    def _should_end_for_soak_after_output(self) -> str | None:
        if not self.config.runtime.soak_enabled:
            return None
        max_frames = self.config.runtime.soak_max_frames
        if max_frames is not None and self._counters.processed_frames >= max_frames:
            self._record_event(
                "soak_limit_reached",
                message=f"soak max_frames reached {max_frames}",
                source_id=self.config.capture.source_id,
                code="SOAK_MAX_FRAMES_REACHED",
                extra={"processed_frames": self._counters.processed_frames},
            )
            return "soak_max_frames_reached"
        return None

    def _decorate_runtime_output(self, output: PipelineRunOutput) -> PipelineRunOutput:
        return replace(output, health_state=self._health_state, uptime_sec=self._current_uptime_sec())

    def _current_uptime_sec(self) -> float:
        if self._run_started_monotonic is None:
            return 0.0
        end_value = self._run_finished_monotonic if self._run_finished_monotonic is not None else self.monotonic_fn()
        return max(0.0, float(end_value - self._run_started_monotonic))

    def _snapshot_event_history(self) -> tuple[dict[str, object], ...]:
        return tuple(event.to_dict() for event in self._event_history)

    def _build_run_metadata(self) -> dict[str, object]:
        return {
            "run_mode": self.config.runtime.run_mode,
            "source_type": self.config.capture.source_type,
            "source_id": self.config.capture.source_id,
            "started_at_epoch_sec": self._run_started_at,
            "ended_at_epoch_sec": self._run_finished_at,
            "uptime_sec": self._current_uptime_sec(),
            "soak_enabled": self.config.runtime.soak_enabled,
            "preview_requested": self.config.runtime.show_preview,
            "preview_active": self.preview_renderer is not None,
            "preview_overlay_enabled": self.config.runtime.preview_overlay_enabled,
            "exported_paths": dict(self._exported_paths),
        }

    def _maybe_export_runtime_artifacts(self, output: PipelineRunOutput) -> None:
        if self.config.runtime.summary_json_path:
            try:
                path = export_json(self.config.runtime.summary_json_path, build_summary_payload(output))
                self._exported_paths["summary_json_path"] = path
                self._record_event("summary_exported", message=f"summary exported to {path}", source_id=self.config.capture.source_id, code="SUMMARY_EXPORTED")
            except Exception as exc:
                LOGGER.warning("summary export_failed message=%s", str(exc))
                self._record_event("summary_export_failed", message=str(exc), source_id=self.config.capture.source_id, code="SUMMARY_EXPORT_FAILED")
        if self.config.runtime.event_export_path:
            recent_events = self._snapshot_event_history()
            try:
                path = export_json(self.config.runtime.event_export_path, build_event_export_payload(output, recent_events))
                self._exported_paths["event_export_path"] = path
                self._record_event("event_exported", message=f"events exported to {path}", source_id=self.config.capture.source_id, code="EVENT_EXPORT_OK")
            except Exception as exc:
                LOGGER.warning("event export_failed message=%s", str(exc))
                self._record_event("event_export_failed", message=str(exc), source_id=self.config.capture.source_id, code="EVENT_EXPORT_FAILED")

    def _attach_runtime_summary(self, output: PipelineRunOutput) -> PipelineRunOutput:
        recent_events: tuple[dict[str, object], ...] = tuple()
        all_recent_events = self._snapshot_event_history()
        if self.config.runtime.summary_include_recent_events:
            recent_events = all_recent_events
        return replace(
            output,
            processed_frames=self._counters.processed_frames,
            submitted_frames=self._counters.submitted_frames,
            skipped_frames=self._counters.skipped_frames,
            failed_frames=self._counters.failed_frames,
            low_quality_count=self._counters.low_quality_count,
            no_code_count=self._counters.no_code_count,
            parse_fail_count=self._counters.parse_fail_count,
            duplicate_count=self._counters.duplicate_count,
            conflict_count=self._counters.conflict_count,
            submit_success_count=self._counters.submit_success_count,
            submit_fail_count=self._counters.submit_fail_count,
            reconnect_attempt_count=self._counters.reconnect_attempt_count,
            reconnect_success_count=self._counters.reconnect_success_count,
            reconnect_fail_count=self._counters.reconnect_fail_count,
            uptime_sec=self._current_uptime_sec(),
            health_state=self._health_state,
            health_transitions=tuple(item.to_dict() for item in self._health_transitions),
            run_metadata=self._build_run_metadata(),
            recent_events=recent_events,
        )


def build_runner(
    config: VisionConfig | None = None,
    *,
    api_client: APIClient | None = None,
    source: FrameSource | None = None,
    decoder: Decoder | None = None,
    scan_result_builder: ScanResultBuilder | None = None,
    preprocessor: FramePreprocessor | None = None,
    preview_renderer: PreviewRenderer | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    time_fn: Callable[[], float] = time.time,
    monotonic_fn: Callable[[], float] = time.monotonic,
    random_fn: Callable[[], float] = random.random,
    mock_asset_id: str = "AS-MOCK-001",
) -> VisionRunner:
    config = config or VisionConfig()
    api_client = api_client or _build_api_client(config)
    source = source or _build_source(config)
    decoder = decoder or _build_decoder(config)
    scan_result_builder = scan_result_builder or _build_scan_result_builder(config, mock_asset_id=mock_asset_id)
    preprocessor = preprocessor or _build_preprocessor(config)
    preview_notice: PreviewStartupNotice | None = None
    if preview_renderer is None:
        preview_renderer, preview_notice = _build_preview_renderer(config)
    pipeline = VisionPipeline(
        source=source,
        decoder=decoder,
        scan_result_builder=scan_result_builder,
        api_client=api_client,
        preprocessor=preprocessor,
    )
    return VisionRunner(
        config=config,
        source=source,
        pipeline=pipeline,
        preview_renderer=preview_renderer,
        preview_notice=preview_notice,
        sleep_fn=sleep_fn,
        time_fn=time_fn,
        monotonic_fn=monotonic_fn,
        random_fn=random_fn,
    )


def _build_api_client(config: VisionConfig) -> APIClient:
    if config.runtime.run_mode == "mock":
        return APIClient(
            config.gateway,
            transport=build_contract_mock_transport(),
            strict_response_validation=True,
        )
    return APIClient(config.gateway, strict_response_validation=True)


def _build_source(config: VisionConfig) -> FrameSource:
    if config.runtime.run_mode == "mock":
        return StaticFrameSource(
            FrameData(
                frame_id="mock-frame-1",
                image=b"mock-image",
                timestamp=time.time(),
                source_id=config.capture.source_id,
                width=1,
                height=1,
                channel_count=1,
            )
        )

    if config.capture.source_type == "webcam":
        return WebcamFrameSource(
            source_value=config.capture.source_value,
            source_id=config.capture.source_id,
            connect_timeout_sec=config.capture.connect_timeout_sec,
        )
    if config.capture.source_type == "ip_camera":
        if not isinstance(config.capture.source_value, str):
            raise LiveModeConfigurationError("live mode ip_camera source_value must be a non-empty stream URL string")
        return IPCameraFrameSource(
            stream_url=config.capture.source_value,
            source_id=config.capture.source_id,
            connect_timeout_sec=config.capture.connect_timeout_sec,
        )
    if config.capture.source_type == "image_file":
        if not isinstance(config.capture.source_value, str):
            raise LiveModeConfigurationError("live mode image_file source_value must be a non-empty image path string")
        return ImageFileFrameSource(image_path=config.capture.source_value, source_id=config.capture.source_id)
    if config.capture.source_type == "video_file":
        if not isinstance(config.capture.source_value, str):
            raise LiveModeConfigurationError("live mode video_file source_value must be a non-empty video path string")
        return VideoFileFrameSource(
            video_path=config.capture.source_value,
            source_id=config.capture.source_id,
            connect_timeout_sec=config.capture.connect_timeout_sec,
        )
    raise LiveModeConfigurationError(
        "live mode only supports 'webcam', 'ip_camera', 'image_file', and 'video_file' sources, "
        f"got {config.capture.source_type!r}"
    )


def _build_decoder(config: VisionConfig) -> Decoder:
    if config.runtime.run_mode != "mock":
        if config.decode.decoder_backend == "stub":
            raise LiveModeNotReadyError("live mode cannot use decoder_backend='stub'; configure a real decoder backend")
        qr_decoder = QRCodeDecoder(config.decode)
        barcode_decoder = BarcodeDecoder(config.decode)
        return HybridDecoder(config.decode, qr_decoder=qr_decoder, barcode_decoder=barcode_decoder)
    return StaticDecoder(
        results=[
            DecodeResult(
                raw_text="AS-MOCK-001",
                symbology="QR",
                confidence=0.99,
                decoder_name="static_stub",
            )
        ]
    )


def _build_scan_result_builder(config: VisionConfig, *, mock_asset_id: str) -> ScanResultBuilder:
    if config.runtime.run_mode != "mock":
        return FormalScanResultBuilder(
            asset_id_parser=AssetIdParser(),
            normalizer=ScanResultNormalizer(),
            deduplicator=ScanResultDeduplicator(config.dedup),
        )
    return MockScanResultBuilder(asset_id=mock_asset_id)


def _build_preprocessor(config: VisionConfig) -> FramePreprocessor:
    if config.runtime.run_mode == "mock":
        return lambda frame: frame

    roi_processor = ROIProcessor(config.preprocess)
    enhancer = ImageEnhancer(config.preprocess)
    quality_checker = QualityChecker(config.preprocess)

    def preprocess(frame: FrameData) -> FrameData:
        prepared = roi_processor.apply(frame)
        prepared = enhancer.prepare(prepared)
        try:
            sharpness = quality_checker.validate(prepared)
            return quality_checker.annotate(prepared, sharpness=sharpness)
        except QualityGateError:
            if not config.preprocess.retry_with_enhancement or config.preprocess.max_retry_count < 1:
                raise
            retried = enhancer.retry(prepared)
            sharpness = quality_checker.validate(retried)
            return quality_checker.annotate(retried, sharpness=sharpness)

    return preprocess


def _build_preview_renderer(config: VisionConfig) -> tuple[PreviewRenderer | None, PreviewStartupNotice | None]:
    if not config.runtime.show_preview:
        return (None, None)
    try:
        return (
            CV2PreviewRenderer(
                window_name=f"vision-preview:{config.capture.source_id}",
                exit_key=config.runtime.preview_exit_key,
                debug_mode=config.runtime.debug_mode,
                overlay_enabled=config.runtime.preview_overlay_enabled,
            ),
            None,
        )
    except PreviewUnavailableError as exc:
        if config.runtime.preview_graceful_degrade:
            LOGGER.warning("preview startup_unavailable message=%s", str(exc))
            return (
                None,
                PreviewStartupNotice(
                    message=f"preview unavailable during startup: {exc}",
                    code="PREVIEW_STARTUP_UNAVAILABLE",
                ),
            )
        raise
