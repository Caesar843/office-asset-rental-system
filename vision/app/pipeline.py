from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
from typing import Callable

from capture.base import (
    CaptureConnectionLostError,
    CaptureReadError,
    CaptureStreamEnded,
    CaptureTemporaryReadError,
    FrameSource,
)
from decoder.base import Decoder, DecoderDependencyError, DecoderError
from gateway.api_client import APIClient, SubmitResult
from models.decode_result import DecodeResult
from models.error_result import VisionErrorResult
from models.frame import FrameData, strip_transient_frame_data
from models.scan_result import ScanResult, ScanSubmitRequest
from parser.asset_id_parser import is_formal_asset_id
from parser.base import ScanResultBuilder
from preprocess.quality_check import PreprocessDependencyError, PreprocessError, QualityGateError


FramePreprocessor = Callable[[FrameData], FrameData]
LOGGER = logging.getLogger(__name__)
_REQUIRED_SUBMIT_FIELDS = ("asset_id", "raw_text", "symbology", "source_id")


def _identity_preprocess(frame: FrameData) -> FrameData:
    return frame


@dataclass(frozen=True, slots=True)
class PipelineRunOutput:
    status: str
    frame: FrameData | None = None
    decode_results: tuple[DecodeResult, ...] = field(default_factory=tuple)
    scan_result: ScanResult | None = None
    submit_request: ScanSubmitRequest | None = None
    submit_result: SubmitResult | None = None
    error: VisionErrorResult | None = None
    processed_frames: int = 0
    submitted_frames: int = 0
    skipped_frames: int = 0
    failed_frames: int = 0
    ended_by: str | None = None
    low_quality_count: int = 0
    no_code_count: int = 0
    parse_fail_count: int = 0
    duplicate_count: int = 0
    conflict_count: int = 0
    submit_success_count: int = 0
    submit_fail_count: int = 0
    transport_fail_count: int = 0
    http_fail_count: int = 0
    protocol_fail_count: int = 0
    business_fail_count: int = 0
    reconnect_attempt_count: int = 0
    reconnect_success_count: int = 0
    reconnect_fail_count: int = 0
    uptime_sec: float = 0.0
    health_state: str | None = None
    health_transitions: tuple[dict[str, object], ...] = field(default_factory=tuple)
    run_metadata: dict[str, object] = field(default_factory=dict)
    recent_events: tuple[dict[str, object], ...] = field(default_factory=tuple)


class VisionPipeline:
    def __init__(
        self,
        *,
        source: FrameSource,
        decoder: Decoder,
        scan_result_builder: ScanResultBuilder,
        api_client: APIClient,
        preprocessor: FramePreprocessor | None = None,
    ) -> None:
        self._source = source
        self._decoder = decoder
        self._scan_result_builder = scan_result_builder
        self._api_client = api_client
        self._preprocessor = preprocessor or _identity_preprocess

    def run_once(self) -> PipelineRunOutput:
        try:
            frame = self._source.read()
        except CaptureStreamEnded:
            LOGGER.info("capture end_of_stream")
            return PipelineRunOutput(status="eof")
        except CaptureTemporaryReadError as exc:
            LOGGER.warning("capture temporary_read_failed message=%s", str(exc))
            return PipelineRunOutput(
                status="failed",
                error=VisionErrorResult(stage="capture", error_code="READ_FRAME_TEMPORARY_FAILURE", message=str(exc)),
            )
        except CaptureConnectionLostError as exc:
            LOGGER.warning("capture connection_lost message=%s", str(exc))
            return PipelineRunOutput(
                status="failed",
                error=VisionErrorResult(stage="capture", error_code="CAPTURE_CONNECTION_LOST", message=str(exc)),
            )
        except CaptureReadError as exc:
            LOGGER.warning("capture read_failed message=%s", str(exc))
            return PipelineRunOutput(
                status="failed",
                error=VisionErrorResult(stage="capture", error_code="READ_FRAME_FAILED", message=str(exc)),
            )
        except Exception as exc:
            LOGGER.exception("capture unexpected_read_failure")
            return PipelineRunOutput(
                status="failed",
                error=VisionErrorResult(stage="capture", error_code="READ_FRAME_FAILED", message=str(exc)),
            )
        LOGGER.info(
            "capture ok frame_id=%s source_id=%s width=%s height=%s",
            frame.frame_id,
            frame.source_id,
            frame.width,
            frame.height,
        )

        try:
            processed = self._preprocessor(frame)
        except PreprocessDependencyError as exc:
            LOGGER.error(
                "preprocess dependency_missing frame_id=%s source_id=%s message=%s",
                frame.frame_id,
                frame.source_id,
                str(exc),
            )
            return PipelineRunOutput(
                status="failed",
                frame=frame,
                error=VisionErrorResult(
                    stage="preprocess",
                    error_code="PREPROCESS_DEPENDENCY_MISSING",
                    message=str(exc),
                    frame_id=frame.frame_id,
                    source_id=frame.source_id,
                ),
            )
        except QualityGateError as exc:
            LOGGER.info(
                "preprocess low_quality frame_id=%s source_id=%s message=%s",
                frame.frame_id,
                frame.source_id,
                str(exc),
            )
            return PipelineRunOutput(
                status="failed",
                frame=frame,
                error=VisionErrorResult(
                    stage="preprocess",
                    error_code="LOW_QUALITY",
                    message=str(exc),
                    frame_id=frame.frame_id,
                    source_id=frame.source_id,
                    extra=getattr(exc, "details", {}),
                ),
            )
        except PreprocessError as exc:
            LOGGER.info(
                "preprocess failed frame_id=%s source_id=%s message=%s",
                frame.frame_id,
                frame.source_id,
                str(exc),
            )
            return PipelineRunOutput(
                status="failed",
                frame=frame,
                error=VisionErrorResult(
                    stage="preprocess",
                    error_code="PREPROCESS_FAILED",
                    message=str(exc),
                    frame_id=frame.frame_id,
                    source_id=frame.source_id,
                    extra=getattr(exc, "details", {}),
                ),
            )
        LOGGER.info(
            "preprocess ok frame_id=%s source_id=%s steps=%s quality=%s",
            processed.frame_id,
            processed.source_id,
            processed.extra.get("preprocess_steps"),
            processed.extra.get("quality"),
        )

        try:
            decoded = self._decoder.decode(processed)
        except DecoderDependencyError as exc:
            LOGGER.error(
                "decode dependency_missing frame_id=%s source_id=%s message=%s",
                processed.frame_id,
                processed.source_id,
                str(exc),
            )
            return PipelineRunOutput(
                status="failed",
                frame=strip_transient_frame_data(processed),
                error=VisionErrorResult(
                    stage="decoder",
                    error_code="DECODER_DEPENDENCY_MISSING",
                    message=str(exc),
                    frame_id=processed.frame_id,
                    source_id=processed.source_id,
                ),
            )
        except DecoderError as exc:
            LOGGER.info(
                "decode failed frame_id=%s source_id=%s message=%s",
                processed.frame_id,
                processed.source_id,
                str(exc),
            )
            return PipelineRunOutput(
                status="failed",
                frame=strip_transient_frame_data(processed),
                error=VisionErrorResult(
                    stage="decoder",
                    error_code="DECODE_FAILED",
                    message=str(exc),
                    frame_id=processed.frame_id,
                    source_id=processed.source_id,
                ),
            )
        except Exception as exc:
            LOGGER.exception(
                "decode unexpected_failure frame_id=%s source_id=%s",
                processed.frame_id,
                processed.source_id,
            )
            return PipelineRunOutput(
                status="failed",
                frame=strip_transient_frame_data(processed),
                error=VisionErrorResult(
                    stage="decoder",
                    error_code="DECODE_FAILED",
                    message=str(exc),
                    frame_id=processed.frame_id,
                    source_id=processed.source_id,
                ),
            )

        decode_results = tuple(decoded)
        processed_for_parser = strip_transient_frame_data(processed)
        LOGGER.info(
            "decode ok frame_id=%s source_id=%s result_count=%s raw_texts=%s symbologies=%s decoder_names=%s",
            processed.frame_id,
            processed.source_id,
            len(decode_results),
            [item.raw_text for item in decode_results],
            [item.symbology for item in decode_results],
            [item.decoder_name for item in decode_results],
        )
        try:
            scan_result = self._scan_result_builder.build(processed_for_parser, decode_results)
        except Exception as exc:
            LOGGER.exception(
                "parser unexpected_failure frame_id=%s source_id=%s",
                processed.frame_id,
                processed.source_id,
            )
            return PipelineRunOutput(
                status="failed",
                frame=frame,
                decode_results=decode_results,
                error=VisionErrorResult(
                    stage="parser",
                    error_code="SCAN_RESULT_BUILD_FAILED",
                    message=str(exc),
                    frame_id=processed.frame_id,
                    source_id=processed.source_id,
                ),
            )
        if isinstance(scan_result, VisionErrorResult):
            LOGGER.info(
                "parser failed frame_id=%s source_id=%s code=%s message=%s",
                scan_result.frame_id,
                scan_result.source_id,
                scan_result.error_code,
                scan_result.message,
            )
            return PipelineRunOutput(
                status="failed",
                frame=frame,
                decode_results=decode_results,
                error=scan_result,
            )
        if not isinstance(scan_result, ScanResult):
            LOGGER.error(
                "parser invalid_scan_result frame_id=%s source_id=%s",
                frame.frame_id,
                frame.source_id,
            )
            return PipelineRunOutput(
                status="failed",
                frame=frame,
                decode_results=decode_results,
                error=VisionErrorResult(
                    stage="parser",
                    error_code="INVALID_SCAN_RESULT",
                    message="scan result builder must return ScanResult or VisionErrorResult",
                    frame_id=frame.frame_id,
                    source_id=frame.source_id,
                ),
            )
        LOGGER.info(
            "parser ok frame_id=%s source_id=%s asset_id=%s symbology=%s decoder_name=%s",
            scan_result.frame_id,
            scan_result.source_id,
            scan_result.asset_id,
            scan_result.symbology,
            scan_result.extra.get("decoder_name"),
        )

        if scan_result.is_duplicate:
            LOGGER.info(
                "dedup duplicate frame_id=%s source_id=%s asset_id=%s duplicate_reason=%s",
                scan_result.frame_id,
                scan_result.source_id,
                scan_result.asset_id,
                scan_result.duplicate_reason,
            )
            return PipelineRunOutput(
                status="skipped_duplicate",
                frame=frame,
                decode_results=decode_results,
                scan_result=scan_result,
            )

        validation_error = self._validate_scan_result_for_submit(scan_result)
        if validation_error is not None:
            LOGGER.warning(
                "submit_request invalid frame_id=%s source_id=%s asset_id=%s errors=%s",
                scan_result.frame_id,
                scan_result.source_id,
                scan_result.asset_id,
                validation_error.extra.get("validation_errors"),
            )
            return PipelineRunOutput(
                status="failed",
                frame=frame,
                decode_results=decode_results,
                scan_result=scan_result,
                error=validation_error,
            )

        try:
            submit_request = scan_result.to_submit_request()
        except Exception as exc:
            LOGGER.error(
                "submit_request build_failed frame_id=%s source_id=%s asset_id=%s message=%s",
                scan_result.frame_id,
                scan_result.source_id,
                scan_result.asset_id,
                str(exc),
            )
            return PipelineRunOutput(
                status="failed",
                frame=frame,
                decode_results=decode_results,
                scan_result=scan_result,
                error=VisionErrorResult(
                    stage="pipeline",
                    error_code="SUBMIT_REQUEST_BUILD_FAILED",
                    message=str(exc),
                    frame_id=frame.frame_id,
                    source_id=frame.source_id,
                ),
            )
        submit_result = self._api_client.submit(submit_request)
        if not submit_result.business_success:
            submit_error = self._build_submit_failure_error(scan_result, submit_result)
            LOGGER.warning(
                "submit failed frame_id=%s source_id=%s asset_id=%s status=%s http_status=%s code=%s message=%s",
                submit_request.frame_id,
                submit_request.source_id,
                submit_request.asset_id,
                submit_result.status,
                submit_result.http_status,
                submit_result.code,
                submit_result.message,
            )
            return PipelineRunOutput(
                status="failed",
                frame=frame,
                decode_results=decode_results,
                scan_result=scan_result,
                submit_request=submit_request,
                submit_result=submit_result,
                error=submit_error,
            )
        LOGGER.info(
            "submit ok frame_id=%s source_id=%s asset_id=%s code=%s message=%s",
            submit_request.frame_id,
            submit_request.source_id,
            submit_request.asset_id,
            submit_result.code,
            submit_result.message,
        )

        return PipelineRunOutput(
            status="submitted",
            frame=frame,
            decode_results=decode_results,
            scan_result=scan_result,
            submit_request=submit_request,
            submit_result=submit_result,
        )

    def _validate_scan_result_for_submit(self, scan_result: ScanResult) -> VisionErrorResult | None:
        validation_errors: list[str] = []
        for field_name in _REQUIRED_SUBMIT_FIELDS:
            value = getattr(scan_result, field_name, None)
            if not isinstance(value, str) or not value.strip():
                validation_errors.append(f"missing_or_blank:{field_name}")
        if not isinstance(scan_result.frame_time, int):
            validation_errors.append("invalid_type:frame_time")
        elif scan_result.frame_time <= 0 or scan_result.frame_time >= 10_000_000_000:
            validation_errors.append("invalid_value:frame_time")
        if not is_formal_asset_id(scan_result.asset_id):
            validation_errors.append("invalid_asset_id")
        if validation_errors:
            return VisionErrorResult(
                stage="pipeline",
                error_code="INVALID_SUBMIT_REQUEST",
                message="scan result failed pre-submit validation",
                frame_id=scan_result.frame_id,
                source_id=scan_result.source_id,
                extra={
                    "classifier": "parse_fail",
                    "validation_errors": tuple(validation_errors),
                },
            )
        return None

    def _build_submit_failure_error(
        self,
        scan_result: ScanResult,
        submit_result: SubmitResult,
    ) -> VisionErrorResult:
        failure_kind = self._classify_submit_failure(submit_result)
        error = submit_result.error or VisionErrorResult(
            stage="gateway",
            error_code=submit_result.code,
            message=submit_result.message,
            frame_id=scan_result.frame_id,
            source_id=scan_result.source_id,
        )
        extra = dict(error.extra)
        extra.update(
            {
                "classifier": "submit_fail",
                "submit_status": submit_result.status,
                "submit_failure_kind": failure_kind,
                "transport_success": submit_result.transport_success,
                "http_ok": submit_result.http_ok,
                "response_valid": submit_result.response_valid,
                "business_success": submit_result.business_success,
            }
        )
        return replace(
            error,
            frame_id=error.frame_id or scan_result.frame_id,
            source_id=error.source_id or scan_result.source_id,
            extra=extra,
        )

    def _classify_submit_failure(self, submit_result: SubmitResult) -> str:
        if not submit_result.transport_success:
            return "transport"
        if not submit_result.http_ok:
            return "http"
        if not submit_result.response_valid:
            return "protocol"
        return "business"
