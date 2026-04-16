from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from app.config import VisionConfig
from app.runner import LiveModeConfigurationError, LiveModeNotReadyError, PreviewUnavailableError, build_runner
from capture.base import (
    CaptureConnectionLostError,
    CaptureDependencyError,
    CaptureOpenError,
    CaptureStreamEnded,
    CaptureTemporaryReadError,
    FrameSource,
)
from capture.mock import StaticFrameSource
from decoder.stub import StaticDecoder
from gateway.api_client import APIClient, TransportResponse
from models.decode_result import DecodeResult
from models.frame import FrameData
from parser.stub import MockScanResultBuilder
from tests._fixtures import blur_image, make_qr_image, remove_temp_file, save_temp_image, save_temp_video
import app.runner as runner_module
import main as main_module


class _OpenFailsSource(FrameSource):
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def open(self) -> None:
        raise self._exc

    def read(self) -> FrameData:
        raise AssertionError("read should not be called when open fails")

    def close(self) -> None:
        return None


class _FiniteFrameSource(FrameSource):
    def __init__(self, frames: list[FrameData]) -> None:
        self._frames = list(frames)
        self._index = 0
        self._is_open = False

    def open(self) -> None:
        self._is_open = True
        self._index = 0

    def read(self) -> FrameData:
        if not self._is_open:
            raise RuntimeError("finite frame source is not open")
        if self._index >= len(self._frames):
            raise CaptureStreamEnded("finite frame source reached end of stream")
        frame = self._frames[self._index]
        self._index += 1
        return frame

    def close(self) -> None:
        self._is_open = False


class _RecordingPreviewRenderer:
    def __init__(self, *, stop_after_calls: int | None = None) -> None:
        self.calls = 0
        self.closed = False
        self._stop_after_calls = stop_after_calls

    def render(self, output) -> bool:
        del output
        self.calls += 1
        return self._stop_after_calls is not None and self.calls >= self._stop_after_calls

    def close(self) -> None:
        self.closed = True


class _TemporaryFailureThenFrameSource(FrameSource):
    def __init__(self, frame: FrameData) -> None:
        self._frame = frame
        self._step = 0
        self._open = False

    def open(self) -> None:
        self._open = True
        self._step = 0

    def read(self) -> FrameData:
        if not self._open:
            raise RuntimeError("source is not open")
        self._step += 1
        if self._step == 1:
            raise CaptureTemporaryReadError("temporary read issue")
        if self._step == 2:
            return self._frame
        raise CaptureStreamEnded("done")

    def close(self) -> None:
        self._open = False


class _ReconnectSuccessSource(FrameSource):
    def __init__(self, frame: FrameData) -> None:
        self._frame = frame
        self._open = False
        self._reconnected = False
        self._served = False
        self.reconnect_calls = 0
        self.open_calls = 0

    def open(self) -> None:
        self._open = True
        self.open_calls += 1
        self._served = False

    def read(self) -> FrameData:
        if not self._open:
            raise RuntimeError("source is not open")
        if self._served:
            raise CaptureStreamEnded("done")
        if not self._reconnected:
            raise CaptureConnectionLostError("stream dropped")
        self._served = True
        self._reconnected = False
        return self._frame

    def close(self) -> None:
        self._open = False

    def supports_reconnect(self) -> bool:
        return True

    def reconnect(self) -> None:
        self.reconnect_calls += 1
        self._reconnected = True
        self._open = True


class _ReconnectFailSource(FrameSource):
    def __init__(self) -> None:
        self._open = False
        self.reconnect_calls = 0

    def open(self) -> None:
        self._open = True

    def read(self) -> FrameData:
        raise CaptureConnectionLostError("stream dropped permanently")

    def close(self) -> None:
        self._open = False

    def supports_reconnect(self) -> bool:
        return True

    def reconnect(self) -> None:
        self.reconnect_calls += 1
        raise CaptureOpenError("reconnect failed")


class _KeyboardInterruptSource(FrameSource):
    def open(self) -> None:
        return None

    def read(self) -> FrameData:
        raise KeyboardInterrupt()

    def close(self) -> None:
        return None


class _FakeClock:
    def __init__(self) -> None:
        self._now = 0.0
        self._wall = 1700000000.0

    def monotonic(self) -> float:
        return self._now

    def time(self) -> float:
        return self._wall + self._now

    def sleep(self, seconds: float) -> None:
        self._now += float(seconds)


class RunnerTests(unittest.TestCase):
    def test_mock_mode_uses_inprocess_formal_contract_transport(self) -> None:
        runner = build_runner(VisionConfig(), mock_asset_id="AS-RUN-001")

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.submit_result.code, "SCAN_ACCEPTED")
        self.assertEqual(result.submit_result.response_payload["asset_id"], "AS-RUN-001")
        self.assertEqual(result.submit_result.response_payload["extra"]["transport"], "inprocess_contract_mock")

    def test_live_mode_stub_decoder_backend_is_rejected(self) -> None:
        config = VisionConfig.from_overrides(runtime={"run_mode": "live"}, decode={"decoder_backend": "stub"})

        with self.assertRaises(LiveModeNotReadyError) as ctx:
            build_runner(config)

        self.assertIn("decoder_backend='stub'", str(ctx.exception))

    def test_live_mode_invalid_ip_camera_source_value_is_rejected(self) -> None:
        config = VisionConfig.from_overrides(
            capture={"source_type": "ip_camera", "source_value": 0, "source_id": "ip-1"},
            runtime={"run_mode": "live"},
        )

        with self.assertRaises(LiveModeConfigurationError) as ctx:
            build_runner(
                config,
                decoder=StaticDecoder(results=[DecodeResult(raw_text="AS-1", symbology="QR")]),
                scan_result_builder=MockScanResultBuilder(asset_id="AS-1"),
            )

        self.assertIn("stream URL string", str(ctx.exception))

    def test_live_mode_dependency_failure_is_reported_as_stable_result(self) -> None:
        config = VisionConfig.from_overrides(runtime={"run_mode": "live"})
        runner = build_runner(
            config,
            source=_OpenFailsSource(
                CaptureDependencyError("live webcam capture requires cv2; install opencv-python before using --run-mode live")
            ),
            decoder=StaticDecoder(results=[DecodeResult(raw_text="AS-2", symbology="QR")]),
            scan_result_builder=MockScanResultBuilder(asset_id="AS-2"),
        )

        result = runner.run()

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.error.error_code, "CAPTURE_DEPENDENCY_MISSING")
        self.assertIn("opencv-python", result.error.message)

    def test_live_mode_camera_open_failure_is_reported_as_stable_result(self) -> None:
        config = VisionConfig.from_overrides(runtime={"run_mode": "live"})
        runner = build_runner(
            config,
            source=_OpenFailsSource(CaptureOpenError("unable to open webcam source 0; check camera availability")),
            decoder=StaticDecoder(results=[DecodeResult(raw_text="AS-3", symbology="QR")]),
            scan_result_builder=MockScanResultBuilder(asset_id="AS-3"),
        )

        result = runner.run()

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.error.error_code, "CAPTURE_OPEN_FAILED")
        self.assertIn("unable to open webcam source", result.error.message)

    def test_live_mode_image_file_source_can_run_real_decode_flow(self) -> None:
        image_path = save_temp_image(make_qr_image("AS-5001"))
        try:
            config = VisionConfig.from_overrides(
                capture={"source_type": "image_file", "source_value": image_path, "source_id": "demo-image"},
                runtime={"run_mode": "live"},
            )
            client = APIClient(
                config.gateway,
                transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                    status_code=200,
                    body=json.dumps(
                        {
                            "success": True,
                            "code": "SCAN_ACCEPTED",
                            "message": "accepted",
                            "asset_id": "AS-5001",
                            "extra": {"server": "runner-live"},
                        }
                    ).encode("utf-8"),
                ),
            )
            runner = build_runner(config, api_client=client)

            result = runner.run()

            self.assertEqual(result.status, "submitted")
            self.assertEqual(result.submit_request.asset_id, "AS-5001")
            self.assertEqual(result.submit_result.code, "SCAN_ACCEPTED")
        finally:
            remove_temp_file(image_path)

    def test_live_mode_video_file_runs_continuous_flow_until_eof(self) -> None:
        video_path = save_temp_video([make_qr_image("AS-5101"), make_qr_image("AS-5101")], fps=4)
        try:
            config = VisionConfig.from_overrides(
                capture={"source_type": "video_file", "source_value": video_path, "source_id": "demo-video", "fps_limit": 30},
                preprocess={"enable_quality_gate": False},
                runtime={"run_mode": "live", "single_run": False, "stop_on_error": False},
            )
            client = APIClient(
                config.gateway,
                transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                    status_code=200,
                    body=json.dumps(
                        {
                            "success": True,
                            "code": "SCAN_ACCEPTED",
                            "message": "accepted",
                            "asset_id": payload["asset_id"],
                            "extra": {"server": "runner-video"},
                        }
                    ).encode("utf-8"),
                ),
            )
            runner = build_runner(config, api_client=client)

            result = runner.run()
        finally:
            remove_temp_file(video_path)

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.submit_request.asset_id, "AS-5101")
        self.assertEqual(result.processed_frames, 2)
        self.assertEqual(result.submitted_frames, 1)
        self.assertEqual(result.skipped_frames, 1)
        self.assertEqual(result.duplicate_count, 1)
        self.assertEqual(result.submit_success_count, 1)
        self.assertEqual(result.reconnect_attempt_count, 0)
        self.assertEqual(result.ended_by, "end_of_stream")

    def test_runner_preview_can_request_graceful_stop_without_affecting_submit(self) -> None:
        frames = [
            FrameData(frame_id="frame-1", image=make_qr_image("AS-5201"), timestamp=1700000000, source_id="webcam-0"),
            FrameData(frame_id="frame-2", image=make_qr_image("AS-5202"), timestamp=1700000005, source_id="webcam-0"),
        ]
        preview = _RecordingPreviewRenderer(stop_after_calls=1)
        config = VisionConfig.from_overrides(
            capture={"source_type": "webcam", "source_value": 0, "source_id": "webcam-0", "fps_limit": 30},
            preprocess={"enable_quality_gate": False},
            runtime={"run_mode": "live", "single_run": False, "show_preview": True, "stop_on_error": False},
        )
        client = APIClient(
            config.gateway,
            transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                status_code=200,
                body=json.dumps(
                    {
                        "success": True,
                        "code": "SCAN_ACCEPTED",
                        "message": "accepted",
                        "asset_id": payload["asset_id"],
                        "extra": {"server": "runner-preview"},
                    }
                ).encode("utf-8"),
            ),
        )
        runner = build_runner(config, source=_FiniteFrameSource(frames), api_client=client, preview_renderer=preview)

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.processed_frames, 1)
        self.assertEqual(result.ended_by, "preview_exit")
        self.assertEqual(preview.calls, 1)
        self.assertTrue(preview.closed)

    def test_show_preview_disabled_does_not_call_renderer_or_change_submission(self) -> None:
        preview = _RecordingPreviewRenderer()
        config = VisionConfig.from_overrides(
            capture={"source_type": "image_file", "source_value": "ignored", "source_id": "demo-image"},
            runtime={"run_mode": "mock", "show_preview": False},
        )
        runner = build_runner(config, preview_renderer=preview, mock_asset_id="AS-RUN-002")

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.submit_request.asset_id, "AS-RUN-002")
        self.assertEqual(preview.calls, 0)
        self.assertTrue(preview.closed)

    def test_debug_mode_does_not_bypass_quality_gate(self) -> None:
        blurred = blur_image(make_qr_image("AS-5301"), kernel_size=31)
        config = VisionConfig.from_overrides(
            runtime={"run_mode": "live", "debug_mode": True},
            preprocess={"laplacian_variance_threshold": 150.0, "retry_with_enhancement": False},
        )
        runner = build_runner(
            config,
            source=StaticFrameSource(
                FrameData(frame_id="blurred-1", image=blurred, timestamp=1700000003, source_id="webcam-0")
            ),
        )

        result = runner.run()

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.error.error_code, "LOW_QUALITY")

    def test_recoverable_temporary_capture_failure_continues_main_loop(self) -> None:
        frame = FrameData(frame_id="recover-1", image=make_qr_image("AS-5401"), timestamp=1700000100, source_id="cam-1")
        config = VisionConfig.from_overrides(
            capture={"source_type": "webcam", "source_id": "cam-1", "fps_limit": 30, "read_failure_tolerance": 2},
            preprocess={"enable_quality_gate": False},
            runtime={"run_mode": "live", "single_run": False, "stop_on_error": False},
        )
        client = APIClient(
            config.gateway,
            transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                status_code=200,
                body=json.dumps(
                    {
                        "success": True,
                        "code": "SCAN_ACCEPTED",
                        "message": "accepted",
                        "asset_id": payload["asset_id"],
                        "extra": {"server": "runner-recover"},
                    }
                ).encode("utf-8"),
            ),
        )
        runner = build_runner(config, source=_TemporaryFailureThenFrameSource(frame), api_client=client, sleep_fn=lambda seconds: None)

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.failed_frames, 1)
        self.assertEqual(result.processed_frames, 1)
        self.assertEqual(result.submit_success_count, 1)
        self.assertEqual(result.ended_by, "end_of_stream")

    def test_connection_lost_can_reconnect_and_resume(self) -> None:
        frame = FrameData(frame_id="reconnect-1", image=make_qr_image("AS-5402"), timestamp=1700000101, source_id="cam-1")
        config = VisionConfig.from_overrides(
            capture={
                "source_type": "ip_camera",
                "source_value": "rtsp://demo",
                "source_id": "cam-1",
                "fps_limit": 30,
                "reconnect_enabled": True,
                "reconnect_max_attempts": 2,
            },
            preprocess={"enable_quality_gate": False},
            runtime={"run_mode": "live", "single_run": False, "stop_on_error": False},
        )
        client = APIClient(
            config.gateway,
            transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                status_code=200,
                body=json.dumps(
                    {
                        "success": True,
                        "code": "SCAN_ACCEPTED",
                        "message": "accepted",
                        "asset_id": payload["asset_id"],
                        "extra": {"server": "runner-reconnect"},
                    }
                ).encode("utf-8"),
            ),
        )
        source = _ReconnectSuccessSource(frame)
        runner = build_runner(config, source=source, api_client=client, sleep_fn=lambda seconds: None)

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.reconnect_attempt_count, 1)
        self.assertEqual(result.reconnect_success_count, 1)
        self.assertEqual(result.reconnect_fail_count, 0)
        self.assertGreaterEqual(source.reconnect_calls, 1)

    def test_reconnect_exhaustion_fails_with_stable_capture_result(self) -> None:
        config = VisionConfig.from_overrides(
            capture={
                "source_type": "ip_camera",
                "source_value": "rtsp://demo",
                "source_id": "cam-1",
                "fps_limit": 30,
                "reconnect_enabled": True,
                "reconnect_max_attempts": 2,
                "reconnect_backoff_sec": 0.01,
            },
            preprocess={"enable_quality_gate": False},
            runtime={"run_mode": "live", "single_run": False, "stop_on_error": False},
        )
        runner = build_runner(config, source=_ReconnectFailSource(), sleep_fn=lambda seconds: None)

        result = runner.run()

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.error.error_code, "CAPTURE_RECONNECT_FAILED")
        self.assertEqual(result.reconnect_attempt_count, 2)
        self.assertEqual(result.reconnect_fail_count, 2)
        self.assertEqual(result.ended_by, "capture_failure")

    def test_runtime_max_frames_alias_reaches_natural_stop(self) -> None:
        clock = _FakeClock()
        config = VisionConfig.from_overrides(
            capture={"fps_limit": 30},
            runtime={
                "run_mode": "mock",
                "single_run": False,
                "soak_enabled": True,
                "max_frames": 3,
            },
        )
        runner = build_runner(config, sleep_fn=clock.sleep, time_fn=clock.time, monotonic_fn=clock.monotonic)

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.ended_by, "soak_max_frames_reached")
        self.assertEqual(result.processed_frames, 3)
        self.assertEqual(result.run_metadata["max_frames"], 3)
        self.assertIsNone(result.run_metadata["max_duration_sec"])

    def test_runtime_max_duration_alias_reaches_natural_stop(self) -> None:
        clock = _FakeClock()
        config = VisionConfig.from_overrides(
            capture={"fps_limit": 2},
            runtime={
                "run_mode": "mock",
                "single_run": False,
                "soak_enabled": True,
                "max_duration_sec": 1,
            },
        )
        runner = build_runner(config, sleep_fn=clock.sleep, time_fn=clock.time, monotonic_fn=clock.monotonic)

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.ended_by, "soak_duration_reached")
        self.assertGreaterEqual(result.uptime_sec, 1.0)
        self.assertEqual(result.run_metadata["max_duration_sec"], 1)

    def test_keyboard_interrupt_path_returns_summary_ready_result(self) -> None:
        config = VisionConfig.from_overrides(runtime={"run_mode": "mock", "single_run": False, "stop_on_error": False})
        runner = build_runner(
            config,
            source=_KeyboardInterruptSource(),
            decoder=StaticDecoder(results=[DecodeResult(raw_text="AS-INT", symbology="QR")]),
            scan_result_builder=MockScanResultBuilder(asset_id="AS-INT"),
        )

        result = runner.run()

        self.assertEqual(result.status, "stopped")
        self.assertEqual(result.ended_by, "keyboard_interrupt")
        self.assertEqual(result.health_state, "STOPPED")
        self.assertIn("uptime_sec", result.run_metadata)
        self.assertTrue(any(event["event_type"] == "runner_stopped" for event in result.recent_events))

    def test_gateway_failure_breakdown_is_counted_without_mixing_transport_and_business(self) -> None:
        frames = [
            FrameData(frame_id="fail-1", image=make_qr_image("AS-5501"), timestamp=1700000200, source_id="cam-1"),
            FrameData(frame_id="fail-2", image=make_qr_image("AS-5502"), timestamp=1700000205, source_id="cam-1"),
        ]
        responses = iter(
            [
                TransportResponse(
                    status_code=200,
                    body=json.dumps(
                        {
                            "success": False,
                            "code": "BUSY",
                            "message": "scanner busy",
                            "asset_id": "AS-5601",
                            "extra": {"queue_depth": 1},
                        }
                    ).encode("utf-8"),
                ),
                TimeoutError("socket timed out"),
            ]
        )

        def transport(url, payload, timeout_sec, headers):
            del url, payload, timeout_sec, headers
            response = next(responses)
            if isinstance(response, Exception):
                raise response
            return response

        config = VisionConfig.from_overrides(
            capture={"fps_limit": 30},
            runtime={"run_mode": "mock", "single_run": False, "stop_on_error": False},
        )
        runner = build_runner(
            config,
            source=_FiniteFrameSource(frames),
            decoder=StaticDecoder(results=[DecodeResult(raw_text="AS-5601", symbology="QR")]),
            scan_result_builder=MockScanResultBuilder(asset_id="AS-5601"),
            api_client=APIClient(config.gateway, transport=transport),
            sleep_fn=lambda seconds: None,
        )

        result = runner.run()

        self.assertEqual(result.processed_frames, 2)
        self.assertEqual(result.submit_fail_count, 2)
        self.assertEqual(result.business_fail_count, 1)
        self.assertEqual(result.transport_fail_count, 1)
        self.assertEqual(result.http_fail_count, 0)
        self.assertEqual(result.protocol_fail_count, 0)

    def test_main_summary_on_exit_can_be_disabled_without_affecting_submission(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()

        with patch.object(main_module.logging, "basicConfig", lambda *args, **kwargs: None):
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = main_module.main(["--run-mode", "mock", "--no-summary-on-exit"])

        self.assertEqual(exit_code, 0)
        self.assertIn("submit success:", stdout.getvalue())
        self.assertNotIn("frames processed=", stdout.getvalue())
        self.assertEqual("", stderr.getvalue())

    def test_main_default_summary_contains_runtime_and_gateway_lines(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()

        with patch.object(main_module.logging, "basicConfig", lambda *args, **kwargs: None):
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = main_module.main(["--run-mode", "mock", "--max-frames", "2"])

        output = stdout.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("runtime duration_sec=", output)
        self.assertIn("frames processed=", output)
        self.assertIn("classifiers low_quality=", output)
        self.assertIn("gateway submit_success=", output)
        self.assertIn("transport_fail=", output)
        self.assertEqual("", stderr.getvalue())

    def test_summary_contains_recent_events_when_enabled(self) -> None:
        runner = build_runner(VisionConfig(), mock_asset_id="AS-RUN-009")

        result = runner.run()

        self.assertTrue(result.recent_events)
        self.assertIn("event_type", result.recent_events[-1])

    def test_preview_builder_gracefully_degrades_when_gui_unavailable(self) -> None:
        config = VisionConfig.from_overrides(runtime={"run_mode": "mock", "show_preview": True})
        with patch.object(
            runner_module,
            "CV2PreviewRenderer",
            side_effect=PreviewUnavailableError("gui unavailable"),
        ):
            runner = build_runner(config, mock_asset_id="AS-RUN-010")

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertIsNone(runner.preview_renderer)


if __name__ == "__main__":
    unittest.main()
