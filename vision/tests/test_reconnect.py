from __future__ import annotations

import json
import unittest

from app.backoff import ReconnectBackoffPolicy
from app.config import VisionConfig
from app.runner import build_runner
from capture.base import CaptureConnectionLostError, CaptureOpenError, CaptureStreamEnded, FrameSource
from decoder.stub import StaticDecoder
from gateway.api_client import APIClient, TransportResponse
from models.decode_result import DecodeResult
from models.frame import FrameData
from parser.stub import MockScanResultBuilder
from tests._fixtures import make_qr_image


class _InitialOpenRecoversSource(FrameSource):
    def __init__(self, frame: FrameData) -> None:
        self._frame = frame
        self._opened = False
        self._served = False
        self._open_attempts = 0
        self.reconnect_calls = 0

    def open(self) -> None:
        self._open_attempts += 1
        if self._open_attempts == 1:
            raise CaptureOpenError("initial device open failed")
        self._opened = True
        self._served = False

    def read(self) -> FrameData:
        if self._served:
            raise CaptureStreamEnded("done")
        self._served = True
        return self._frame

    def close(self) -> None:
        self._opened = False

    def supports_reconnect(self) -> bool:
        return True

    def reconnect(self) -> None:
        self.reconnect_calls += 1
        self.open()


class _ConnectionLostThenReconnectSource(FrameSource):
    def __init__(self, frame: FrameData) -> None:
        self._frame = frame
        self._served = False
        self._reconnected = False
        self.reconnect_calls = 0

    def open(self) -> None:
        self._served = False

    def read(self) -> FrameData:
        if self._served:
            raise CaptureStreamEnded("done")
        if not self._reconnected:
            raise CaptureConnectionLostError("stream dropped")
        self._served = True
        return self._frame

    def close(self) -> None:
        return None

    def supports_reconnect(self) -> bool:
        return True

    def reconnect(self) -> None:
        self.reconnect_calls += 1
        self._reconnected = True


class _FailsFirstReconnectSource(FrameSource):
    def __init__(self, frame: FrameData) -> None:
        self._frame = frame
        self._reconnected = False
        self._served = False
        self.reconnect_calls = 0

    def open(self) -> None:
        self._served = False

    def read(self) -> FrameData:
        if self._served:
            raise CaptureStreamEnded("done")
        if not self._reconnected:
            raise CaptureConnectionLostError("stream dropped")
        self._served = True
        return self._frame

    def close(self) -> None:
        return None

    def supports_reconnect(self) -> bool:
        return True

    def reconnect(self) -> None:
        self.reconnect_calls += 1
        if self.reconnect_calls == 1:
            raise CaptureOpenError("retry failed once")
        self._reconnected = True


class _RecordingClock:
    def __init__(self) -> None:
        self.sleeps: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(float(seconds))


def _success_client(config: VisionConfig) -> APIClient:
    return APIClient(
        config.gateway,
        transport=lambda url, payload, timeout_sec, headers: TransportResponse(
            status_code=200,
            body=json.dumps(
                {
                    "success": True,
                    "code": "SCAN_ACCEPTED",
                    "message": "accepted",
                    "asset_id": payload["asset_id"],
                    "extra": {"server": "reconnect-test"},
                }
            ).encode("utf-8"),
        ),
    )


class ReconnectTests(unittest.TestCase):
    def test_backoff_policy_supports_fixed_and_exponential_modes(self) -> None:
        fixed = ReconnectBackoffPolicy.from_config(
            VisionConfig.from_overrides(capture={"reconnect_backoff_mode": "fixed"}).capture
        )
        exponential = ReconnectBackoffPolicy.from_config(
            VisionConfig.from_overrides(capture={"reconnect_backoff_mode": "exponential"}).capture
        )

        self.assertEqual(fixed.delay_for_attempt(3), 0.5)
        self.assertEqual(exponential.delay_for_attempt(3), 2.0)

    def test_backoff_policy_can_enable_jitter(self) -> None:
        config = VisionConfig.from_overrides(
            capture={
                "reconnect_jitter_enabled": True,
                "reconnect_jitter_ratio": 0.2,
            }
        )
        policy = ReconnectBackoffPolicy.from_config(config.capture)

        self.assertAlmostEqual(policy.delay_for_attempt(1, random_fn=lambda: 1.0), 0.6)
        self.assertAlmostEqual(policy.delay_for_attempt(1, random_fn=lambda: 0.0), 0.4)

    def test_initial_open_failure_can_recover_via_reconnect(self) -> None:
        frame = FrameData(frame_id="frame-open-1", image=make_qr_image("AS-OPEN-1"), timestamp=1700000000, source_id="cam-1")
        config = VisionConfig.from_overrides(
            capture={"source_type": "ip_camera", "source_value": "rtsp://demo", "source_id": "cam-1"},
            preprocess={"enable_quality_gate": False},
            runtime={"run_mode": "live", "single_run": False, "stop_on_error": False},
        )
        source = _InitialOpenRecoversSource(frame)
        runner = build_runner(
            config,
            source=source,
            decoder=StaticDecoder(results=[DecodeResult(raw_text="AS-OPEN-1", symbology="QR")]),
            scan_result_builder=MockScanResultBuilder(asset_id="AS-OPEN-1"),
            api_client=_success_client(config),
            sleep_fn=lambda seconds: None,
        )

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.reconnect_attempt_count, 1)
        self.assertEqual(result.reconnect_success_count, 1)
        self.assertGreaterEqual(source.reconnect_calls, 1)

    def test_exponential_backoff_delay_is_applied_on_failed_reconnect(self) -> None:
        frame = FrameData(frame_id="frame-retry-1", image=make_qr_image("AS-RETRY-1"), timestamp=1700000001, source_id="cam-1")
        config = VisionConfig.from_overrides(
            capture={
                "source_type": "ip_camera",
                "source_value": "rtsp://demo",
                "source_id": "cam-1",
                "reconnect_backoff_mode": "exponential",
                "reconnect_max_attempts": 2,
            },
            preprocess={"enable_quality_gate": False},
            runtime={"run_mode": "live", "single_run": False, "stop_on_error": False},
        )
        source = _FailsFirstReconnectSource(frame)
        clock = _RecordingClock()
        runner = build_runner(
            config,
            source=source,
            decoder=StaticDecoder(results=[DecodeResult(raw_text="AS-RETRY-1", symbology="QR")]),
            scan_result_builder=MockScanResultBuilder(asset_id="AS-RETRY-1"),
            api_client=_success_client(config),
            sleep_fn=clock.sleep,
        )

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertGreaterEqual(len(clock.sleeps), 1)
        self.assertAlmostEqual(clock.sleeps[0], 0.5)
        self.assertEqual(result.reconnect_success_count, 1)


if __name__ == "__main__":
    unittest.main()
