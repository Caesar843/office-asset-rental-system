from __future__ import annotations

import json
import unittest

from app.config import VisionConfig
from app.pipeline import VisionPipeline
from app.runner import build_runner
from capture.base import CaptureConnectionLostError, CaptureStreamEnded, FrameSource
from capture.mock import StaticFrameSource
from decoder.stub import StaticDecoder
from gateway.api_client import APIClient, TransportResponse
from models.decode_result import DecodeResult
from models.frame import FrameData
from parser.stub import MockScanResultBuilder
from tests._fixtures import make_qr_image


class _ReconnectLoggingSource(FrameSource):
    def __init__(self, frame: FrameData) -> None:
        self._frame = frame
        self._reconnected = False
        self._served = False

    def open(self) -> None:
        self._served = False

    def read(self) -> FrameData:
        if self._served:
            raise CaptureStreamEnded("done")
        if not self._reconnected:
            raise CaptureConnectionLostError("connection lost")
        self._served = True
        self._reconnected = False
        return self._frame

    def close(self) -> None:
        return None

    def supports_reconnect(self) -> bool:
        return True

    def reconnect(self) -> None:
        self._reconnected = True


class LoggingTests(unittest.TestCase):
    def test_pipeline_logs_key_fields_for_successful_submit(self) -> None:
        frame = FrameData(frame_id="frame-log-1", image=b"x", timestamp=1700000000, source_id="webcam-0")
        source = StaticFrameSource(frame)
        pipeline = VisionPipeline(
            source=source,
            decoder=StaticDecoder(
                results=[DecodeResult(raw_text="AS-9901", symbology="QR", decoder_name="static_stub")]
            ),
            scan_result_builder=MockScanResultBuilder(asset_id="AS-9901"),
            api_client=APIClient(
                VisionConfig().gateway,
                transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                    status_code=200,
                    body=json.dumps(
                        {
                            "success": True,
                            "code": "SCAN_ACCEPTED",
                            "message": "accepted",
                            "asset_id": payload["asset_id"],
                            "extra": {"server": "logging-test"},
                        }
                    ).encode("utf-8"),
                ),
            ),
        )

        with self.assertLogs("app.pipeline", level="INFO") as ctx:
            with source:
                output = pipeline.run_once()

        self.assertEqual(output.status, "submitted")
        log_text = "\n".join(ctx.output)
        self.assertIn("frame_id=frame-log-1", log_text)
        self.assertIn("source_id=webcam-0", log_text)
        self.assertIn("asset_id=AS-9901", log_text)
        self.assertIn("code=SCAN_ACCEPTED", log_text)

    def test_runner_logs_reconnect_attempt_and_result(self) -> None:
        frame = FrameData(frame_id="frame-log-2", image=make_qr_image("AS-9902"), timestamp=1700000001, source_id="cam-1")
        config = VisionConfig.from_overrides(
            capture={
                "source_type": "ip_camera",
                "source_value": "rtsp://demo",
                "source_id": "cam-1",
                "reconnect_enabled": True,
                "reconnect_max_attempts": 1,
            },
            preprocess={"enable_quality_gate": False},
            runtime={"run_mode": "live", "single_run": False, "stop_on_error": False},
        )
        runner = build_runner(
            config,
            source=_ReconnectLoggingSource(frame),
            decoder=StaticDecoder(results=[DecodeResult(raw_text="AS-9902", symbology="QR")]),
            scan_result_builder=MockScanResultBuilder(asset_id="AS-9902"),
            api_client=APIClient(
                config.gateway,
                transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                    status_code=200,
                    body=json.dumps(
                        {
                            "success": True,
                            "code": "SCAN_ACCEPTED",
                            "message": "accepted",
                            "asset_id": payload["asset_id"],
                            "extra": {"server": "logging-test"},
                        }
                    ).encode("utf-8"),
                ),
            ),
            sleep_fn=lambda seconds: None,
        )

        with self.assertLogs("app.runner", level="INFO") as ctx:
            result = runner.run()

        self.assertEqual(result.status, "submitted")
        log_text = "\n".join(ctx.output)
        self.assertIn("reconnect_attempt", log_text)
        self.assertIn("reconnect_success", log_text)
        self.assertIn("health transition", log_text)


if __name__ == "__main__":
    unittest.main()
