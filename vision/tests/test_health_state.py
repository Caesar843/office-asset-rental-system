from __future__ import annotations

import json
import unittest

from app.config import VisionConfig
from app.runner import build_runner
from capture.base import CaptureConnectionLostError, CaptureStreamEnded, FrameSource
from decoder.stub import StaticDecoder
from gateway.api_client import APIClient, TransportResponse
from models.decode_result import DecodeResult
from models.frame import FrameData
from parser.stub import MockScanResultBuilder
from tests._fixtures import make_qr_image


class _ReconnectHealthSource(FrameSource):
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
            raise CaptureConnectionLostError("connection dropped")
        self._served = True
        return self._frame

    def close(self) -> None:
        return None

    def supports_reconnect(self) -> bool:
        return True

    def reconnect(self) -> None:
        self._reconnected = True


class HealthStateTests(unittest.TestCase):
    def test_health_transitions_are_exposed_without_polluting_submit_contract(self) -> None:
        frame = FrameData(frame_id="health-1", image=make_qr_image("AS-HEALTH-1"), timestamp=1700000000, source_id="cam-1")
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
            source=_ReconnectHealthSource(frame),
            decoder=StaticDecoder(results=[DecodeResult(raw_text="AS-HEALTH-1", symbology="QR")]),
            scan_result_builder=MockScanResultBuilder(asset_id="AS-HEALTH-1"),
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
                            "extra": {"server": "health-test"},
                        }
                    ).encode("utf-8"),
                ),
            ),
            sleep_fn=lambda seconds: None,
        )

        result = runner.run()
        transitions = [(item.get("from_state"), item.get("to_state")) for item in result.health_transitions]

        self.assertEqual(result.status, "submitted")
        self.assertIn(("STOPPED", "STARTING"), transitions)
        self.assertIn(("STARTING", "RUNNING"), transitions)
        self.assertIn(("RUNNING", "RECONNECTING"), transitions)
        self.assertIn(("RECONNECTING", "RUNNING"), transitions)
        self.assertIn(("RUNNING", "STOPPING"), transitions)
        self.assertEqual(result.health_state, "STOPPED")
        self.assertTrue(any(event["event_type"] == "health_transition" for event in result.recent_events))
        self.assertNotIn("health_state", result.submit_request.to_payload())
        self.assertNotIn("device_status", result.submit_request.to_payload())


if __name__ == "__main__":
    unittest.main()
