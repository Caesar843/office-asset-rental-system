from __future__ import annotations

import json
import unittest
from pathlib import Path

from app.config import VisionConfig
from app.runner import build_runner


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


class ReportExportTests(unittest.TestCase):
    def test_summary_and_event_exports_are_written_when_configured(self) -> None:
        clock = _FakeClock()
        summary_file = Path("tests") / "_tmp_summary_export.json"
        event_file = Path("tests") / "_tmp_event_export.json"
        try:
            summary_path = str(summary_file)
            event_path = str(event_file)
            config = VisionConfig.from_overrides(
                runtime={
                    "run_mode": "mock",
                    "single_run": False,
                    "soak_enabled": True,
                    "soak_max_frames": 2,
                    "summary_json_path": summary_path,
                    "event_export_path": event_path,
                }
            )
            runner = build_runner(config, sleep_fn=clock.sleep, time_fn=clock.time, monotonic_fn=clock.monotonic)

            result = runner.run()

            self.assertEqual(result.status, "submitted")
            self.assertTrue(Path(summary_path).exists())
            self.assertTrue(Path(event_path).exists())

            summary_payload = json.loads(Path(summary_path).read_text(encoding="utf-8"))
            event_payload = json.loads(Path(event_path).read_text(encoding="utf-8"))

            self.assertEqual(summary_payload["summary_counters"]["processed_frames"], 2)
            self.assertEqual(summary_payload["run_metadata"]["source_type"], "webcam")
            self.assertIn("health_transitions", summary_payload)
            self.assertIn("recent_events", event_payload)
            exported_json = json.dumps(event_payload, ensure_ascii=False)
            self.assertNotIn("request_seq", exported_json)
            self.assertNotIn("hw_seq", exported_json)
            self.assertNotIn("device_status", exported_json)
        finally:
            summary_file.unlink(missing_ok=True)
            event_file.unlink(missing_ok=True)

    def test_export_is_optional_and_not_forced_when_paths_absent(self) -> None:
        clock = _FakeClock()
        config = VisionConfig.from_overrides(
            runtime={"run_mode": "mock", "single_run": False, "soak_enabled": True, "soak_max_frames": 1}
        )
        runner = build_runner(config, sleep_fn=clock.sleep, time_fn=clock.time, monotonic_fn=clock.monotonic)

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.run_metadata["exported_paths"], {})


if __name__ == "__main__":
    unittest.main()
