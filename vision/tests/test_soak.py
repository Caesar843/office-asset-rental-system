from __future__ import annotations

import unittest

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


class SoakTests(unittest.TestCase):
    def test_soak_duration_sec_reaches_natural_stop(self) -> None:
        clock = _FakeClock()
        config = VisionConfig.from_overrides(
            capture={"fps_limit": 2},
            runtime={
                "run_mode": "mock",
                "single_run": False,
                "soak_enabled": True,
                "soak_duration_sec": 1,
            },
        )
        runner = build_runner(config, sleep_fn=clock.sleep, time_fn=clock.time, monotonic_fn=clock.monotonic)

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.ended_by, "soak_duration_reached")
        self.assertGreaterEqual(result.uptime_sec, 1.0)
        self.assertGreaterEqual(result.processed_frames, 2)
        self.assertEqual(result.submit_success_count, result.submitted_frames)

    def test_soak_max_frames_reaches_natural_stop(self) -> None:
        clock = _FakeClock()
        config = VisionConfig.from_overrides(
            capture={"fps_limit": 30},
            runtime={
                "run_mode": "mock",
                "single_run": False,
                "soak_enabled": True,
                "soak_max_frames": 3,
            },
        )
        runner = build_runner(config, sleep_fn=clock.sleep, time_fn=clock.time, monotonic_fn=clock.monotonic)

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.ended_by, "soak_max_frames_reached")
        self.assertEqual(result.processed_frames, 3)
        self.assertEqual(result.submitted_frames, 3)
        self.assertEqual(result.submit_success_count, 3)

    def test_soak_with_dual_limits_uses_first_trigger(self) -> None:
        clock = _FakeClock()
        config = VisionConfig.from_overrides(
            capture={"fps_limit": 2},
            runtime={
                "run_mode": "mock",
                "single_run": False,
                "soak_enabled": True,
                "soak_duration_sec": 1,
                "soak_max_frames": 10,
            },
        )
        runner = build_runner(config, sleep_fn=clock.sleep, time_fn=clock.time, monotonic_fn=clock.monotonic)

        result = runner.run()

        self.assertEqual(result.ended_by, "soak_duration_reached")
        self.assertLess(result.processed_frames, 10)

    def test_soak_summary_contains_runtime_counters(self) -> None:
        clock = _FakeClock()
        config = VisionConfig.from_overrides(
            runtime={
                "run_mode": "mock",
                "single_run": False,
                "soak_enabled": True,
                "soak_max_frames": 2,
            },
        )
        runner = build_runner(config, sleep_fn=clock.sleep, time_fn=clock.time, monotonic_fn=clock.monotonic)

        result = runner.run()

        self.assertEqual(result.processed_frames, 2)
        self.assertGreaterEqual(result.uptime_sec, 0.0)
        self.assertEqual(result.health_state, "STOPPED")
        self.assertIn("uptime_sec", result.run_metadata)


if __name__ == "__main__":
    unittest.main()
