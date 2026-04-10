from __future__ import annotations

import unittest

from app.config import VisionConfig


class VisionConfigTests(unittest.TestCase):
    def test_defaults_are_loaded(self) -> None:
        config = VisionConfig()
        self.assertEqual(config.gateway.base_url, "http://127.0.0.1:8000")
        self.assertEqual(config.gateway.scan_result_path, "/scan/result")
        self.assertEqual(config.gateway.request_timeout_sec, 5.0)
        self.assertEqual(config.capture.source_type, "webcam")
        self.assertEqual(config.capture.source_value, 0)
        self.assertEqual(config.capture.source_id, "webcam-0")
        self.assertEqual(config.capture.connect_timeout_sec, 3.0)
        self.assertTrue(config.capture.reconnect_enabled)
        self.assertEqual(config.capture.reconnect_max_attempts, 3)
        self.assertEqual(config.capture.reconnect_backoff_mode, "fixed")
        self.assertEqual(config.capture.reconnect_backoff_max_sec, 8.0)
        self.assertFalse(config.capture.reconnect_jitter_enabled)
        self.assertEqual(config.capture.reconnect_jitter_ratio, 0.15)
        self.assertEqual(config.capture.read_failure_tolerance, 2)
        self.assertEqual(config.dedup.window_sec, 2)
        self.assertFalse(config.runtime.show_preview)
        self.assertFalse(config.runtime.debug_mode)
        self.assertEqual(config.runtime.event_history_size, 50)
        self.assertTrue(config.runtime.preview_graceful_degrade)
        self.assertTrue(config.runtime.summary_include_recent_events)
        self.assertFalse(config.runtime.soak_enabled)
        self.assertIsNone(config.runtime.soak_duration_sec)
        self.assertIsNone(config.runtime.soak_max_frames)
        self.assertIsNone(config.runtime.summary_json_path)
        self.assertIsNone(config.runtime.event_export_path)
        self.assertTrue(config.runtime.health_logging_enabled)
        self.assertEqual(config.runtime.summary_verbosity, "standard")
        self.assertTrue(config.runtime.preview_overlay_enabled)

    def test_invalid_numeric_config_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            VisionConfig.from_overrides(capture={"fps_limit": 0})
        with self.assertRaises(ValueError):
            VisionConfig.from_overrides(capture={"reconnect_max_attempts": -1})
        with self.assertRaises(ValueError):
            VisionConfig.from_overrides(capture={"reconnect_backoff_mode": "linear"})
        with self.assertRaises(ValueError):
            VisionConfig.from_overrides(capture={"reconnect_jitter_ratio": 1.5})
        with self.assertRaises(ValueError):
            VisionConfig.from_overrides(runtime={"event_history_size": 0})
        with self.assertRaises(ValueError):
            VisionConfig.from_overrides(runtime={"summary_verbosity": "verbose"})
        with self.assertRaises(ValueError):
            VisionConfig.from_overrides(runtime={"soak_enabled": True, "single_run": True, "soak_duration_sec": 10})
        with self.assertRaises(ValueError):
            VisionConfig.from_overrides(runtime={"soak_duration_sec": 10})
        with self.assertRaises(ValueError):
            VisionConfig.from_overrides(runtime={"summary_json_path": "  "})

    def test_frozen_scan_result_path_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            VisionConfig.from_overrides(gateway={"scan_result_path": "/vision/result"})


if __name__ == "__main__":
    unittest.main()
