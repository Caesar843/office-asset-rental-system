from __future__ import annotations

import unittest
from unittest.mock import patch

from app.config import VisionConfig
from app.runner import PreviewUnavailableError, build_runner
import app.runner as runner_module


class _FailingPreviewRenderer:
    def __init__(self) -> None:
        self.closed = False

    def render(self, output) -> bool:
        del output
        raise RuntimeError("preview render exploded")

    def close(self) -> None:
        self.closed = True


class PreviewTests(unittest.TestCase):
    def test_preview_disabled_leaves_mock_submit_unchanged(self) -> None:
        runner = build_runner(
            VisionConfig.from_overrides(runtime={"show_preview": False}),
            mock_asset_id="AS-PREVIEW-001",
        )

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.submit_request.asset_id, "AS-PREVIEW-001")

    def test_preview_gracefully_degrades_when_gui_unavailable(self) -> None:
        config = VisionConfig.from_overrides(runtime={"show_preview": True, "preview_graceful_degrade": True})
        with patch.object(runner_module, "CV2PreviewRenderer", side_effect=PreviewUnavailableError("headless")):
            runner = build_runner(config, mock_asset_id="AS-PREVIEW-002")

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertIsNone(runner.preview_renderer)
        self.assertTrue(any(event["event_type"] == "preview_startup_unavailable" for event in result.recent_events))

    def test_preview_runtime_failure_gracefully_degrades(self) -> None:
        config = VisionConfig.from_overrides(runtime={"show_preview": True, "preview_graceful_degrade": True})
        preview = _FailingPreviewRenderer()
        runner = build_runner(config, preview_renderer=preview, mock_asset_id="AS-PREVIEW-003")

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertIsNone(runner.preview_renderer)
        self.assertTrue(preview.closed)
        self.assertTrue(any(event["event_type"] == "preview_runtime_failed" for event in result.recent_events))

    def test_preview_overlay_toggle_does_not_change_submit_chain(self) -> None:
        config = VisionConfig.from_overrides(
            runtime={"show_preview": False, "preview_overlay_enabled": False},
        )
        runner = build_runner(config, mock_asset_id="AS-PREVIEW-004")

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.submit_request.asset_id, "AS-PREVIEW-004")
        self.assertNotIn("health_state", result.submit_request.to_payload())


if __name__ == "__main__":
    unittest.main()
