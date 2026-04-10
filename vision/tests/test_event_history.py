from __future__ import annotations

import unittest

from app.config import VisionConfig
from app.runner import build_runner


class EventHistoryTests(unittest.TestCase):
    def test_recent_event_history_is_bounded(self) -> None:
        config = VisionConfig.from_overrides(
            runtime={"event_history_size": 3, "summary_include_recent_events": True},
        )
        runner = build_runner(config, mock_asset_id="AS-HISTORY-001")

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertLessEqual(len(result.recent_events), 3)
        self.assertEqual(result.recent_events[-1]["event_type"], "health_transition")
        self.assertTrue(any(event["event_type"] == "submit_success" for event in result.recent_events))

    def test_recent_event_history_can_be_hidden_from_summary(self) -> None:
        config = VisionConfig.from_overrides(
            runtime={"summary_include_recent_events": False},
        )
        runner = build_runner(config, mock_asset_id="AS-HISTORY-002")

        result = runner.run()

        self.assertEqual(result.status, "submitted")
        self.assertEqual(result.recent_events, ())


if __name__ == "__main__":
    unittest.main()
