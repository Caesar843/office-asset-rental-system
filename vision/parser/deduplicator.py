from __future__ import annotations

from dataclasses import dataclass
from dataclasses import replace

from app.config import DedupConfig
from models.scan_result import ScanResult


@dataclass(slots=True)
class _FrameSeenEntry:
    frame_time: int
    asset_ids: set[str]


class ScanResultDeduplicator:
    def __init__(self, config: DedupConfig) -> None:
        self._config = config
        self._recent_by_key: dict[tuple[str, str], int] = {}
        self._seen_assets_by_frame: dict[tuple[str, str], _FrameSeenEntry] = {}
        self._latest_frame_time = 0

    def apply(self, scan_result: ScanResult) -> ScanResult:
        self._latest_frame_time = max(self._latest_frame_time, scan_result.frame_time)
        self._prune(self._latest_frame_time)

        frame_key = (scan_result.frame_id or str(scan_result.frame_time), scan_result.source_id)
        frame_entry = self._seen_assets_by_frame.setdefault(
            frame_key,
            _FrameSeenEntry(frame_time=scan_result.frame_time, asset_ids=set()),
        )
        frame_entry.frame_time = max(frame_entry.frame_time, scan_result.frame_time)
        if self._config.enable_same_frame_dedup and scan_result.asset_id in frame_entry.asset_ids:
            return replace(
                scan_result,
                is_duplicate=True,
                duplicate_reason="same_frame_same_asset",
            )

        key = (scan_result.asset_id, scan_result.source_id)
        last_seen = self._recent_by_key.get(key)
        if (
            self._config.enable_time_window_dedup
            and last_seen is not None
            and scan_result.frame_time <= last_seen + self._config.dedup_window_sec
        ):
            frame_entry.asset_ids.add(scan_result.asset_id)
            return replace(
                scan_result,
                is_duplicate=True,
                duplicate_reason="within_dedup_window",
            )

        frame_entry.asset_ids.add(scan_result.asset_id)
        if self._config.enable_time_window_dedup:
            self._recent_by_key[key] = scan_result.frame_time
        return scan_result

    def _prune(self, current_frame_time: int) -> None:
        min_allowed = current_frame_time - self._config.dedup_window_sec
        stale_keys = [key for key, seen_time in self._recent_by_key.items() if seen_time < min_allowed]
        for key in stale_keys:
            self._recent_by_key.pop(key, None)

        stale_frame_keys = [
            frame_key
            for frame_key, frame_entry in self._seen_assets_by_frame.items()
            if frame_entry.frame_time < min_allowed
        ]
        for frame_key in stale_frame_keys:
            self._seen_assets_by_frame.pop(frame_key, None)

    def cache_sizes(self) -> dict[str, int]:
        return {
            "recent_by_key": len(self._recent_by_key),
            "seen_assets_by_frame": len(self._seen_assets_by_frame),
        }
