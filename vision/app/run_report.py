from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from app.config import VisionConfig
from app.pipeline import PipelineRunOutput


def _format_timestamp(value: float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value).isoformat(timespec="seconds")


def format_recent_event(event: dict[str, object]) -> str:
    head = f"[{event.get('event_type', 'event')}] {event.get('message', '')}"
    details: list[str] = []
    for key in ("code", "frame_id", "source_id", "asset_id", "duplicate_reason"):
        value = event.get(key)
        if value is not None:
            details.append(f"{key}={value}")
    extra = event.get("extra")
    if isinstance(extra, dict):
        for key in ("from_state", "to_state", "reason", "attempt", "raw_text", "symbology"):
            value = extra.get(key)
            if value is not None:
                details.append(f"{key}={value}")
    if not details:
        return head
    return f"{head} ({', '.join(details)})"


def build_run_metadata(output: PipelineRunOutput) -> dict[str, object]:
    metadata = dict(output.run_metadata)
    metadata.setdefault("status", output.status)
    metadata.setdefault("ended_by", output.ended_by)
    metadata.setdefault("health_state", output.health_state)
    metadata.setdefault("uptime_sec", output.uptime_sec)
    return metadata


def build_summary_payload(output: PipelineRunOutput) -> dict[str, object]:
    return {
        "run_metadata": build_run_metadata(output),
        "summary_counters": {
            "processed_frames": output.processed_frames,
            "submitted_frames": output.submitted_frames,
            "skipped_frames": output.skipped_frames,
            "failed_frames": output.failed_frames,
            "low_quality_count": output.low_quality_count,
            "no_code_count": output.no_code_count,
            "parse_fail_count": output.parse_fail_count,
            "duplicate_count": output.duplicate_count,
            "conflict_count": output.conflict_count,
            "submit_success_count": output.submit_success_count,
            "submit_fail_count": output.submit_fail_count,
            "transport_fail_count": output.transport_fail_count,
            "http_fail_count": output.http_fail_count,
            "protocol_fail_count": output.protocol_fail_count,
            "business_fail_count": output.business_fail_count,
            "reconnect_attempt_count": output.reconnect_attempt_count,
            "reconnect_success_count": output.reconnect_success_count,
            "reconnect_fail_count": output.reconnect_fail_count,
        },
        "health_transitions": list(output.health_transitions),
    }


def build_event_export_payload(output: PipelineRunOutput, recent_events: tuple[dict[str, object], ...]) -> dict[str, object]:
    return {
        "run_metadata": build_run_metadata(output),
        "health_transitions": list(output.health_transitions),
        "recent_events": list(recent_events),
    }


def export_json(path: str, payload: dict[str, object]) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return str(target)


def render_cli_summary(
    config: VisionConfig,
    output: PipelineRunOutput,
    *,
    recent_events: tuple[dict[str, object], ...] | None = None,
) -> str:
    metadata = build_run_metadata(output)
    events = recent_events if recent_events is not None else output.recent_events
    lines = [
        f"runtime duration_sec={output.uptime_sec:.3f} status={output.status} ended_by={output.ended_by} health={output.health_state}",
        (
            "source "
            f"run_mode={config.runtime.run_mode} source_type={config.capture.source_type} "
            f"source_id={config.capture.source_id} start={_format_timestamp(metadata.get('started_at_epoch_sec'))} "
            f"end={_format_timestamp(metadata.get('ended_at_epoch_sec'))}"
        ),
        (
            "frames "
            f"processed={output.processed_frames} submitted={output.submitted_frames} "
            f"skipped={output.skipped_frames} failed={output.failed_frames}"
        ),
        (
            "classifiers "
            f"low_quality={output.low_quality_count} no_code={output.no_code_count} "
            f"parse_fail={output.parse_fail_count} duplicate={output.duplicate_count} conflict={output.conflict_count}"
        ),
        (
            "gateway "
            f"submit_success={output.submit_success_count} submit_fail={output.submit_fail_count} "
            f"transport_fail={output.transport_fail_count} http_fail={output.http_fail_count} "
            f"protocol_fail={output.protocol_fail_count} business_fail={output.business_fail_count} "
            f"reconnect_attempt={output.reconnect_attempt_count} reconnect_success={output.reconnect_success_count} "
            f"reconnect_fail={output.reconnect_fail_count}"
        ),
    ]
    exported_paths = metadata.get("exported_paths")
    if isinstance(exported_paths, dict) and exported_paths:
        formatted = ", ".join(f"{key}={value}" for key, value in sorted(exported_paths.items()))
        lines.append(f"exports {formatted}")

    verbosity = config.runtime.summary_verbosity
    if verbosity == "compact":
        return "\n".join(lines)

    if events:
        lines.append("recent events:")
        limit = 5 if verbosity == "standard" else min(10, len(events))
        for event in list(events)[-limit:]:
            lines.append(f"  - {format_recent_event(event)}")

    if verbosity == "detailed" and output.health_transitions:
        lines.append("health transitions:")
        for item in output.health_transitions:
            from_state = item.get("from_state", "NONE")
            lines.append(
                f"  - {from_state} -> {item.get('to_state')} reason={item.get('reason')} "
                f"timestamp={item.get('timestamp')}"
            )

    return "\n".join(lines)
