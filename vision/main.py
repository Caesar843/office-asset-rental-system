from __future__ import annotations

import argparse
import logging
import sys

from app.config import VisionConfig
from app.run_report import render_cli_summary
from app.runner import build_runner


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Vision module runner")
    parser.add_argument("--run-mode", choices=("mock", "live"), default="mock")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--source-type", default="webcam")
    parser.add_argument("--source-value")
    parser.add_argument("--source-id", default="webcam-0")
    parser.add_argument("--fps-limit", type=int, default=5)
    parser.add_argument("--connect-timeout-sec", type=float, default=3.0)
    parser.add_argument("--disable-reconnect", action="store_true")
    parser.add_argument("--reconnect-max-attempts", type=int, default=3)
    parser.add_argument("--reconnect-backoff-sec", type=float, default=0.5)
    parser.add_argument("--reconnect-backoff-mode", choices=("fixed", "exponential"), default="fixed")
    parser.add_argument("--reconnect-backoff-max-sec", type=float, default=8.0)
    parser.add_argument("--reconnect-jitter-enabled", action="store_true")
    parser.add_argument("--reconnect-jitter-ratio", type=float, default=0.15)
    parser.add_argument("--read-failure-tolerance", type=int, default=2)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--max-duration-sec", type=int)
    parser.add_argument("--max-frames", type=int)
    parser.add_argument("--soak", action="store_true")
    parser.add_argument("--soak-duration-sec", type=int)
    parser.add_argument("--soak-max-frames", type=int)
    parser.add_argument("--no-summary-on-exit", action="store_true")
    parser.add_argument("--show-preview", action="store_true")
    parser.add_argument("--debug-mode", action="store_true")
    parser.add_argument("--strict-preview", action="store_true")
    parser.add_argument("--disable-preview-overlay", action="store_true")
    parser.add_argument("--event-history-size", type=int, default=50)
    parser.add_argument("--no-recent-events", action="store_true")
    parser.add_argument("--summary-json-path")
    parser.add_argument("--event-export-path")
    parser.add_argument("--disable-health-logging", action="store_true")
    parser.add_argument("--summary-verbosity", choices=("compact", "standard", "detailed"), default="standard")
    parser.add_argument("--mock-asset-id", default="AS-MOCK-001")
    return parser


def _coerce_source_value(source_type: str, raw_value: str | None) -> int | str:
    if raw_value is None:
        return 0
    if source_type == "webcam":
        try:
            return int(raw_value)
        except ValueError:
            return raw_value
    return raw_value


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    duration_limit = args.max_duration_sec if args.max_duration_sec is not None else args.soak_duration_sec
    frame_limit = args.max_frames if args.max_frames is not None else args.soak_max_frames
    soak_enabled = args.soak or duration_limit is not None or frame_limit is not None
    single_run = not (args.continuous or soak_enabled)
    try:
        config = VisionConfig.from_overrides(
            capture={
                "source_type": args.source_type,
                "source_value": _coerce_source_value(args.source_type, args.source_value),
                "source_id": args.source_id,
                "fps_limit": args.fps_limit,
                "connect_timeout_sec": args.connect_timeout_sec,
                "reconnect_enabled": not args.disable_reconnect,
                "reconnect_max_attempts": args.reconnect_max_attempts,
                "reconnect_backoff_sec": args.reconnect_backoff_sec,
                "reconnect_backoff_mode": args.reconnect_backoff_mode,
                "reconnect_backoff_max_sec": args.reconnect_backoff_max_sec,
                "reconnect_jitter_enabled": args.reconnect_jitter_enabled,
                "reconnect_jitter_ratio": args.reconnect_jitter_ratio,
                "read_failure_tolerance": args.read_failure_tolerance,
            },
            gateway={"base_url": args.base_url},
            runtime={
                "run_mode": args.run_mode,
                "single_run": single_run,
                "log_level": args.log_level,
                "show_preview": args.show_preview,
                "debug_mode": args.debug_mode,
                "preview_graceful_degrade": not args.strict_preview,
                "preview_overlay_enabled": not args.disable_preview_overlay,
                "event_history_size": args.event_history_size,
                "summary_include_recent_events": not args.no_recent_events,
                "summary_on_exit": not args.no_summary_on_exit,
                "soak_enabled": soak_enabled,
                "max_duration_sec": duration_limit,
                "max_frames": frame_limit,
                "summary_json_path": args.summary_json_path,
                "event_export_path": args.event_export_path,
                "health_logging_enabled": not args.disable_health_logging,
                "summary_verbosity": args.summary_verbosity,
            },
        )
        logging.basicConfig(
            level=getattr(logging, config.runtime.log_level, logging.INFO),
            format="%(levelname)s %(name)s %(message)s",
        )
        runner = build_runner(config, mock_asset_id=args.mock_asset_id)
        result = runner.run()
    except Exception as exc:
        print(f"vision startup failed: {exc}", file=sys.stderr)
        return 1

    summary_text = render_cli_summary(config, result, recent_events=result.recent_events)
    if result.status == "submitted" and result.submit_result is not None:
        print(
            "submit success: "
            f"http_status={result.submit_result.http_status} "
            f"code={result.submit_result.code} "
            f"message={result.submit_result.message} "
            f"asset_id={result.submit_result.response_payload.get('asset_id')}"
        )
        if config.runtime.summary_on_exit:
            print(summary_text)
        return 0

    if result.error is not None:
        print(
            "submit failed: "
            f"stage={result.error.stage} "
            f"code={result.error.error_code} "
            f"message={result.error.message}",
            file=sys.stderr,
        )
    else:
        print(f"runner finished with status={result.status}", file=sys.stderr)
    if config.runtime.summary_on_exit:
        print(summary_text, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
