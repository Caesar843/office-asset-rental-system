from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time

import runtime_paths  # noqa: F401
from fastapi.testclient import TestClient
from mock_mcu import MockMCUServer


MOCK_MODES = (
    "confirmed",
    "cancelled",
    "timeout",
    "late_confirm",
    "duplicate_confirm",
    "mismatch_action",
    "mismatch_request_seq",
    "mismatch_request_id",
    "busy",
    "ack_error",
    "invalid",
    "no_ack",
    "offline",
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one API + mock_mcu demo flow and print HTTP/WS results.")
    parser.add_argument("--action", choices=("borrow", "return"), required=True)
    parser.add_argument("--asset-id", required=True)
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--user-name", required=True)
    parser.add_argument("--timeout-ms", type=int, default=300)
    parser.add_argument("--repository-kind", choices=("inmemory", "sqlite", "mysql"), default="inmemory")
    parser.add_argument("--initial-status", choices=("IN_STOCK", "BORROWED"), default="")
    parser.add_argument("--mock-host", default="127.0.0.1")
    parser.add_argument("--mock-port", type=int, default=9100)
    parser.add_argument("--mock-mode", choices=MOCK_MODES, default="confirmed")
    parser.add_argument("--mock-delay", type=float, default=0.05)
    parser.add_argument("--mock-heartbeat", type=float, default=5.0)
    parser.add_argument("--serial-ack-timeout", type=float, default=0.1)
    parser.add_argument("--serial-max-retries", type=int, default=3)
    parser.add_argument("--serial-offline-timeout", type=float, default=15.0)
    parser.add_argument("--health-wait", type=float, default=0.2, help="Seconds to wait before first /health probe.")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    return parser


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level), format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")


def infer_initial_status(args: argparse.Namespace) -> str:
    if args.initial_status:
        return args.initial_status
    return "IN_STOCK" if args.action == "borrow" else "BORROWED"


def configure_environment(args: argparse.Namespace) -> None:
    os.environ["BACKEND_REPOSITORY_KIND"] = args.repository_kind
    os.environ["BACKEND_SERIAL_PORT"] = f"socket://{args.mock_host}:{args.mock_port}"
    os.environ["BACKEND_SERIAL_ACK_TIMEOUT"] = str(args.serial_ack_timeout)
    os.environ["BACKEND_SERIAL_MAX_RETRIES"] = str(args.serial_max_retries)
    os.environ["BACKEND_SERIAL_OFFLINE_TIMEOUT"] = str(args.serial_offline_timeout)
    os.environ["BACKEND_MOCK_MCU_HOST"] = args.mock_host
    os.environ["BACKEND_MOCK_MCU_PORT"] = str(args.mock_port)

    if args.repository_kind == "inmemory":
        os.environ["BACKEND_INITIAL_ASSETS_JSON"] = json.dumps(
            {args.asset_id: infer_initial_status(args)},
            ensure_ascii=False,
        )
    else:
        os.environ.pop("BACKEND_INITIAL_ASSETS_JSON", None)


def collect_websocket_messages(websocket) -> list[dict[str, object]]:
    messages: list[dict[str, object]] = []
    for _ in range(5):
        payload = websocket.receive_json()
        messages.append(payload)
        if payload.get("success") is not None:
            break
    return messages


def build_request_payload(args: argparse.Namespace) -> dict[str, object]:
    return {
        "asset_id": args.asset_id,
        "user_id": args.user_id,
        "user_name": args.user_name,
        "timeout_ms": args.timeout_ms,
    }


def main() -> int:
    args = build_arg_parser().parse_args()
    configure_logging(args.log_level)
    configure_environment(args)

    mock_server: MockMCUServer | None = None
    if args.mock_mode != "offline":
        mock_server = MockMCUServer(
            host=args.mock_host,
            port=args.mock_port,
            mode=args.mock_mode,
            confirm_delay=args.mock_delay,
            heartbeat_interval=args.mock_heartbeat,
        )
        mock_server.start()

    try:
        from api_app import build_default_runtime, create_app

        runtime = build_default_runtime()
        app = create_app(runtime)
        endpoint = "/transactions/borrow" if args.action == "borrow" else "/transactions/return"

        with TestClient(app) as client:
            time.sleep(args.health_wait)
            health_before = client.get("/health").json()
            with client.websocket_connect("/ws/status") as websocket:
                response = client.post(endpoint, json=build_request_payload(args))
                websocket_messages = collect_websocket_messages(websocket)
            health_after = client.get("/health").json()
            asset_snapshot = client.get(f"/assets/{args.asset_id}").json()

        final_asset_status = runtime.repository.get_asset_status(args.asset_id)
        payload = {
            "mock_mode": args.mock_mode,
            "repository_kind": args.repository_kind,
            "health_before": health_before,
            "api_result": response.json(),
            "websocket_messages": websocket_messages,
            "health_after": health_after,
            "asset_snapshot_after": asset_snapshot,
            "repository_asset_status_after": None if final_asset_status is None else final_asset_status.value,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0 if response.json().get("success") else 1
    finally:
        if mock_server is not None:
            mock_server.stop()


if __name__ == "__main__":
    sys.exit(main())
