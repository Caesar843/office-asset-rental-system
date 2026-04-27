from __future__ import annotations

import argparse
import json
import logging
import os

import runtime_paths  # noqa: F401
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
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Start the API app together with a demo mock_mcu process.")
    parser.add_argument("--api-host", default=os.getenv("BACKEND_API_HOST", "127.0.0.1"))
    parser.add_argument("--api-port", type=int, default=int(os.getenv("BACKEND_API_PORT", "8000")))
    parser.add_argument("--repository-kind", default=os.getenv("BACKEND_REPOSITORY_KIND", "inmemory"))
    parser.add_argument("--transport-mode", choices=("mock", "real"), default="")
    parser.add_argument("--serial-port", default=os.getenv("BACKEND_SERIAL_PORT", ""))
    parser.add_argument("--baudrate", type=int, default=int(os.getenv("BACKEND_SERIAL_BAUDRATE", "115200")))
    parser.add_argument("--ack-timeout", type=float, default=float(os.getenv("BACKEND_SERIAL_ACK_TIMEOUT", "0.5")))
    parser.add_argument("--max-retries", type=int, default=int(os.getenv("BACKEND_SERIAL_MAX_RETRIES", "3")))
    parser.add_argument(
        "--offline-timeout",
        type=float,
        default=float(os.getenv("BACKEND_SERIAL_OFFLINE_TIMEOUT", "15.0")),
    )
    parser.add_argument("--mock-host", default=os.getenv("BACKEND_MOCK_MCU_HOST", "127.0.0.1"))
    parser.add_argument("--mock-port", type=int, default=int(os.getenv("BACKEND_MOCK_MCU_PORT", "9100")))
    parser.add_argument("--mock-mode", choices=MOCK_MODES, default="confirmed")
    parser.add_argument("--mock-delay", type=float, default=0.2)
    parser.add_argument("--mock-heartbeat", type=float, default=5.0)
    parser.add_argument("--skip-mock", action="store_true", help="Do not auto-start mock_mcu.")
    parser.add_argument(
        "--initial-assets-json",
        default=os.getenv("BACKEND_INITIAL_ASSETS_JSON", ""),
        help="Optional JSON object used only when repository-kind is inmemory.",
    )
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    return parser


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level), format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")


def resolve_transport_mode(args: argparse.Namespace) -> str:
    requested = args.transport_mode.strip().lower()
    if requested:
        return requested
    serial_port = args.serial_port.strip()
    if serial_port and not serial_port.startswith("socket://"):
        return "real"
    return "mock"


def configure_environment(args: argparse.Namespace) -> tuple[str, str]:
    transport_mode = resolve_transport_mode(args)
    serial_port = args.serial_port.strip()
    if transport_mode == "real":
        if not serial_port:
            raise ValueError("real mode requires --serial-port, for example COM3 or /dev/ttyUSB0")
        if serial_port.startswith("socket://"):
            raise ValueError("real mode requires a physical serial port, not socket://")
        args.skip_mock = True
    elif not serial_port:
        serial_port = f"socket://{args.mock_host}:{args.mock_port}"

    os.environ["BACKEND_API_HOST"] = args.api_host
    os.environ["BACKEND_API_PORT"] = str(args.api_port)
    os.environ["BACKEND_REPOSITORY_KIND"] = args.repository_kind
    os.environ["BACKEND_SERIAL_PORT"] = serial_port
    os.environ["BACKEND_SERIAL_BAUDRATE"] = str(args.baudrate)
    os.environ["BACKEND_SERIAL_ACK_TIMEOUT"] = str(args.ack_timeout)
    os.environ["BACKEND_SERIAL_MAX_RETRIES"] = str(args.max_retries)
    os.environ["BACKEND_SERIAL_OFFLINE_TIMEOUT"] = str(args.offline_timeout)
    os.environ["BACKEND_MOCK_MCU_HOST"] = args.mock_host
    os.environ["BACKEND_MOCK_MCU_PORT"] = str(args.mock_port)
    if args.initial_assets_json:
        os.environ["BACKEND_INITIAL_ASSETS_JSON"] = args.initial_assets_json
    else:
        os.environ.pop("BACKEND_INITIAL_ASSETS_JSON", None)
    return serial_port, transport_mode


def print_summary(args: argparse.Namespace, *, serial_port: str, transport_mode: str) -> None:
    base_url = f"http://{args.api_host}:{args.api_port}"
    summary = {
        "api_base_url": base_url,
        "health_url": f"{base_url}/health",
        "websocket_url": f"ws://{args.api_host}:{args.api_port}/ws/status",
        "repository_kind": args.repository_kind,
        "transport_mode": transport_mode,
        "serial_port": serial_port,
        "serial_target_kind": "real_serial" if transport_mode == "real" else "mock_socket",
        "mock_started": not args.skip_mock,
        "mock_mode": None if args.skip_mock else args.mock_mode,
        "health_first_checks": [
            "repository_mode and repository_status",
            "serial_details.mode / configured_port / diagnosis",
            "device_status and serial_open",
        ],
        "switch_to_real_hint": "Restart with --transport-mode real --serial-port COM3 --repository-kind mysql",
        "switch_to_mock_hint": (
            f"Restart with --transport-mode mock --mock-port {args.mock_port} --repository-kind mysql"
        ),
        "borrow_example": {
            "method": "POST",
            "url": f"{base_url}/transactions/borrow",
            "json": {"asset_id": "AS-0924", "user_id": "U-1001", "user_name": "Demo Borrow", "timeout_ms": 3000},
        },
        "return_example": {
            "method": "POST",
            "url": f"{base_url}/transactions/return",
            "json": {"asset_id": "AS-0925", "user_id": "U-1002", "user_name": "Demo Return", "timeout_ms": 3000},
        },
        "inbound_example": {
            "method": "POST",
            "url": f"{base_url}/transactions/inbound",
            "json": {
                "asset_id": "AS-0926",
                "user_id": "U-ADMIN",
                "user_name": "Demo Admin",
                "asset_name": "Demo Inbound Asset",
                "category_id": 1,
                "location": "Inbound Shelf",
                "timeout_ms": 3000,
            },
        },
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    configure_logging(args.log_level)
    try:
        serial_port, transport_mode = configure_environment(args)
    except ValueError as exc:
        parser.error(str(exc))

    mock_server: MockMCUServer | None = None
    if not args.skip_mock:
        mock_server = MockMCUServer(
            host=args.mock_host,
            port=args.mock_port,
            mode=args.mock_mode,
            confirm_delay=args.mock_delay,
            heartbeat_interval=args.mock_heartbeat,
        )
        mock_server.start()

    print_summary(args, serial_port=serial_port, transport_mode=transport_mode)

    try:
        import uvicorn

        from api_app import create_app

        app = create_app()
        uvicorn.run(app, host=args.api_host, port=args.api_port, reload=False, log_level=args.log_level.lower())
    finally:
        if mock_server is not None:
            mock_server.stop()


if __name__ == "__main__":
    main()
