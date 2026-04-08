from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Any

import runtime_paths  # noqa: F401
import serial_manager as serial_runtime
from models import DeviceStatus
from serial_manager import SerialManager


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Preflight-check the real serial runtime before the final demo.")
    parser.add_argument("--serial-port", default=os.getenv("BACKEND_SERIAL_PORT", ""))
    parser.add_argument("--baudrate", type=int, default=int(os.getenv("BACKEND_SERIAL_BAUDRATE", "115200")))
    parser.add_argument("--read-timeout", type=float, default=float(os.getenv("BACKEND_SERIAL_READ_TIMEOUT", "0.2")))
    parser.add_argument(
        "--offline-timeout",
        type=float,
        default=float(os.getenv("BACKEND_SERIAL_OFFLINE_TIMEOUT", "15.0")),
    )
    parser.add_argument("--wait-seconds", type=float, default=3.0, help="How long to wait for heartbeat after opening.")
    parser.add_argument("--list-only", action="store_true", help="Only print serial configuration and detected ports.")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    return parser


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level), format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")


def list_available_ports() -> list[dict[str, str]]:
    if serial_runtime.serial is None:
        return []
    try:
        from serial.tools import list_ports  # type: ignore
    except Exception:
        return []

    ports: list[dict[str, str]] = []
    for port in list_ports.comports():
        ports.append(
            {
                "device": str(getattr(port, "device", "")),
                "description": str(getattr(port, "description", "")),
                "hwid": str(getattr(port, "hwid", "")),
            }
        )
    return ports


def classify_open_error(*, pyserial_available: bool, detail: str) -> str:
    lowered = detail.lower()
    if not pyserial_available:
        return "pyserial_missing"
    if (
        "filenotfounderror" in lowered
        or "no such file or directory" in lowered
        or "cannot find the file specified" in lowered
        or "找不到指定的文件" in detail
    ):
        return "serial_port_not_found"
    if (
        "permissionerror" in lowered
        or "access is denied" in lowered
        or "拒绝访问" in detail
        or "could not exclusively lock port" in lowered
    ):
        return "serial_port_busy"
    return "serial_open_failed"


def build_next_steps(*, serial_port: str, error_kind: str | None, mode: str, wait_seconds: float) -> list[str]:
    if mode != "real_serial":
        return [
            "This script is for real devices only.",
            "Set BACKEND_SERIAL_PORT to a physical port like COM3 or /dev/ttyUSB0.",
            "If you intentionally want the socket demo path, use run_mock_api_flow.py instead.",
        ]
    if error_kind == "pyserial_missing":
        return [
            "Install requirements so pyserial is available in this interpreter.",
            f"Keep BACKEND_SERIAL_PORT={serial_port} and rerun this preflight.",
            "Only start the final API demo after this preflight passes.",
        ]
    if error_kind == "serial_port_not_found":
        return [
            f"Verify that the device is really exposed as {serial_port} in Device Manager or /dev.",
            "If the OS assigned a different COM/tty name, update BACKEND_SERIAL_PORT.",
            "Reconnect the cable or power-cycle the device, then rerun this preflight.",
        ]
    if error_kind == "serial_port_busy":
        return [
            f"Close other serial tools that may already hold {serial_port}.",
            "Reconnect the device if the OS still reports the port as busy.",
            "Rerun this preflight before starting the API.",
        ]
    if error_kind == "waiting_for_heartbeat":
        return [
            f"The port opened, but no heartbeat arrived within {wait_seconds:.1f}s.",
            "Check that the device firmware is powered and sending heartbeat frames.",
            "Retry this preflight after confirming the device-side ready state.",
        ]
    if error_kind == "device_offline":
        return [
            f"The port opened but then went OFFLINE on {serial_port}.",
            "Check cable, power and whether the COM/tty assignment changed.",
            "Retry this preflight before using the real-device API flow.",
        ]
    return [
        f"Use this port in the final demo: BACKEND_SERIAL_PORT={serial_port}.",
        "Then start the API or run run_real_device_flow.py for a full rehearsal.",
        "If the API later degrades, compare /health.serial_details with this preflight output.",
    ]


def main() -> int:
    args = build_arg_parser().parse_args()
    configure_logging(args.log_level)

    serial_port = args.serial_port.strip()
    mode = "mock_socket" if serial_port.startswith("socket://") else "real_serial"
    ports = list_available_ports()
    payload: dict[str, Any] = {
        "mode": mode,
        "configured_port": serial_port or None,
        "baudrate": args.baudrate,
        "read_timeout_seconds": args.read_timeout,
        "offline_timeout_seconds": args.offline_timeout,
        "wait_seconds": args.wait_seconds,
        "pyserial_available": serial_runtime.serial is not None,
        "available_ports": ports,
        "serial_open": False,
        "device_status": DeviceStatus.UNKNOWN.value,
        "ready_for_real_demo": False,
        "startup_error_kind": None,
        "startup_error": None,
        "status_events": [],
        "recommended_api_command": (
            'python "C:\\Users\\lenovo\\office-asset-rental-system\\backend_service\\api_app.py"'
        ),
        "recommended_flow_command": (
            f'python "C:\\Users\\lenovo\\office-asset-rental-system\\backend_service\\run_real_device_flow.py" '
            f'--action borrow --asset-id AS-0924 --user-id U-1001 --user-name Demo --serial-port {serial_port or "<PORT>"} '
            "--repository-kind mysql"
        ),
    }

    if not serial_port:
        payload["startup_error_kind"] = "serial_port_missing"
        payload["startup_error"] = "BACKEND_SERIAL_PORT is empty. Set it to a real serial port like COM3 or /dev/ttyUSB0."
        payload["next_steps"] = build_next_steps(serial_port="<PORT>", error_kind="serial_port_missing", mode="real_serial", wait_seconds=args.wait_seconds)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 2

    if args.list_only or mode != "real_serial":
        error_kind = None if mode == "real_serial" else "not_real_serial_mode"
        if error_kind is not None:
            payload["startup_error_kind"] = error_kind
            payload["startup_error"] = f"{serial_port} is not a physical serial port. Use COMx or /dev/ttyUSBx for real mode."
        payload["next_steps"] = build_next_steps(
            serial_port=serial_port,
            error_kind=error_kind,
            mode=mode,
            wait_seconds=args.wait_seconds,
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0 if mode == "real_serial" else 2

    status_events: list[dict[str, Any]] = []

    def status_handler(status: DeviceStatus) -> None:
        status_events.append({"status": status.value, "at": round(time.time(), 3)})

    serial_manager = SerialManager(
        port=serial_port,
        baudrate=args.baudrate,
        read_timeout=args.read_timeout,
        offline_timeout=args.offline_timeout,
    )
    serial_manager.set_status_handler(status_handler)

    exit_code = 1
    error_kind: str | None = None
    try:
        serial_manager.open()
        payload["serial_open"] = serial_manager.is_open
        time.sleep(args.wait_seconds)
        payload["serial_open"] = serial_manager.is_open
        payload["device_status"] = serial_manager.device_status.value
        if serial_manager.device_status == DeviceStatus.ONLINE:
            payload["ready_for_real_demo"] = True
            exit_code = 0
        elif serial_manager.device_status == DeviceStatus.UNKNOWN:
            error_kind = "waiting_for_heartbeat"
        else:
            error_kind = "device_offline"
    except Exception as exc:
        detail = str(exc).strip() or type(exc).__name__
        error_kind = classify_open_error(pyserial_available=serial_runtime.serial is not None, detail=detail)
        payload["startup_error_kind"] = error_kind
        payload["startup_error"] = detail
    finally:
        payload["status_events"] = status_events
        payload["serial_open"] = payload["serial_open"] or serial_manager.is_open
        if serial_manager.is_open:
            serial_manager.close()

    if error_kind is not None and payload["startup_error_kind"] is None:
        payload["startup_error_kind"] = error_kind
    payload["next_steps"] = build_next_steps(
        serial_port=serial_port,
        error_kind=payload["startup_error_kind"],
        mode=mode,
        wait_seconds=args.wait_seconds,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
