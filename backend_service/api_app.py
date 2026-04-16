from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from enum import Enum
import csv
import io
import json
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any, Callable
from urllib.parse import urlparse

import runtime_paths  # noqa: F401
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response
import serial_manager as serial_runtime
from api_schemas import (
    AssetSnapshotResponse,
    BorrowRequestBody,
    BorrowRequestActionResponse,
    BorrowRequestCreateBody,
    BorrowRequestReviewBody,
    BorrowRequestStartBorrowBody,
    BusinessResultResponse,
    HealthResponse,
    InboundRequestBody,
    BorrowRequestRecordResponse,
    ReturnRequestBody,
    ScanResultRequestBody,
    ScanResultResponse,
    StatusMessageResponse,
)
from db_repository import MySQLTransactionRepository, RepositoryProbeResult, SQLiteTransactionRepository
from models import (
    ActionType,
    AssetStatus,
    BorrowApprovalCommand,
    BorrowCommand,
    BorrowRequestCreateCommand,
    BorrowRequestStatus,
    ConfirmResult,
    DeviceStatus,
    InboundCommand,
    ReturnCommand,
)
from repository import InMemoryTransactionRepository, TransactionRepository
from serial_manager import SerialManager
from service import AssetConfirmService
from status_hub import StatusHub

LOGGER = logging.getLogger(__name__)

DEFAULT_DEMO_ASSETS = {
    "AS-0924": AssetStatus.IN_STOCK,
    "AS-0925": AssetStatus.BORROWED,
}

DEFAULT_MOCK_HOST = "127.0.0.1"
DEFAULT_MOCK_PORT = 9100
SERIAL_ENV_VARS = (
    "BACKEND_SERIAL_PORT",
    "BACKEND_SERIAL_BAUDRATE",
    "BACKEND_SERIAL_READ_TIMEOUT",
    "BACKEND_SERIAL_ACK_TIMEOUT",
    "BACKEND_SERIAL_MAX_RETRIES",
    "BACKEND_SERIAL_OFFLINE_TIMEOUT",
)
REAL_SERIAL_PORT_EXAMPLES = ("COM3", "/dev/ttyUSB0")
DASHBOARD_TIME_RANGES = ("all", "today", "7d", "30d")


@dataclass(slots=True)
class SerialSelection:
    port: str
    baudrate: int
    read_timeout: float
    ack_timeout: float
    max_retries: int
    offline_timeout: float
    transport: str
    pyserial_available: bool
    mock_host: str | None = None
    mock_port: int | None = None

    @property
    def mode(self) -> str:
        return "mock_socket" if self.transport == "socket" else "real_serial"

    @property
    def mode_label(self) -> str:
        return "Mock MCU Socket" if self.transport == "socket" else "Real Serial Device"

    @property
    def is_mock_mode(self) -> bool:
        return self.transport == "socket"

    @property
    def is_real_serial_mode(self) -> bool:
        return self.transport != "socket"

    @property
    def target_label(self) -> str:
        if self.is_mock_mode and self.mock_host is not None and self.mock_port is not None:
            return f"{self.mock_host}:{self.mock_port}"
        return self.port

    @property
    def mock_mcu_script_path(self) -> str:
        return str((Path(__file__).resolve().parent.parent / "serial_comm" / "mock_mcu.py").resolve())

    @property
    def mock_flow_script_path(self) -> str:
        return str((Path(__file__).resolve().parent / "run_mock_api_flow.py").resolve())

    @property
    def real_preflight_script_path(self) -> str:
        return str((Path(__file__).resolve().parent / "check_real_serial_runtime.py").resolve())

    @property
    def real_flow_script_path(self) -> str:
        return str((Path(__file__).resolve().parent / "run_real_device_flow.py").resolve())

    @property
    def mock_mcu_start_command(self) -> str | None:
        if not self.is_mock_mode or self.mock_host is None or self.mock_port is None:
            return None
        return (
            f'python "{self.mock_mcu_script_path}" --host {self.mock_host} '
            f"--port {self.mock_port} --mode confirmed"
        )

    @property
    def mock_flow_command(self) -> str | None:
        if not self.is_mock_mode:
            return None
        return (
            f'python "{self.mock_flow_script_path}" --action borrow --asset-id AS-0924 '
            f'--user-id U-1001 --user-name Demo --repository-kind mysql --mock-port {self.mock_port or DEFAULT_MOCK_PORT}'
        )

    @property
    def real_preflight_command(self) -> str | None:
        if not self.is_real_serial_mode:
            return None
        return f'python "{self.real_preflight_script_path}" --serial-port {self.port}'

    @property
    def real_flow_command(self) -> str | None:
        if not self.is_real_serial_mode:
            return None
        return (
            f'python "{self.real_flow_script_path}" --action borrow --asset-id AS-0924 '
            f'--user-id U-1001 --user-name Demo --serial-port {self.port} --repository-kind mysql'
        )

    @property
    def startup_hint(self) -> str:
        if self.is_mock_mode:
            return self.mock_mcu_start_command or "Start mock_mcu before opening the API runtime."
        examples = " or ".join(REAL_SERIAL_PORT_EXAMPLES)
        preflight = self.real_preflight_command or "python check_real_serial_runtime.py --serial-port <PORT>"
        return (
            f"Set BACKEND_SERIAL_PORT to a real device path such as {examples}, "
            f"make sure it does not start with socket://, then run the preflight: {preflight}"
        )

    @property
    def switch_to_mock_hint(self) -> str:
        return (
            f"Set BACKEND_SERIAL_PORT=socket://{DEFAULT_MOCK_HOST}:{DEFAULT_MOCK_PORT} "
            f"and start mock_mcu: python \"{self.mock_mcu_script_path}\" --host {DEFAULT_MOCK_HOST} "
            f"--port {DEFAULT_MOCK_PORT} --mode confirmed"
        )

    @property
    def switch_to_real_hint(self) -> str:
        examples = " or ".join(REAL_SERIAL_PORT_EXAMPLES)
        return (
            f"Set BACKEND_SERIAL_PORT to a physical serial port such as {examples} "
            "and ensure it does not start with socket://."
        )

    def _real_diagnosis_steps(self, diagnosis: str) -> list[str]:
        preflight = self.real_preflight_command or "python check_real_serial_runtime.py --serial-port <PORT>"
        if diagnosis == "pyserial_missing":
            return [
                "Install dependencies so pyserial is available in the current interpreter.",
                f"Keep BACKEND_SERIAL_PORT pointed at the real device port, then rerun: {preflight}",
                "After preflight is healthy, start api_app.py or the real-device rehearsal script.",
            ]
        if diagnosis == "serial_port_not_found":
            return [
                f"Verify the physical device is really exposed as {self.port} in Device Manager or /dev.",
                "Correct BACKEND_SERIAL_PORT if the OS assigned a different COM/tty name.",
                f"Rerun the preflight: {preflight}",
            ]
        if diagnosis in {"serial_port_busy", "permission_denied"}:
            return [
                f"Close other serial tools that may already hold {self.port}.",
                "Replug or repower the device if the OS still reports the port as busy.",
                f"Rerun the preflight: {preflight}",
            ]
        if diagnosis == "waiting_for_heartbeat":
            return [
                f"Keep the API attached to {self.port} and confirm the device is powered.",
                "Wait for the MCU heartbeat or perform the device-side ready action.",
                "Refresh /health until device_status changes from UNKNOWN to ONLINE.",
            ]
        if diagnosis == "device_offline":
            return [
                f"Check cable, power and that the device still enumerates on {self.port}.",
                "If the port moved, update BACKEND_SERIAL_PORT and restart the API.",
                f"Rerun the preflight: {preflight}",
            ]
        return [
            f"Verify BACKEND_SERIAL_PORT={self.port} and that the device is powered.",
            f"Rerun the preflight: {preflight}",
            "If the port opens but health stays degraded, inspect serial logs for heartbeat loss or ACK timeout.",
        ]

    def _mock_diagnosis_steps(self, diagnosis: str) -> list[str]:
        hint = self.mock_mcu_start_command or "python ../serial_comm/mock_mcu.py --host 127.0.0.1 --port 9100 --mode confirmed"
        if diagnosis in {"connect_timeout", "connection_refused"}:
            return [
                f"Start mock_mcu first: {hint}",
                f"Keep BACKEND_SERIAL_PORT={self.port} aligned with the mock host and port.",
                "Refresh /health until device_status becomes ONLINE.",
            ]
        if diagnosis == "waiting_for_heartbeat":
            return [
                "Confirm mock_mcu is already running and has accepted the socket connection.",
                "Refresh /health after the first heartbeat arrives.",
                "If it stays UNKNOWN, restart both mock_mcu and the API.",
            ]
        if diagnosis == "device_offline":
            return [
                "mock_mcu was reachable but the API stopped receiving heartbeats.",
                "Restart mock_mcu or switch the API back to a healthy socket target.",
                "Use run_mock_api_flow.py to confirm the fallback path still works end-to-end.",
            ]
        return [
            f"Ensure mock_mcu is reachable on {self.target_label}.",
            f"Recheck the startup command: {hint}",
            "If /health is still degraded, inspect startup_error and socket logs.",
        ]

    def next_steps(self, diagnosis: str) -> list[str]:
        if self.is_mock_mode:
            return self._mock_diagnosis_steps(diagnosis)
        return self._real_diagnosis_steps(diagnosis)

    def classify_open_error(self, exc: Exception) -> tuple[str, str]:
        detail = str(exc).strip() or type(exc).__name__
        lowered = detail.lower()

        if not self.pyserial_available and self.is_real_serial_mode:
            return "pyserial_missing", detail

        if self.is_real_serial_mode:
            if (
                "filenotfounderror" in lowered
                or "no such file or directory" in lowered
                or "cannot find the file specified" in lowered
                or "找不到指定的文件" in detail
            ):
                return "serial_port_not_found", detail
            if (
                "permissionerror" in lowered
                or "access is denied" in lowered
                or "拒绝访问" in detail
                or "could not exclusively lock port" in lowered
            ):
                return "serial_port_busy", detail
            return "serial_open_failed", detail

        if isinstance(exc, TimeoutError) or "timed out" in lowered:
            return "connect_timeout", detail
        if isinstance(exc, ConnectionRefusedError) or "actively refused" in lowered or "10061" in lowered:
            return "connection_refused", detail
        if "nodename nor servname provided" in lowered or "name or service not known" in lowered:
            return "name_resolution_failed", detail
        return "open_failed", detail

    def format_open_error(self, exc: Exception) -> tuple[str, str]:
        error_kind, detail = self.classify_open_error(exc)
        if self.is_real_serial_mode:
            preflight = self.real_preflight_command or "python check_real_serial_runtime.py --serial-port <PORT>"
            if error_kind == "pyserial_missing":
                return error_kind, (
                    "pyserial is required for real serial mode but is not available. "
                    f"Install dependencies, then rerun: {preflight}"
                )
            if error_kind == "serial_port_not_found":
                return error_kind, (
                    f"serial port {self.port} was not found. Check BACKEND_SERIAL_PORT, cable and OS port assignment, "
                    f"then rerun: {preflight}"
                )
            if error_kind in {"serial_port_busy", "permission_denied"}:
                return error_kind, (
                    f"serial port {self.port} is busy or access was denied. Close other serial tools and rerun: {preflight}"
                )
            return error_kind, (
                f"failed to open serial port {self.port}: {detail}. "
                f"Check cable, power, driver and rerun: {preflight}"
            )

        endpoint = f"{self.mock_host}:{self.mock_port}"
        hint = self.mock_mcu_start_command
        if error_kind == "connection_refused":
            return error_kind, f"mock_mcu is not listening on {endpoint}. Start it first: {hint}"
        if error_kind == "connect_timeout":
            return error_kind, f"mock_mcu connect timed out on {endpoint}. Start it first: {hint}"
        if error_kind == "name_resolution_failed":
            return error_kind, f"socket target {endpoint} could not be resolved: {detail}"
        return error_kind, f"failed to open socket transport {endpoint}: {detail}. Start mock_mcu first: {hint}"

    def to_health_dict(
        self,
        *,
        serial_open: bool,
        device_status: DeviceStatus,
        startup_error: str | None,
        startup_error_kind: str | None,
    ) -> dict[str, Any]:
        diagnosis = "connected"
        if startup_error_kind is not None:
            diagnosis = startup_error_kind
        elif not serial_open and device_status == DeviceStatus.OFFLINE:
            diagnosis = "serial_closed"
        elif device_status == DeviceStatus.UNKNOWN:
            diagnosis = "waiting_for_heartbeat"
        elif device_status == DeviceStatus.OFFLINE:
            diagnosis = "device_offline"

        details: dict[str, Any] = {
            "mode": self.mode,
            "mode_label": self.mode_label,
            "transport": self.transport,
            "target": self.target_label,
            "configured_port": self.port,
            "configured_from_env": list(SERIAL_ENV_VARS),
            "is_mock_mode": self.is_mock_mode,
            "is_real_serial_mode": self.is_real_serial_mode,
            "baudrate": self.baudrate,
            "read_timeout_seconds": self.read_timeout,
            "ack_timeout_seconds": self.ack_timeout,
            "max_retries": self.max_retries,
            "offline_timeout_seconds": self.offline_timeout,
            "pyserial_available": self.pyserial_available,
            "diagnosis": diagnosis,
            "startup_error_kind": startup_error_kind,
            "startup_hint": self.startup_hint,
            "preflight_command": self.real_preflight_command if self.is_real_serial_mode else None,
            "demo_flow_command": self.real_flow_command if self.is_real_serial_mode else self.mock_flow_command,
            "switch_to_mock_hint": self.switch_to_mock_hint,
            "switch_to_real_hint": self.switch_to_real_hint,
            "next_steps": self.next_steps(diagnosis),
        }
        if startup_error:
            details["startup_error"] = startup_error
        if self.is_mock_mode:
            details["socket_host"] = self.mock_host
            details["socket_port"] = self.mock_port
            details["mock_mcu_script"] = self.mock_mcu_script_path
        return details


@dataclass(slots=True)
class ApiRuntime:
    serial_manager: SerialManager
    repository: TransactionRepository
    service: AssetConfirmService
    status_hub: StatusHub
    serial_config: SerialSelection | None = None
    requested_repository_mode: str = "inmemory"
    repository_mode: str = "inmemory"
    repository_fallback: bool = False
    repository_ready: bool = True
    repository_status: str = "ok"
    repository_details: dict[str, Any] = field(default_factory=dict)
    startup_error: str | None = None
    startup_error_kind: str | None = None
    exception_records: list[dict[str, Any]] = field(default_factory=list)
    exception_records_lock: Any = field(default_factory=Lock)

    def add_startup_error(self, message: str) -> None:
        if self.startup_error:
            if message not in self.startup_error:
                self.startup_error = f"{self.startup_error}; {message}"
            return
        self.startup_error = message

    def open(self) -> None:
        try:
            self.service.open()
        except Exception as exc:
            formatted_error = str(exc)
            error_kind = "open_failed"
            if self.serial_config is not None:
                error_kind, formatted_error = self.serial_config.format_open_error(exc)
            self.startup_error_kind = error_kind
            self.add_startup_error(formatted_error)
            LOGGER.exception("api runtime failed to open serial manager: %s", exc)
            self.service.update_device_status(DeviceStatus.OFFLINE)

    def close(self) -> None:
        try:
            self.service.close()
        except Exception:
            LOGGER.exception("api runtime close failed")

    @property
    def serial_open(self) -> bool:
        return bool(getattr(self.serial_manager, "is_open", False))

    @property
    def health_status(self) -> str:
        if (
            self.startup_error
            or not self.repository_ready
            or self.repository_fallback
            or self.service.device_status == DeviceStatus.OFFLINE
        ):
            return "degraded"
        if self.repository_status == "warning" or self.service.device_status == DeviceStatus.UNKNOWN:
            return "warning"
        return "ok"

    @property
    def serial_health_details(self) -> dict[str, Any]:
        if self.serial_config is None:
            return {
                "transport": "unknown",
                "configured_port": getattr(self.serial_manager, "port", None),
                "baudrate": getattr(self.serial_manager, "baudrate", None),
                "diagnosis": "connected" if self.serial_open else "serial_closed",
                "startup_error": self.startup_error,
                "startup_error_kind": self.startup_error_kind,
            }
        return self.serial_config.to_health_dict(
            serial_open=self.serial_open,
            device_status=self.service.device_status,
            startup_error=self.startup_error,
            startup_error_kind=self.startup_error_kind,
        )


@dataclass(slots=True)
class RepositorySelection:
    repository: TransactionRepository
    requested_mode: str
    active_mode: str
    fallback_used: bool
    ready: bool
    status: str
    details: dict[str, Any]
    startup_error: str | None = None


def _coerce_asset_status(raw_status: str) -> AssetStatus:
    normalized = raw_status.strip()
    for status in AssetStatus:
        if normalized == status.name or normalized == status.value:
            return status
    raise ValueError(f"unsupported asset status: {raw_status}")


def _load_initial_assets_from_env() -> dict[str, AssetStatus]:
    raw = os.getenv("BACKEND_INITIAL_ASSETS_JSON")
    if not raw:
        return dict(DEFAULT_DEMO_ASSETS)

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        LOGGER.warning("invalid BACKEND_INITIAL_ASSETS_JSON, using demo assets instead: %s", exc)
        return dict(DEFAULT_DEMO_ASSETS)

    if not isinstance(payload, dict):
        LOGGER.warning("BACKEND_INITIAL_ASSETS_JSON must be a JSON object, using demo assets instead")
        return dict(DEFAULT_DEMO_ASSETS)

    assets: dict[str, AssetStatus] = {}
    for asset_id, raw_status in payload.items():
        if not isinstance(asset_id, str) or not isinstance(raw_status, str):
            LOGGER.warning("skip invalid initial asset entry: %s=%s", asset_id, raw_status)
            continue
        assets[asset_id] = _coerce_asset_status(raw_status)
    return assets or dict(DEFAULT_DEMO_ASSETS)


def build_status_callback(status_hub: StatusHub) -> Callable[[dict[str, object]], None]:
    def callback(payload: dict[str, object]) -> None:
        message = StatusMessageResponse.model_validate(payload)
        status_hub.publish(message)

    return callback


def _inmemory_probe_details(*, requested_mode: str, startup_error: str | None = None) -> dict[str, Any]:
    initial_assets = _load_initial_assets_from_env()
    details: dict[str, Any] = {
        "backend": "inmemory",
        "ready": True,
        "status": "ok",
        "warnings": [],
        "errors": [],
        "details": {"demo_asset_count": len(initial_assets)},
    }
    if requested_mode != "inmemory":
        details["status"] = "warning"
        details["warnings"] = [startup_error or f"repository request fell back from {requested_mode} to in-memory"]
        details["details"] = {
            "demo_asset_count": len(initial_assets),
            "fallback_target": "inmemory",
            "requested_mode": requested_mode,
        }
    return details


def _selection_from_probe(
    *,
    repository: TransactionRepository,
    requested_mode: str,
    active_mode: str,
    fallback_used: bool,
    probe: RepositoryProbeResult,
    startup_error: str | None = None,
) -> RepositorySelection:
    return RepositorySelection(
        repository=repository,
        requested_mode=requested_mode,
        active_mode=active_mode,
        fallback_used=fallback_used,
        ready=probe.ready,
        status=probe.status,
        details=probe.to_dict(),
        startup_error=startup_error,
    )


def _fallback_to_inmemory(*, requested_mode: str, startup_error: str) -> RepositorySelection:
    repository = InMemoryTransactionRepository(initial_assets=_load_initial_assets_from_env())
    details = _inmemory_probe_details(requested_mode=requested_mode, startup_error=startup_error)
    return RepositorySelection(
        repository=repository,
        requested_mode=requested_mode,
        active_mode="inmemory",
        fallback_used=True,
        ready=True,
        status="fallback",
        details=details,
        startup_error=startup_error,
    )


def _build_repository_from_env() -> RepositorySelection:
    requested_mode = os.getenv("BACKEND_REPOSITORY_KIND", "inmemory").strip().lower() or "inmemory"

    if requested_mode == "inmemory":
        repository = InMemoryTransactionRepository(initial_assets=_load_initial_assets_from_env())
        return RepositorySelection(
            repository=repository,
            requested_mode="inmemory",
            active_mode="inmemory",
            fallback_used=False,
            ready=True,
            status="ok",
            details=_inmemory_probe_details(requested_mode="inmemory"),
            startup_error=None,
        )

    if requested_mode == "sqlite":
        sqlite_path = os.getenv(
            "BACKEND_SQLITE_PATH",
            str(Path(__file__).resolve().with_name("backend_service.sqlite3")),
        )
        repository = SQLiteTransactionRepository(sqlite_path)
        probe = repository.probe()
        LOGGER.info("api runtime using sqlite repository: %s", sqlite_path)
        return _selection_from_probe(
            repository=repository,
            requested_mode="sqlite",
            active_mode="sqlite",
            fallback_used=False,
            probe=probe,
        )

    if requested_mode == "mysql":
        try:
            repository = MySQLTransactionRepository.from_env()
            probe = repository.probe()
        except Exception as exc:
            warning = f"mysql repository unavailable, fallback to in-memory: {exc}"
            LOGGER.exception(warning)
            return _fallback_to_inmemory(requested_mode="mysql", startup_error=warning)

        if not probe.ready:
            warning = f"mysql repository probe failed, fallback to in-memory: {probe.errors}"
            LOGGER.warning(warning)
            return _fallback_to_inmemory(requested_mode="mysql", startup_error=warning)

        if probe.status == "warning":
            LOGGER.warning("api runtime using mysql repository with warnings: %s", probe.warnings)
        else:
            LOGGER.info("api runtime using mysql repository")
        return _selection_from_probe(
            repository=repository,
            requested_mode="mysql",
            active_mode="mysql",
            fallback_used=False,
            probe=probe,
        )

    warning = f"unsupported repository kind '{requested_mode}', fallback to in-memory"
    LOGGER.warning(warning)
    return _fallback_to_inmemory(requested_mode=requested_mode, startup_error=warning)


def _build_serial_selection_from_env() -> SerialSelection:
    mock_host = os.getenv("BACKEND_MOCK_MCU_HOST", DEFAULT_MOCK_HOST).strip() or DEFAULT_MOCK_HOST
    mock_port = int(os.getenv("BACKEND_MOCK_MCU_PORT", str(DEFAULT_MOCK_PORT)))
    serial_port = os.getenv("BACKEND_SERIAL_PORT", "").strip() or f"socket://{mock_host}:{mock_port}"
    baudrate = int(os.getenv("BACKEND_SERIAL_BAUDRATE", "115200"))
    read_timeout = float(os.getenv("BACKEND_SERIAL_READ_TIMEOUT", "0.2"))
    ack_timeout = float(os.getenv("BACKEND_SERIAL_ACK_TIMEOUT", "0.5"))
    max_retries = int(os.getenv("BACKEND_SERIAL_MAX_RETRIES", "3"))
    offline_timeout = float(os.getenv("BACKEND_SERIAL_OFFLINE_TIMEOUT", "15.0"))

    parsed = urlparse(serial_port)
    transport = "socket" if parsed.scheme == "socket" else "serial"
    socket_host = (parsed.hostname or mock_host) if transport == "socket" else None
    socket_port = (parsed.port or mock_port) if transport == "socket" else None

    selection = SerialSelection(
        port=serial_port,
        baudrate=baudrate,
        read_timeout=read_timeout,
        ack_timeout=ack_timeout,
        max_retries=max_retries,
        offline_timeout=offline_timeout,
        transport=transport,
        pyserial_available=serial_runtime.serial is not None,
        mock_host=socket_host,
        mock_port=socket_port,
    )
    LOGGER.info(
        "api runtime serial target prepared: mode=%s transport=%s target=%s port=%s baudrate=%s ack_timeout=%.2fs max_retries=%s",
        selection.mode,
        selection.transport,
        selection.target_label,
        selection.port,
        selection.baudrate,
        selection.ack_timeout,
        selection.max_retries,
    )
    if selection.is_mock_mode and selection.mock_mcu_start_command is not None:
        LOGGER.info("api runtime mock_mcu hint: %s", selection.mock_mcu_start_command)
    if selection.is_real_serial_mode:
        LOGGER.info("api runtime real-serial hint: %s", selection.startup_hint)
    return selection


def build_default_runtime() -> ApiRuntime:
    serial_selection = _build_serial_selection_from_env()
    status_hub = StatusHub()
    serial_manager = SerialManager(
        port=serial_selection.port,
        baudrate=serial_selection.baudrate,
        read_timeout=serial_selection.read_timeout,
        ack_timeout=serial_selection.ack_timeout,
        max_retries=serial_selection.max_retries,
        offline_timeout=serial_selection.offline_timeout,
    )
    selection = _build_repository_from_env()
    service = AssetConfirmService(
        serial_manager=serial_manager,
        repository=selection.repository,
        status_callback=build_status_callback(status_hub),
    )
    return ApiRuntime(
        serial_manager=serial_manager,
        repository=selection.repository,
        service=service,
        status_hub=status_hub,
        serial_config=serial_selection,
        requested_repository_mode=selection.requested_mode,
        repository_mode=selection.active_mode,
        repository_fallback=selection.fallback_used,
        repository_ready=selection.ready,
        repository_status=selection.status,
        repository_details=selection.details,
        startup_error=selection.startup_error,
    )


def _available_actions(asset_status: AssetStatus | None) -> list[str]:
    if asset_status == AssetStatus.IN_STOCK:
        return ["BORROW"]
    if asset_status == AssetStatus.BORROWED:
        return ["RETURN"]
    return []


def _build_asset_snapshot(
    asset_id: str,
    asset_status: AssetStatus | None,
    device_status: DeviceStatus,
) -> AssetSnapshotResponse:
    return AssetSnapshotResponse(
        asset_id=asset_id,
        exists=asset_status is not None,
        asset_status=None if asset_status is None else asset_status.value,
        available_actions=_available_actions(asset_status),
        device_status=device_status.value,
    )


def _list_asset_status_map(repository: TransactionRepository) -> dict[str, str]:
    if isinstance(repository, InMemoryTransactionRepository):
        return {asset_id: asset_status.value for asset_id, asset_status in repository.assets.items()}

    if isinstance(repository, (SQLiteTransactionRepository, MySQLTransactionRepository)):
        connection = repository._connect()
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT id, qr_code, status FROM assets ORDER BY id")
            rows = cursor.fetchall()
        finally:
            connection.close()

        assets: dict[str, str] = {}
        for row in rows:
            asset_db_id, qr_code, status_code = tuple(row)
            asset_id = str(qr_code or asset_db_id)
            try:
                asset_status = repository._db_status_to_asset_status(status_code)
            except ValueError:
                LOGGER.warning("skip asset with unsupported status: asset_id=%s status=%s", asset_id, status_code)
                continue
            assets[asset_id] = asset_status.value
        return assets

    raise RuntimeError(f"repository does not support asset listing: {type(repository).__name__}")


def _normalize_dashboard_time_range(raw_time_range: str | None) -> str:
    normalized = (raw_time_range or "all").strip().lower()
    return normalized if normalized in DASHBOARD_TIME_RANGES else "all"


def _parse_operation_time(raw_operation_time: Any) -> datetime | None:
    if raw_operation_time is None:
        return None
    if isinstance(raw_operation_time, datetime):
        return raw_operation_time
    if isinstance(raw_operation_time, str):
        try:
            return datetime.fromisoformat(raw_operation_time)
        except ValueError:
            LOGGER.warning("skip operation with invalid op_time: %s", raw_operation_time)
            return None
    LOGGER.warning("skip operation with unsupported op_time type: %s", type(raw_operation_time).__name__)
    return None


def _is_operation_in_time_range(operation_time: datetime | None, time_range: str) -> bool:
    if time_range == "all" or operation_time is None:
        return True

    reference_now = datetime.now(operation_time.tzinfo) if operation_time.tzinfo is not None else datetime.now()
    if time_range == "today":
        return operation_time.date() == reference_now.date()

    cutoff_days = 7 if time_range == "7d" else 30
    return operation_time >= reference_now - timedelta(days=cutoff_days)


def _list_dashboard_operations(repository: TransactionRepository) -> list[dict[str, Any]]:
    if isinstance(repository, InMemoryTransactionRepository):
        return [
            {
                "asset_id": record.asset_id,
                "op_type": record.action_type.value,
                "op_time": getattr(record, "op_time", None),
            }
            for record in repository.records
        ]

    if isinstance(repository, (SQLiteTransactionRepository, MySQLTransactionRepository)):
        connection = repository._connect()
        try:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT assets.id, assets.qr_code, operation_records.op_type, operation_records.op_time "
                "FROM operation_records "
                "JOIN assets ON operation_records.asset_id = assets.id "
                "ORDER BY operation_records.op_time DESC, operation_records.op_id DESC"
            )
            rows = cursor.fetchall()
        finally:
            connection.close()

        return [
            {
                "asset_id": str(qr_code or asset_db_id),
                "op_type": str(op_type),
                "op_time": op_time,
            }
            for asset_db_id, qr_code, op_type, op_time in (tuple(row) for row in rows)
        ]

    raise RuntimeError(f"repository does not support dashboard aggregation: {type(repository).__name__}")


def _build_dashboard_payload(
    repository: TransactionRepository,
    *,
    time_range: str,
) -> dict[str, Any]:
    asset_status_map = _list_asset_status_map(repository)
    summary = {
        "in_stock": 0,
        "borrowed": 0,
        "maintenance": 0,
        "scrapped": 0,
    }
    status_summary_map = {
        AssetStatus.IN_STOCK.value: "in_stock",
        AssetStatus.BORROWED.value: "borrowed",
        AssetStatus.MAINTENANCE.value: "maintenance",
        AssetStatus.SCRAPPED.value: "scrapped",
    }
    for asset_status in asset_status_map.values():
        summary_key = status_summary_map.get(asset_status)
        if summary_key is not None:
            summary[summary_key] += 1

    filtered_operations: list[dict[str, Any]] = []
    for operation in _list_dashboard_operations(repository):
        operation_time = _parse_operation_time(operation.get("op_time"))
        if _is_operation_in_time_range(operation_time, time_range):
            filtered_operations.append(operation)

    borrow_top_counter: Counter[str] = Counter()
    borrow_count = 0
    return_count = 0
    for operation in filtered_operations:
        op_type = operation.get("op_type")
        if op_type == ActionType.BORROW.value:
            borrow_count += 1
            borrow_top_counter[str(operation["asset_id"])] += 1
        elif op_type == ActionType.RETURN.value:
            return_count += 1

    borrow_top_assets = [
        {"asset_id": asset_id, "count": count}
        for asset_id, count in sorted(borrow_top_counter.items(), key=lambda item: (-item[1], item[0]))[:5]
    ]

    return {
        "filters": {
            "time_range": time_range,
            "category": None,
        },
        "summary": summary,
        "operation_stats": {
            "borrow_count": borrow_count,
            "return_count": return_count,
        },
        "borrow_top_assets": borrow_top_assets,
        "available_filters": {
            "time_ranges": list(DASHBOARD_TIME_RANGES),
            "categories": [],
        },
    }


def _stringify_export_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    return str(value)


def _csv_download_response(
    *,
    rows: list[dict[str, Any]],
    fieldnames: list[str],
    filename: str,
) -> Response:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({field: _stringify_export_value(row.get(field)) for field in fieldnames})

    return Response(
        content=buffer.getvalue().encode("utf-8-sig"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _json_download_response(*, payload: dict[str, Any], filename: str) -> Response:
    return Response(
        content=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _db_repository_table_rows(
    repository: SQLiteTransactionRepository | MySQLTransactionRepository,
    table_name: str,
    *,
    order_by: str | None = None,
) -> list[dict[str, Any]]:
    connection = repository._connect()
    try:
        cursor = connection.cursor()
        sql = f"SELECT * FROM {table_name}"
        if order_by:
            sql = f"{sql} ORDER BY {order_by}"
        cursor.execute(sql)
        columns = [str(column[0]) for column in (cursor.description or ())]
        return [dict(zip(columns, tuple(row))) for row in cursor.fetchall()]
    finally:
        connection.close()


def _maybe_db_repository_table_rows(
    repository: SQLiteTransactionRepository | MySQLTransactionRepository,
    table_name: str,
    *,
    order_by: str | None = None,
) -> list[dict[str, Any]]:
    try:
        return _db_repository_table_rows(repository, table_name, order_by=order_by)
    except Exception as exc:
        LOGGER.warning("optional export table unavailable: table=%s error=%s", table_name, exc)
        return []


def _list_asset_export_rows(repository: TransactionRepository) -> list[dict[str, Any]]:
    if isinstance(repository, InMemoryTransactionRepository):
        category_names = getattr(repository, "categories", {})
        return [
            {
                "asset_id": asset_id,
                "asset_status": asset_status.value,
                "asset_name": getattr(repository, "asset_details", {}).get(asset_id, {}).get("asset_name", ""),
                "category": category_names.get(
                    getattr(repository, "asset_details", {}).get(asset_id, {}).get("category_id"),
                    "",
                ),
                "location": getattr(repository, "asset_details", {}).get(asset_id, {}).get("location", ""),
            }
            for asset_id, asset_status in sorted(repository.assets.items())
        ]

    if isinstance(repository, (SQLiteTransactionRepository, MySQLTransactionRepository)):
        asset_rows = _db_repository_table_rows(repository, "assets", order_by="id")
        category_rows = _maybe_db_repository_table_rows(repository, "categories", order_by="id")
        category_names = {
            row.get("id"): row.get("cat_name") or row.get("name") or ""
            for row in category_rows
            if row.get("id") is not None
        }

        export_rows: list[dict[str, Any]] = []
        for asset_row in asset_rows:
            raw_status = asset_row.get("status")
            try:
                asset_status = repository._db_status_to_asset_status(raw_status).value
            except Exception:
                asset_status = _stringify_export_value(raw_status)

            export_rows.append(
                {
                    "asset_id": asset_row.get("qr_code") or asset_row.get("id") or "",
                    "asset_status": asset_status,
                    "asset_name": asset_row.get("asset_name") or "",
                    "category": category_names.get(asset_row.get("category_id"), ""),
                    "location": asset_row.get("location") or "",
                }
            )
        return export_rows

    raise RuntimeError(f"repository does not support asset export: {type(repository).__name__}")


def _list_operation_export_rows(repository: TransactionRepository) -> list[dict[str, Any]]:
    if isinstance(repository, InMemoryTransactionRepository):
        return [
            {
                "asset_id": record.asset_id,
                "action_type": record.action_type.value,
                "user_id": record.user_id,
                "user_name": record.user_name,
                "op_time": getattr(record, "op_time", None) or "",
                "hw_seq": record.hw_seq,
                "hw_result": record.hw_result,
            }
            for record in repository.records
        ]

    if isinstance(repository, (SQLiteTransactionRepository, MySQLTransactionRepository)):
        operation_rows = _db_repository_table_rows(repository, "operation_records", order_by="op_time DESC, op_id DESC")
        asset_rows = _maybe_db_repository_table_rows(repository, "assets", order_by="id")
        user_rows = _maybe_db_repository_table_rows(repository, "users", order_by="user_id")

        asset_id_map = {
            row.get("id"): row.get("qr_code") or row.get("id") or ""
            for row in asset_rows
            if row.get("id") is not None
        }
        user_id_map = {
            row.get("user_id"): row.get("student_id") or row.get("user_id") or ""
            for row in user_rows
            if row.get("user_id") is not None
        }
        user_name_map = {
            row.get("user_id"): row.get("user_name") or ""
            for row in user_rows
            if row.get("user_id") is not None
        }

        return [
            {
                "asset_id": asset_id_map.get(row.get("asset_id"), row.get("asset_id") or ""),
                "action_type": row.get("op_type") or "",
                "user_id": user_id_map.get(row.get("user_id"), row.get("user_id") or ""),
                "user_name": user_name_map.get(row.get("user_id"), ""),
                "op_time": row.get("op_time") or "",
                "hw_seq": row.get("hw_seq") or "",
                "hw_result": row.get("hw_result") or "",
            }
            for row in operation_rows
        ]

    raise RuntimeError(f"repository does not support operation export: {type(repository).__name__}")


def _normalize_action_type_filter(raw_action_type: str | None) -> str | None:
    normalized = (raw_action_type or "").strip().upper()
    return normalized if normalized in {action.value for action in ActionType} else None


def _normalize_borrow_request_status_filter(raw_status: str | None) -> BorrowRequestStatus | None:
    normalized = (raw_status or "").strip().upper()
    if not normalized:
        return None
    try:
        return BorrowRequestStatus(normalized)
    except ValueError:
        return None


def _build_borrow_requests_payload(
    repository: TransactionRepository,
    *,
    status: str | None,
    applicant_user_id: str | None,
    asset_id: str | None,
) -> dict[str, Any]:
    normalized_status = _normalize_borrow_request_status_filter(status)
    normalized_applicant_user_id = (applicant_user_id or "").strip() or None
    normalized_asset_id = (asset_id or "").strip() or None
    items = [
        record.to_dict()
        for record in repository.list_borrow_requests(
            status=normalized_status,
            applicant_user_id=normalized_applicant_user_id,
            asset_id=normalized_asset_id,
        )
    ]
    return {
        "filters": {
            "status": None if normalized_status is None else normalized_status.value,
            "applicant_user_id": normalized_applicant_user_id,
            "asset_id": normalized_asset_id,
        },
        "items": items,
        "total": len(items),
        "available_filters": {
            "statuses": [status.value for status in BorrowRequestStatus],
        },
    }


def _list_operation_trace_rows(repository: TransactionRepository) -> list[dict[str, Any]]:
    if isinstance(repository, InMemoryTransactionRepository):
        return [
            {
                "asset_id": record.asset_id,
                "action_type": record.action_type.value,
                "user_id": record.user_id,
                "user_name": record.user_name,
                "op_time": getattr(record, "op_time", None),
                "hw_seq": record.hw_seq,
                "hw_result": record.hw_result,
                "hw_sn": record.hw_sn,
            }
            for record in reversed(repository.records)
        ]

    if isinstance(repository, (SQLiteTransactionRepository, MySQLTransactionRepository)):
        operation_rows = _db_repository_table_rows(repository, "operation_records", order_by="op_time DESC, op_id DESC")
        asset_rows = _maybe_db_repository_table_rows(repository, "assets", order_by="id")
        user_rows = _maybe_db_repository_table_rows(repository, "users", order_by="user_id")

        asset_id_map = {
            row.get("id"): row.get("qr_code") or row.get("id") or ""
            for row in asset_rows
            if row.get("id") is not None
        }
        user_id_map = {
            row.get("user_id"): row.get("student_id") or row.get("user_id") or ""
            for row in user_rows
            if row.get("user_id") is not None
        }
        user_name_map = {
            row.get("user_id"): row.get("user_name") or ""
            for row in user_rows
            if row.get("user_id") is not None
        }

        return [
            {
                "asset_id": asset_id_map.get(row.get("asset_id"), row.get("asset_id") or ""),
                "action_type": row.get("op_type") or "",
                "user_id": user_id_map.get(row.get("user_id"), row.get("user_id") or ""),
                "user_name": user_name_map.get(row.get("user_id"), ""),
                "op_time": row.get("op_time"),
                "hw_seq": row.get("hw_seq"),
                "hw_result": row.get("hw_result"),
                "hw_sn": row.get("hw_sn"),
            }
            for row in operation_rows
        ]

    raise RuntimeError(f"repository does not support records trace view: {type(repository).__name__}")


def _build_records_payload(
    repository: TransactionRepository,
    *,
    asset_id: str | None,
    action_type: str | None,
    time_range: str,
) -> dict[str, Any]:
    normalized_asset_id = (asset_id or "").strip()
    normalized_action_type = _normalize_action_type_filter(action_type)
    normalized_time_range = _normalize_dashboard_time_range(time_range)

    items: list[dict[str, Any]] = []
    for row in _list_operation_trace_rows(repository):
        row_asset_id = str(row.get("asset_id") or "")
        row_action_type = str(row.get("action_type") or "")
        row_op_time = _parse_operation_time(row.get("op_time"))

        if normalized_asset_id and row_asset_id != normalized_asset_id:
            continue
        if normalized_action_type and row_action_type != normalized_action_type:
            continue
        if not _is_operation_in_time_range(row_op_time, normalized_time_range):
            continue

        items.append(
            {
                "asset_id": row_asset_id,
                "action_type": row_action_type,
                "user_id": row.get("user_id") or "",
                "user_name": row.get("user_name") or "",
                "op_time": _stringify_export_value(row_op_time or row.get("op_time")),
                "hw_seq": row.get("hw_seq"),
                "hw_result": row.get("hw_result") or "",
                "hw_sn": row.get("hw_sn"),
            }
        )

    return {
        "filters": {
            "asset_id": normalized_asset_id or None,
            "action_type": normalized_action_type,
            "time_range": normalized_time_range,
        },
        "items": items,
        "total": len(items),
        "available_filters": {
            "action_types": [ActionType.BORROW.value, ActionType.RETURN.value, ActionType.INBOUND.value],
            "time_ranges": list(DASHBOARD_TIME_RANGES),
        },
    }


ASSET_CHANGE_TRANSITIONS = {
    ActionType.INBOUND.value: ("未建档", AssetStatus.IN_STOCK.value),
    ActionType.BORROW.value: (AssetStatus.IN_STOCK.value, AssetStatus.BORROWED.value),
    ActionType.RETURN.value: (AssetStatus.BORROWED.value, AssetStatus.IN_STOCK.value),
}


def _build_asset_changes_payload(
    repository: TransactionRepository,
    *,
    asset_id: str | None,
    action_type: str | None,
    time_range: str,
) -> dict[str, Any]:
    records_payload = _build_records_payload(
        repository,
        asset_id=asset_id,
        action_type=action_type,
        time_range=time_range,
    )

    items: list[dict[str, Any]] = []
    for record in records_payload["items"]:
        transition = ASSET_CHANGE_TRANSITIONS.get(str(record.get("action_type") or ""))
        if transition is None:
            continue

        from_status, to_status = transition
        items.append(
            {
                "asset_id": record.get("asset_id") or "",
                "action_type": record.get("action_type") or "",
                "from_status": from_status,
                "to_status": to_status,
                "user_id": record.get("user_id") or "",
                "user_name": record.get("user_name") or "",
                "op_time": record.get("op_time") or "",
                "hw_seq": record.get("hw_seq"),
                "hw_result": record.get("hw_result") or "",
                "hw_sn": record.get("hw_sn"),
            }
        )

    return {
        "filters": records_payload["filters"],
        "items": items,
        "total": len(items),
        "available_filters": records_payload["available_filters"],
    }


EXCEPTION_RECORD_CODES = (
    ConfirmResult.DEVICE_OFFLINE.value,
    ConfirmResult.ASSET_NOT_FOUND.value,
    ConfirmResult.STATE_INVALID.value,
    ConfirmResult.PERMISSION_DENIED.value,
    ConfirmResult.PARAM_INVALID.value,
    ConfirmResult.BUSY.value,
    ConfirmResult.INTERNAL_ERROR.value,
    ConfirmResult.ACK_TIMEOUT.value,
    ConfirmResult.HW_RESULT_TIMEOUT.value,
    ConfirmResult.CANCELLED.value,
    ConfirmResult.TIMEOUT.value,
    ConfirmResult.ACK_ERROR.value,
    ConfirmResult.ACK_INVALID.value,
)
MAX_RUNTIME_EXCEPTION_RECORDS = 200


def _normalize_exception_code_filter(raw_code: str | None) -> str | None:
    normalized = (raw_code or "").strip().upper()
    return normalized if normalized in EXCEPTION_RECORD_CODES else None


def _record_runtime_exception(
    runtime: ApiRuntime,
    *,
    result_payload: dict[str, Any],
) -> None:
    if result_payload.get("success") is not False:
        return

    code = str(result_payload.get("code") or "").strip()
    if not code or code == ConfirmResult.CONFIRMED.value:
        return

    exception_record = {
        "asset_id": result_payload.get("asset_id") or "",
        "action_type": result_payload.get("action_type") or "",
        "user_id": result_payload.get("user_id") or "",
        "user_name": result_payload.get("user_name") or "",
        "code": code,
        "message": result_payload.get("message") or "",
        "event_time": datetime.now().isoformat(sep=" ", timespec="seconds"),
        "request_seq": result_payload.get("request_seq"),
        "hw_seq": result_payload.get("hw_seq"),
        "hw_result": result_payload.get("hw_result"),
    }

    with runtime.exception_records_lock:
        runtime.exception_records.append(exception_record)
        if len(runtime.exception_records) > MAX_RUNTIME_EXCEPTION_RECORDS:
            del runtime.exception_records[:-MAX_RUNTIME_EXCEPTION_RECORDS]


def _build_exceptions_payload(
    runtime: ApiRuntime,
    *,
    asset_id: str | None,
    action_type: str | None,
    code: str | None,
    time_range: str,
) -> dict[str, Any]:
    normalized_asset_id = (asset_id or "").strip()
    normalized_action_type = _normalize_action_type_filter(action_type)
    normalized_code = _normalize_exception_code_filter(code)
    normalized_time_range = _normalize_dashboard_time_range(time_range)

    with runtime.exception_records_lock:
        exception_rows = list(reversed(runtime.exception_records))

    items: list[dict[str, Any]] = []
    for row in exception_rows:
        row_asset_id = str(row.get("asset_id") or "")
        row_action_type = str(row.get("action_type") or "")
        row_code = str(row.get("code") or "")
        row_event_time = _parse_operation_time(row.get("event_time"))

        if normalized_asset_id and row_asset_id != normalized_asset_id:
            continue
        if normalized_action_type and row_action_type != normalized_action_type:
            continue
        if normalized_code and row_code != normalized_code:
            continue
        if not _is_operation_in_time_range(row_event_time, normalized_time_range):
            continue

        items.append(
            {
                "asset_id": row_asset_id,
                "action_type": row_action_type,
                "user_id": row.get("user_id") or "",
                "user_name": row.get("user_name") or "",
                "code": row_code,
                "message": row.get("message") or "",
                "event_time": _stringify_export_value(row_event_time or row.get("event_time")),
                "request_seq": row.get("request_seq"),
                "hw_seq": row.get("hw_seq"),
                "hw_result": row.get("hw_result"),
            }
        )

    return {
        "filters": {
            "asset_id": normalized_asset_id or None,
            "action_type": normalized_action_type,
            "code": normalized_code,
            "time_range": normalized_time_range,
        },
        "items": items,
        "total": len(items),
        "available_filters": {
            "action_types": [ActionType.BORROW.value, ActionType.RETURN.value, ActionType.INBOUND.value],
            "time_ranges": list(DASHBOARD_TIME_RANGES),
            "codes": list(EXCEPTION_RECORD_CODES),
        },
    }


def create_app(runtime: ApiRuntime | None = None) -> FastAPI:
    runtime = runtime or build_default_runtime()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await runtime.status_hub.startup()
        runtime.open()
        app.state.runtime = runtime
        try:
            yield
        finally:
            runtime.close()
            await runtime.status_hub.shutdown()

    app = FastAPI(
        title="Office Asset Rental Backend",
        version="0.6.2",
        description="Borrow / Return API and websocket status bridge for the office asset rental demo.",
        lifespan=lifespan,
    )
    app.mount("/static", StaticFiles(directory="static"), name="static")

    @app.get("/")
    def read_frontend():
        return FileResponse("static/frontend.html")

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(
            status=runtime.health_status,
            device_status=runtime.service.device_status.value,
            serial_open=runtime.serial_open,
            serial_details=runtime.serial_health_details,
            requested_repository_mode=runtime.requested_repository_mode,
            repository_mode=runtime.repository_mode,
            repository_fallback=runtime.repository_fallback,
            repository_ready=runtime.repository_ready,
            repository_status=runtime.repository_status,
            repository_details=runtime.repository_details,
            startup_error=runtime.startup_error,
        )

    @app.get("/dashboard")
    def get_dashboard(time_range: str = "all", category: str | None = None) -> dict[str, Any]:
        del category
        normalized_time_range = _normalize_dashboard_time_range(time_range)
        try:
            return _build_dashboard_payload(runtime.repository, time_range=normalized_time_range)
        except Exception as exc:
            LOGGER.exception("failed to build dashboard payload")
            raise HTTPException(status_code=500, detail="failed to build dashboard payload") from exc

    @app.get("/records")
    def get_records(
        asset_id: str | None = None,
        action_type: str | None = None,
        time_range: str = "all",
    ) -> dict[str, Any]:
        try:
            return _build_records_payload(
                runtime.repository,
                asset_id=asset_id,
                action_type=action_type,
                time_range=time_range,
            )
        except Exception as exc:
            LOGGER.exception("failed to build records payload")
            raise HTTPException(status_code=500, detail="failed to build records payload") from exc

    @app.get("/asset-changes")
    def get_asset_changes(
        asset_id: str | None = None,
        action_type: str | None = None,
        time_range: str = "all",
    ) -> dict[str, Any]:
        try:
            return _build_asset_changes_payload(
                runtime.repository,
                asset_id=asset_id,
                action_type=action_type,
                time_range=time_range,
            )
        except Exception as exc:
            LOGGER.exception("failed to build asset changes payload")
            raise HTTPException(status_code=500, detail="failed to build asset changes payload") from exc

    @app.get("/exceptions")
    def get_exceptions(
        asset_id: str | None = None,
        action_type: str | None = None,
        code: str | None = None,
        time_range: str = "all",
    ) -> dict[str, Any]:
        try:
            return _build_exceptions_payload(
                runtime,
                asset_id=asset_id,
                action_type=action_type,
                code=code,
                time_range=time_range,
            )
        except Exception as exc:
            LOGGER.exception("failed to build exceptions payload")
            raise HTTPException(status_code=500, detail="failed to build exceptions payload") from exc

    @app.get("/export/assets.csv")
    def export_assets_csv() -> Response:
        try:
            rows = _list_asset_export_rows(runtime.repository)
            return _csv_download_response(
                rows=rows,
                fieldnames=["asset_id", "asset_status", "asset_name", "category", "location"],
                filename="assets_export.csv",
            )
        except Exception as exc:
            LOGGER.exception("failed to export assets csv")
            raise HTTPException(status_code=500, detail="failed to export assets csv") from exc

    @app.get("/export/operations.csv")
    def export_operations_csv() -> Response:
        try:
            rows = _list_operation_export_rows(runtime.repository)
            return _csv_download_response(
                rows=rows,
                fieldnames=["asset_id", "action_type", "user_id", "user_name", "op_time", "hw_seq", "hw_result"],
                filename="operation_records_export.csv",
            )
        except Exception as exc:
            LOGGER.exception("failed to export operations csv")
            raise HTTPException(status_code=500, detail="failed to export operations csv") from exc

    @app.get("/export/records.csv")
    def export_records_csv(
        asset_id: str | None = None,
        action_type: str | None = None,
        time_range: str = "all",
    ) -> Response:
        try:
            payload = _build_records_payload(
                runtime.repository,
                asset_id=asset_id,
                action_type=action_type,
                time_range=time_range,
            )
            return _csv_download_response(
                rows=payload["items"],
                fieldnames=["asset_id", "action_type", "user_id", "user_name", "op_time", "hw_seq", "hw_result", "hw_sn"],
                filename="records_export.csv",
            )
        except Exception as exc:
            LOGGER.exception("failed to export records csv")
            raise HTTPException(status_code=500, detail="failed to export records csv") from exc

    @app.get("/export/exceptions.csv")
    def export_exceptions_csv(
        asset_id: str | None = None,
        action_type: str | None = None,
        code: str | None = None,
        time_range: str = "all",
    ) -> Response:
        try:
            payload = _build_exceptions_payload(
                runtime,
                asset_id=asset_id,
                action_type=action_type,
                code=code,
                time_range=time_range,
            )
            return _csv_download_response(
                rows=payload["items"],
                fieldnames=[
                    "asset_id",
                    "action_type",
                    "user_id",
                    "user_name",
                    "code",
                    "message",
                    "event_time",
                    "request_seq",
                    "hw_seq",
                    "hw_result",
                ],
                filename="exceptions_export.csv",
            )
        except Exception as exc:
            LOGGER.exception("failed to export exceptions csv")
            raise HTTPException(status_code=500, detail="failed to export exceptions csv") from exc

    @app.get("/export/dashboard.json")
    def export_dashboard_json(time_range: str = "all", category: str | None = None) -> Response:
        del category
        normalized_time_range = _normalize_dashboard_time_range(time_range)
        try:
            payload = _build_dashboard_payload(runtime.repository, time_range=normalized_time_range)
            return _json_download_response(payload=payload, filename="dashboard_report.json")
        except Exception as exc:
            LOGGER.exception("failed to export dashboard json")
            raise HTTPException(status_code=500, detail="failed to export dashboard json") from exc

    @app.get("/assets", response_model=dict[str, str])
    def list_assets() -> dict[str, str]:
        try:
            return _list_asset_status_map(runtime.repository)
        except Exception as exc:
            LOGGER.exception("failed to list assets")
            raise HTTPException(status_code=500, detail="failed to list assets") from exc

    @app.get("/assets/{asset_id}", response_model=AssetSnapshotResponse)
    def get_asset(asset_id: str) -> AssetSnapshotResponse:
        asset_status = runtime.service.get_asset_status(asset_id)
        return _build_asset_snapshot(asset_id, asset_status, runtime.service.device_status)

    @app.post("/borrow-requests", response_model=BorrowRequestActionResponse)
    def create_borrow_request(body: BorrowRequestCreateBody) -> BorrowRequestActionResponse:
        try:
            command = BorrowRequestCreateCommand(
                asset_id=body.asset_id,
                user_id=body.user_id,
                user_name=body.user_name,
                reason=body.reason,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        result = runtime.service.create_borrow_request(command)
        return BorrowRequestActionResponse.model_validate(result.to_dict())

    @app.get("/borrow-requests")
    def list_borrow_requests(
        status: str | None = None,
        applicant_user_id: str | None = None,
        asset_id: str | None = None,
    ) -> dict[str, Any]:
        try:
            return _build_borrow_requests_payload(
                runtime.repository,
                status=status,
                applicant_user_id=applicant_user_id,
                asset_id=asset_id,
            )
        except Exception as exc:
            LOGGER.exception("failed to build borrow requests payload")
            raise HTTPException(status_code=500, detail="failed to build borrow requests payload") from exc

    @app.post("/borrow-requests/{request_id}/approve", response_model=BorrowRequestActionResponse)
    def approve_borrow_request(request_id: str, body: BorrowRequestReviewBody) -> BorrowRequestActionResponse:
        try:
            command = BorrowApprovalCommand(
                request_id=request_id,
                reviewer_user_id=body.reviewer_user_id,
                reviewer_user_name=body.reviewer_user_name,
                approved=True,
                review_comment=body.review_comment,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        result = runtime.service.review_borrow_request(command)
        return BorrowRequestActionResponse.model_validate(result.to_dict())

    @app.post("/borrow-requests/{request_id}/reject", response_model=BorrowRequestActionResponse)
    def reject_borrow_request(request_id: str, body: BorrowRequestReviewBody) -> BorrowRequestActionResponse:
        try:
            command = BorrowApprovalCommand(
                request_id=request_id,
                reviewer_user_id=body.reviewer_user_id,
                reviewer_user_name=body.reviewer_user_name,
                approved=False,
                review_comment=body.review_comment,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        result = runtime.service.review_borrow_request(command)
        return BorrowRequestActionResponse.model_validate(result.to_dict())

    @app.post("/borrow-requests/{request_id}/start-borrow", response_model=BusinessResultResponse)
    def start_borrow_from_request(request_id: str, body: BorrowRequestStartBorrowBody) -> BusinessResultResponse:
        result = runtime.service.start_borrow_from_request(request_id, timeout_ms=body.timeout_ms)
        result_payload = result.to_dict()
        _record_runtime_exception(runtime, result_payload=result_payload)
        return BusinessResultResponse.model_validate(result_payload)

    @app.post("/transactions/borrow", response_model=BusinessResultResponse)
    def post_borrow(body: BorrowRequestBody) -> BusinessResultResponse:
        try:
            command = BorrowCommand(
                asset_id=body.asset_id,
                user_id=body.user_id,
                user_name=body.user_name,
                timeout_ms=body.timeout_ms,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        result = runtime.service.request_borrow(command)
        result_payload = result.to_dict()
        _record_runtime_exception(runtime, result_payload=result_payload)
        return BusinessResultResponse.model_validate(result_payload)

    @app.post("/transactions/return", response_model=BusinessResultResponse)
    def post_return(body: ReturnRequestBody) -> BusinessResultResponse:
        try:
            command = ReturnCommand(
                asset_id=body.asset_id,
                user_id=body.user_id,
                user_name=body.user_name,
                timeout_ms=body.timeout_ms,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        result = runtime.service.request_return(command)
        result_payload = result.to_dict()
        _record_runtime_exception(runtime, result_payload=result_payload)
        return BusinessResultResponse.model_validate(result_payload)

    @app.post("/transactions/inbound", response_model=BusinessResultResponse)
    def post_inbound(body: InboundRequestBody) -> BusinessResultResponse:
        try:
            command = InboundCommand(
                asset_id=body.asset_id,
                user_id=body.user_id,
                user_name=body.user_name,
                asset_name=body.asset_name,
                category_id=body.category_id,
                location=body.location,
                timeout_ms=body.timeout_ms,
                request_source="api",
                raw_text=body.raw_text,
                symbology=body.symbology,
            )
        except ValueError as exc:
            result_payload = {
                "success": False,
                "code": ConfirmResult.PARAM_INVALID.value,
                "message": str(exc),
                "asset_id": body.asset_id,
                "action_type": ActionType.INBOUND.value,
                "user_id": body.user_id,
                "user_name": body.user_name,
                "seq_id": -1,
                "request_seq": None,
                "request_id": None,
                "hw_seq": None,
                "hw_result": None,
                "hw_sn": None,
                "device_status": runtime.service.device_status.value,
                "transaction_state": "FAILED",
                "extra": {},
            }
            _record_runtime_exception(runtime, result_payload=result_payload)
            return BusinessResultResponse.model_validate(result_payload)

        result = runtime.service.request_inbound(command)
        result_payload = result.to_dict()
        _record_runtime_exception(runtime, result_payload=result_payload)
        return BusinessResultResponse.model_validate(result_payload)

    @app.post("/scan/result", response_model=ScanResultResponse)
    def post_scan_result(body: ScanResultRequestBody) -> ScanResultResponse:
        try:
            asset_status = runtime.service.get_asset_status(body.asset_id)
            exists = asset_status is not None
            return ScanResultResponse(
                success=exists,
                code="SCAN_ACCEPTED" if exists else "ASSET_NOT_FOUND",
                message="扫描结果已接收" if exists else "资产不存在",
                asset_id=body.asset_id,
                extra={
                    "exists": exists,
                    "asset_status": None if asset_status is None else asset_status.value,
                    "device_status": runtime.service.device_status.value,
                },
            )
        except Exception:
            LOGGER.exception("scan result handling failed for asset_id=%s", body.asset_id)
            return ScanResultResponse(
                success=False,
                code="INTERNAL_ERROR",
                message="扫描结果处理失败",
                asset_id=body.asset_id,
                extra={},
            )

    @app.websocket("/ws/status")
    async def websocket_status(websocket: WebSocket) -> None:
        await runtime.status_hub.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            await runtime.status_hub.disconnect(websocket)
        except Exception:
            await runtime.status_hub.disconnect(websocket)
            raise

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api_app:app",
        host=os.getenv("BACKEND_API_HOST", "127.0.0.1"),
        port=int(os.getenv("BACKEND_API_PORT", "8000")),
        reload=False,
    )
