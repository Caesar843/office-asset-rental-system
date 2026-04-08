from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import runtime_paths  # noqa: F401
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect

import serial_manager as serial_runtime
from api_schemas import (
    AssetSnapshotResponse,
    BorrowRequestBody,
    BusinessResultResponse,
    HealthResponse,
    ReturnRequestBody,
    ScanResultRequestBody,
    ScanResultResponse,
    StatusMessageResponse,
)
from db_repository import MySQLTransactionRepository, RepositoryProbeResult, SQLiteTransactionRepository
from models import AssetStatus, BorrowCommand, DeviceStatus, ReturnCommand
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

    @app.get("/assets/{asset_id}", response_model=AssetSnapshotResponse)
    def get_asset(asset_id: str) -> AssetSnapshotResponse:
        asset_status = runtime.service.get_asset_status(asset_id)
        return _build_asset_snapshot(asset_id, asset_status, runtime.service.device_status)

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
        return BusinessResultResponse.model_validate(result.to_dict())

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
        return BusinessResultResponse.model_validate(result.to_dict())

    @app.post("/scan/result", response_model=ScanResultResponse)
    def post_scan_result(body: ScanResultRequestBody) -> ScanResultResponse:
        asset_status = runtime.service.get_asset_status(body.asset_id)
        return ScanResultResponse(
            asset_id=body.asset_id,
            exists=asset_status is not None,
            asset_status=None if asset_status is None else asset_status.value,
            device_status=runtime.service.device_status.value,
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
