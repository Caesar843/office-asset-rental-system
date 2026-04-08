from __future__ import annotations

import json
import os
import threading
import time
import unittest
from typing import Callable
from unittest.mock import patch

import runtime_paths  # noqa: F401
import serial_manager as serial_runtime
from fastapi.testclient import TestClient

from api_app import ApiRuntime, build_default_runtime, build_status_callback, create_app
from models import ActionType, AssetStatus, ConfirmResult, DeviceStatus
from mock_mcu import MockMCUServer
from protocol import Frame, MsgType
from repository import InMemoryTransactionRepository
from serial_manager import SendResult, SerialManager
from service import AssetConfirmService
from status_hub import StatusHub
from transaction_manager import TransactionManager


class FakeSerialManager:
    def __init__(
        self,
        *,
        open_status: DeviceStatus = DeviceStatus.ONLINE,
        send_result: SendResult | None = None,
        response_factory: Callable[[dict[str, object], int], Frame | None] | None = None,
        response_delay: float = 0.01,
    ) -> None:
        self._open_status = open_status
        self._send_result = send_result
        self._response_factory = response_factory
        self._response_delay = response_delay
        self._next_seq = 100
        self._frame_handler: Callable[[Frame], None] | None = None
        self._status_handler = None
        self.is_open = False

    def set_frame_handler(self, handler: Callable[[Frame], None]) -> None:
        self._frame_handler = handler

    def set_status_handler(self, handler) -> None:
        self._status_handler = handler

    def open(self) -> None:
        self.is_open = True
        if self._status_handler is not None:
            self._status_handler(self._open_status)

    def close(self) -> None:
        self.is_open = False
        if self._status_handler is not None:
            self._status_handler(DeviceStatus.OFFLINE)

    def reserve_seq_id(self) -> int:
        seq_id = self._next_seq
        self._next_seq += 1
        return seq_id

    def send_request(self, msg_type: MsgType, payload: dict[str, object], seq_id: int | None = None) -> SendResult:
        actual_seq_id = self.reserve_seq_id() if seq_id is None else seq_id
        template = self._send_result or SendResult(
            success=True,
            seq_id=actual_seq_id,
            ack_type=MsgType.ACK_OK,
            message="FRAME_RECEIVED",
            ack_payload={"detail": "FRAME_RECEIVED"},
        )
        result = SendResult(
            success=template.success,
            seq_id=actual_seq_id,
            ack_type=template.ack_type,
            message=template.message,
            ack_payload=template.ack_payload,
        )

        if result.success and self._response_factory is not None and self._frame_handler is not None:
            frame = self._response_factory(payload, actual_seq_id)
            if frame is not None:
                threading.Thread(target=self._emit_frame, args=(frame,), daemon=True).start()

        return result

    def _emit_frame(self, frame: Frame) -> None:
        time.sleep(self._response_delay)
        if self._frame_handler is not None:
            self._frame_handler(frame)


class FailingCommitRepository:
    def __init__(self, *, asset_id: str, initial_status: AssetStatus, failure: Exception) -> None:
        self._asset_id = asset_id
        self._asset_status: AssetStatus | None = initial_status
        self._failure = failure
        self.rollback_calls: list[tuple[str, str]] = []

    def get_asset_status(self, asset_id: str) -> AssetStatus | None:
        if asset_id != self._asset_id:
            return None
        return self._asset_status

    def apply_operation_atomically(self, record):
        raise self._failure

    def rollback_transaction(self, asset_id: str, reason: str) -> None:
        self.rollback_calls.append((asset_id, reason))


class ApiAppTests(unittest.TestCase):
    def build_runtime(
        self,
        *,
        serial_manager,
        initial_assets: dict[str, AssetStatus] | None = None,
        repository=None,
        hw_wait_grace_seconds: float = 0.0,
        status_callback: Callable[[dict[str, object]], None] | None = None,
    ) -> ApiRuntime:
        repository = repository or InMemoryTransactionRepository(initial_assets=initial_assets)
        status_hub = StatusHub()
        callback = status_callback or build_status_callback(status_hub)
        service = AssetConfirmService(
            serial_manager=serial_manager,
            repository=repository,
            transaction_manager=TransactionManager(hw_wait_grace_seconds=hw_wait_grace_seconds),
            status_callback=callback,
        )
        return ApiRuntime(
            serial_manager=serial_manager,
            repository=repository,
            service=service,
            status_hub=status_hub,
            requested_repository_mode="inmemory",
            repository_mode="inmemory",
            repository_fallback=False,
            repository_ready=True,
            repository_status="ok",
            repository_details={
                "backend": "inmemory",
                "ready": True,
                "status": "ok",
                "warnings": [],
                "errors": [],
                "details": {"demo_asset_count": len(getattr(repository, "assets", {}))},
            },
        )

    def build_event_frame(
        self,
        payload: dict[str, object],
        *,
        confirm_result: str,
        hw_seq: int = 0x80000001,
    ) -> Frame:
        return Frame.build(
            MsgType.EVT_USER_ACTION,
            seq_id=hw_seq,
            payload={
                "asset_id": payload["asset_id"],
                "request_seq": payload["request_seq"],
                "request_id": payload["request_id"],
                "action_type": payload["action_type"],
                "confirm_result": confirm_result,
                "hw_sn": "STM32F103-A23",
            },
        )

    def test_health_returns_device_status_and_serial_state(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-1001": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/health")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["device_status"], DeviceStatus.ONLINE.value)
        self.assertTrue(payload["serial_open"])
        self.assertEqual(payload["requested_repository_mode"], "inmemory")
        self.assertEqual(payload["repository_mode"], "inmemory")
        self.assertFalse(payload["repository_fallback"])
        self.assertTrue(payload["repository_ready"])
        self.assertEqual(payload["repository_status"], "ok")
        self.assertEqual(payload["repository_details"]["backend"], "inmemory")
        self.assertEqual(payload["serial_details"]["diagnosis"], "connected")

    def test_health_exposes_repository_fallback_metadata(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-1001": AssetStatus.IN_STOCK},
        )
        runtime.requested_repository_mode = "mysql"
        runtime.repository_mode = "inmemory"
        runtime.repository_fallback = True
        runtime.repository_ready = True
        runtime.repository_status = "fallback"
        runtime.repository_details = {
            "backend": "inmemory",
            "ready": True,
            "status": "warning",
            "warnings": ["mysql repository unavailable, fallback to in-memory"],
            "errors": [],
            "details": {"requested_mode": "mysql", "fallback_target": "inmemory"},
        }
        runtime.startup_error = "mysql repository unavailable, fallback to in-memory"
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/health")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "degraded")
        self.assertEqual(payload["requested_repository_mode"], "mysql")
        self.assertEqual(payload["repository_mode"], "inmemory")
        self.assertTrue(payload["repository_fallback"])
        self.assertEqual(payload["repository_status"], "fallback")
        self.assertIn("fallback to in-memory", payload["startup_error"])

    def test_health_reports_mock_socket_diagnostics_when_mock_is_missing(self) -> None:
        env = {
            "BACKEND_REPOSITORY_KIND": "inmemory",
            "BACKEND_MOCK_MCU_HOST": "127.0.0.1",
            "BACKEND_MOCK_MCU_PORT": "9391",
            "BACKEND_SERIAL_PORT": "socket://127.0.0.1:9391",
            "BACKEND_INITIAL_ASSETS_JSON": json.dumps({"AS-1091": "IN_STOCK"}),
        }
        with patch.dict(os.environ, env, clear=False):
            runtime = build_default_runtime()
            app = create_app(runtime)
            with TestClient(app) as client:
                response = client.get("/health")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "degraded")
        self.assertEqual(payload["device_status"], DeviceStatus.OFFLINE.value)
        self.assertFalse(payload["serial_open"])
        self.assertEqual(payload["serial_details"]["mode"], "mock_socket")
        self.assertTrue(payload["serial_details"]["is_mock_mode"])
        self.assertFalse(payload["serial_details"]["is_real_serial_mode"])
        self.assertEqual(payload["serial_details"]["transport"], "socket")
        self.assertEqual(payload["serial_details"]["configured_port"], "socket://127.0.0.1:9391")
        self.assertIn(payload["serial_details"]["diagnosis"], {"connection_refused", "connect_timeout", "open_failed"})
        self.assertIn("mock_mcu.py", payload["serial_details"]["startup_hint"])
        self.assertIn("run_mock_api_flow.py", payload["serial_details"]["demo_flow_command"])
        self.assertIn("mock_mcu", payload["startup_error"])

    def test_health_reports_real_serial_diagnostics_when_pyserial_is_missing(self) -> None:
        env = {
            "BACKEND_REPOSITORY_KIND": "inmemory",
            "BACKEND_SERIAL_PORT": "COM7",
            "BACKEND_INITIAL_ASSETS_JSON": json.dumps({"AS-1093": "IN_STOCK"}),
        }
        with patch.object(serial_runtime, "serial", None):
            with patch.dict(os.environ, env, clear=False):
                runtime = build_default_runtime()
                app = create_app(runtime)
                with TestClient(app) as client:
                    response = client.get("/health")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "degraded")
        self.assertEqual(payload["device_status"], DeviceStatus.OFFLINE.value)
        self.assertFalse(payload["serial_open"])
        self.assertEqual(payload["serial_details"]["mode"], "real_serial")
        self.assertFalse(payload["serial_details"]["is_mock_mode"])
        self.assertTrue(payload["serial_details"]["is_real_serial_mode"])
        self.assertEqual(payload["serial_details"]["transport"], "serial")
        self.assertEqual(payload["serial_details"]["configured_port"], "COM7")
        self.assertEqual(payload["serial_details"]["diagnosis"], "pyserial_missing")
        self.assertEqual(payload["serial_details"]["startup_error_kind"], "pyserial_missing")
        self.assertIn("check_real_serial_runtime.py", payload["serial_details"]["startup_hint"])
        self.assertIn("check_real_serial_runtime.py", payload["serial_details"]["preflight_command"])
        self.assertIn("--serial-port COM7", payload["serial_details"]["preflight_command"])
        self.assertIn("run_real_device_flow.py", payload["serial_details"]["demo_flow_command"])
        self.assertIn("socket://127.0.0.1:9100", payload["serial_details"]["switch_to_mock_hint"])
        self.assertIn("pyserial", payload["startup_error"])

    def test_health_and_websocket_work_with_real_serial_manager_and_mock_mcu(self) -> None:
        mock_server = MockMCUServer(host="127.0.0.1", port=9392, mode="confirmed", confirm_delay=0.05)
        mock_server.start()
        time.sleep(0.1)
        env = {
            "BACKEND_REPOSITORY_KIND": "inmemory",
            "BACKEND_MOCK_MCU_HOST": "127.0.0.1",
            "BACKEND_MOCK_MCU_PORT": "9392",
            "BACKEND_SERIAL_PORT": "socket://127.0.0.1:9392",
            "BACKEND_SERIAL_ACK_TIMEOUT": "0.1",
            "BACKEND_SERIAL_MAX_RETRIES": "3",
            "BACKEND_INITIAL_ASSETS_JSON": json.dumps({"AS-1092": "IN_STOCK"}),
        }
        try:
            with patch.dict(os.environ, env, clear=False):
                runtime = build_default_runtime()
                app = create_app(runtime)
                with TestClient(app) as client:
                    time.sleep(0.2)
                    health = client.get("/health").json()
                    with client.websocket_connect("/ws/status") as websocket:
                        response = client.post(
                            "/transactions/borrow",
                            json={"asset_id": "AS-1092", "user_id": "U-1092", "user_name": "Demo User", "timeout_ms": 300},
                        )
                        messages = [websocket.receive_json(), websocket.receive_json(), websocket.receive_json()]
        finally:
            mock_server.stop()

        self.assertEqual(health["status"], "ok")
        self.assertEqual(health["device_status"], DeviceStatus.ONLINE.value)
        self.assertTrue(health["serial_open"])
        self.assertEqual(health["serial_details"]["transport"], "socket")
        self.assertEqual(health["serial_details"]["diagnosis"], "connected")
        self.assertEqual(response.status_code, 200)
        self.assertEqual([message["code"] for message in messages], ["WAITING_ACK", "WAITING_HW", "CONFIRMED"])
        self.assertTrue(response.json()["success"])
        self.assertEqual(response.json()["transaction_state"], "COMPLETED")

    def test_get_asset_returns_actions_and_scan_result_is_placeholder_only(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-1001": AssetStatus.IN_STOCK, "AS-1002": AssetStatus.BORROWED},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            asset_response = client.get("/assets/AS-1001")
            scan_response = client.post("/scan/result", json={"asset_id": "AS-1002"})
            missing_response = client.get("/assets/AS-404")

        self.assertEqual(asset_response.status_code, 200)
        self.assertEqual(asset_response.json()["available_actions"], [ActionType.BORROW.value])
        self.assertEqual(scan_response.status_code, 200)
        scan_payload = scan_response.json()
        self.assertEqual(
            set(scan_payload.keys()),
            {"asset_id", "exists", "asset_status", "device_status"},
        )
        self.assertEqual(scan_payload["asset_status"], AssetStatus.BORROWED.value)
        self.assertFalse(missing_response.json()["exists"])

    def test_borrow_api_success_with_mock_mcu_confirmed(self) -> None:
        mock_server = MockMCUServer(host="127.0.0.1", port=9301, mode="confirmed", confirm_delay=0.05)
        mock_server.start()
        time.sleep(0.2)
        runtime = self.build_runtime(
            serial_manager=SerialManager(port="socket://127.0.0.1:9301", ack_timeout=0.1, max_retries=3),
            initial_assets={"AS-2001": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        try:
            with TestClient(app) as client:
                response = client.post(
                    "/transactions/borrow",
                    json={"asset_id": "AS-2001", "user_id": "U-2001", "user_name": "Li Qingyun", "timeout_ms": 300},
                )
        finally:
            mock_server.stop()

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["success"])
        self.assertEqual(payload["code"], ConfirmResult.CONFIRMED.value)
        self.assertEqual(payload["transaction_state"], "COMPLETED")

    def test_return_api_success_with_mock_mcu_confirmed(self) -> None:
        mock_server = MockMCUServer(host="127.0.0.1", port=9302, mode="confirmed", confirm_delay=0.05)
        mock_server.start()
        time.sleep(0.2)
        runtime = self.build_runtime(
            serial_manager=SerialManager(port="socket://127.0.0.1:9302", ack_timeout=0.1, max_retries=3),
            initial_assets={"AS-2002": AssetStatus.BORROWED},
        )
        app = create_app(runtime)

        try:
            with TestClient(app) as client:
                response = client.post(
                    "/transactions/return",
                    json={"asset_id": "AS-2002", "user_id": "U-2002", "user_name": "Su Mingyue", "timeout_ms": 300},
                )
        finally:
            mock_server.stop()

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["success"])
        self.assertEqual(payload["code"], ConfirmResult.CONFIRMED.value)
        self.assertEqual(payload["transaction_state"], "COMPLETED")

    def test_borrow_api_offline_failure(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(open_status=DeviceStatus.OFFLINE),
            initial_assets={"AS-3001": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.post(
                "/transactions/borrow",
                json={"asset_id": "AS-3001", "user_id": "U-3001", "user_name": "User A", "timeout_ms": 100},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["success"])
        self.assertEqual(payload["code"], ConfirmResult.DEVICE_OFFLINE.value)

    def test_borrow_api_asset_not_found_failure(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.post(
                "/transactions/borrow",
                json={"asset_id": "AS-404", "user_id": "U-3001", "user_name": "User B", "timeout_ms": 100},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["code"], ConfirmResult.ASSET_NOT_FOUND.value)

    def test_return_api_state_invalid_failure(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-3002": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.post(
                "/transactions/return",
                json={"asset_id": "AS-3002", "user_id": "U-3002", "user_name": "User C", "timeout_ms": 100},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["code"], ConfirmResult.STATE_INVALID.value)

    def test_borrow_api_ack_timeout_failure(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(
                send_result=SendResult(
                    success=False,
                    seq_id=100,
                    ack_type=None,
                    message="ACK timeout after 3 retries",
                    ack_payload=None,
                )
            ),
            initial_assets={"AS-3003": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.post(
                "/transactions/borrow",
                json={"asset_id": "AS-3003", "user_id": "U-3003", "user_name": "User D", "timeout_ms": 100},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["code"], ConfirmResult.ACK_TIMEOUT.value)

    def test_borrow_api_ack_busy_failure(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(
                send_result=SendResult(
                    success=False,
                    seq_id=100,
                    ack_type=MsgType.ACK_BUSY,
                    message="DEVICE_BUSY",
                    ack_payload={"detail": "DEVICE_BUSY"},
                )
            ),
            initial_assets={"AS-3004": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.post(
                "/transactions/borrow",
                json={"asset_id": "AS-3004", "user_id": "U-3004", "user_name": "User E", "timeout_ms": 100},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["code"], ConfirmResult.BUSY.value)

    def test_borrow_api_hardware_cancelled_failure(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(
                response_factory=lambda payload, seq_id: self.build_event_frame(
                    payload,
                    confirm_result=ConfirmResult.CANCELLED.value,
                )
            ),
            initial_assets={"AS-3005": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.post(
                "/transactions/borrow",
                json={"asset_id": "AS-3005", "user_id": "U-3005", "user_name": "User F", "timeout_ms": 100},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["code"], ConfirmResult.CANCELLED.value)

    def test_borrow_api_hw_result_timeout_failure(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-3006": AssetStatus.IN_STOCK},
            hw_wait_grace_seconds=0.0,
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.post(
                "/transactions/borrow",
                json={"asset_id": "AS-3006", "user_id": "U-3006", "user_name": "User G", "timeout_ms": 10},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["code"], ConfirmResult.HW_RESULT_TIMEOUT.value)

    def test_websocket_receives_waiting_and_confirmed_messages_with_unified_schema(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(
                response_factory=lambda payload, seq_id: self.build_event_frame(
                    payload,
                    confirm_result=ConfirmResult.CONFIRMED.value,
                )
            ),
            initial_assets={"AS-4001": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            with client.websocket_connect("/ws/status") as websocket:
                response = client.post(
                    "/transactions/borrow",
                    json={"asset_id": "AS-4001", "user_id": "U-4001", "user_name": "User H", "timeout_ms": 100},
                )
                messages = [websocket.receive_json(), websocket.receive_json(), websocket.receive_json()]

        self.assertEqual(response.status_code, 200)
        self.assertEqual([message["code"] for message in messages], ["WAITING_ACK", "WAITING_HW", "CONFIRMED"])
        self.assertTrue(all("success" in message for message in messages))
        self.assertTrue(all("extra" in message for message in messages))
        self.assertIsNone(messages[0]["success"])
        self.assertIsNone(messages[1]["success"])
        self.assertTrue(messages[2]["success"])

    def test_websocket_receives_failed_message_for_commit_failure(self) -> None:
        repository = FailingCommitRepository(
            asset_id="AS-4002",
            initial_status=AssetStatus.IN_STOCK,
            failure=RuntimeError("db down"),
        )
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(
                response_factory=lambda payload, seq_id: self.build_event_frame(
                    payload,
                    confirm_result=ConfirmResult.CONFIRMED.value,
                )
            ),
            repository=repository,
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            with client.websocket_connect("/ws/status") as websocket:
                response = client.post(
                    "/transactions/borrow",
                    json={"asset_id": "AS-4002", "user_id": "U-4002", "user_name": "User I", "timeout_ms": 100},
                )
                messages = [websocket.receive_json(), websocket.receive_json(), websocket.receive_json()]

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["success"])
        self.assertEqual(payload["code"], ConfirmResult.INTERNAL_ERROR.value)
        self.assertEqual(payload["transaction_state"], "FAILED")
        self.assertEqual(messages[-1]["code"], ConfirmResult.INTERNAL_ERROR.value)
        self.assertEqual(messages[-1]["transaction_state"], "FAILED")
        self.assertFalse(messages[-1]["success"])
        self.assertEqual(repository.rollback_calls, [("AS-4002", "db down")])

    def test_websocket_receives_timeout_message_for_hw_result_timeout(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-4003": AssetStatus.IN_STOCK},
            hw_wait_grace_seconds=0.0,
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            with client.websocket_connect("/ws/status") as websocket:
                response = client.post(
                    "/transactions/borrow",
                    json={"asset_id": "AS-4003", "user_id": "U-4003", "user_name": "User J", "timeout_ms": 10},
                )
                messages = [websocket.receive_json(), websocket.receive_json(), websocket.receive_json()]

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["code"], ConfirmResult.HW_RESULT_TIMEOUT.value)
        self.assertEqual(messages[-1]["code"], ConfirmResult.HW_RESULT_TIMEOUT.value)
        self.assertEqual(messages[-1]["transaction_state"], "FAILED")
        self.assertFalse(messages[-1]["success"])

    def test_websocket_receives_offline_status_message(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-4004": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            with client.websocket_connect("/ws/status") as websocket:
                runtime.service.update_device_status(DeviceStatus.OFFLINE)
                message = websocket.receive_json()

        self.assertEqual(message["code"], ConfirmResult.DEVICE_OFFLINE.value)
        self.assertEqual(message["device_status"], DeviceStatus.OFFLINE.value)
        self.assertIsNone(message["success"])
        self.assertEqual(message["extra"], {})

    def test_status_callback_exception_does_not_break_borrow_api_result(self) -> None:
        def raising_callback(payload: dict[str, object]) -> None:
            raise RuntimeError("callback down")

        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(
                response_factory=lambda payload, seq_id: self.build_event_frame(
                    payload,
                    confirm_result=ConfirmResult.CONFIRMED.value,
                )
            ),
            initial_assets={"AS-4005": AssetStatus.IN_STOCK},
            status_callback=raising_callback,
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.post(
                "/transactions/borrow",
                json={"asset_id": "AS-4005", "user_id": "U-4005", "user_name": "User K", "timeout_ms": 100},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["success"])
        self.assertEqual(payload["code"], ConfirmResult.CONFIRMED.value)


if __name__ == "__main__":
    unittest.main()
