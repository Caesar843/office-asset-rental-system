from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
import json
import os
import tempfile
import threading
import time
import unittest
from typing import Callable
from unittest.mock import patch

import runtime_paths  # noqa: F401
import serial_manager as serial_runtime
from fastapi.testclient import TestClient

from api_app import ApiRuntime, build_default_runtime, build_status_callback, create_app
from db_repository import SQLiteTransactionRepository
from models import AcceptanceResult, ActionType, AssetStatus, ConfirmResult, DeviceStatus, OperationRecordInput
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
    def __init__(self, *, asset_id: str, initial_status: AssetStatus | None, failure: Exception) -> None:
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

    def apply_inbound_atomically(self, commit):
        raise self._failure

    def category_exists(self, category_id: int) -> bool:
        return True

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

    def mark_mysql_repository_unavailable(
        self,
        runtime: ApiRuntime,
        *,
        repository_mode: str = "inmemory",
        repository_fallback: bool = True,
        repository_ready: bool = False,
        repository_status: str = "error",
        startup_error: str = "mysql repository unavailable: dependency missing",
    ) -> ApiRuntime:
        runtime.requested_repository_mode = "mysql"
        runtime.repository_mode = repository_mode
        runtime.repository_fallback = repository_fallback
        runtime.repository_ready = repository_ready
        runtime.repository_status = repository_status
        runtime.repository_details = {
            "backend": repository_mode,
            "ready": repository_ready,
            "status": repository_status,
            "warnings": [],
            "errors": [startup_error],
            "details": {
                "requested_mode": "mysql",
                "fallback_target": repository_mode,
                "write_blocked": True,
            },
        }
        runtime.startup_error = startup_error
        return runtime

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

    def build_operation_record(
        self,
        *,
        asset_id: str,
        user_id: str,
        action_type: ActionType,
        request_seq: int,
    ) -> OperationRecordInput:
        return OperationRecordInput(
            asset_id=asset_id,
            user_id=user_id,
            user_name="Dashboard Tester",
            action_type=action_type,
            request_seq=request_seq,
            request_id=f"req-{request_seq}",
            hw_seq=0x80000000 + request_seq,
            hw_result=ConfirmResult.CONFIRMED.value,
            hw_sn="STM32F103-A23",
            due_time=None,
        )

    def decode_csv_body(self, response) -> str:
        return response.content.decode("utf-8-sig")

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
        runtime = self.mark_mysql_repository_unavailable(
            self.build_runtime(
                serial_manager=FakeSerialManager(),
                initial_assets={"AS-1001": AssetStatus.IN_STOCK},
            )
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/health")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "degraded")
        self.assertEqual(payload["requested_repository_mode"], "mysql")
        self.assertEqual(payload["repository_mode"], "inmemory")
        self.assertTrue(payload["repository_fallback"])
        self.assertFalse(payload["repository_ready"])
        self.assertEqual(payload["repository_status"], "error")
        self.assertTrue(payload["repository_details"]["details"]["write_blocked"])
        self.assertIn("mysql repository unavailable", payload["startup_error"])

    def test_mysql_unavailable_write_endpoints_are_rejected_with_503(self) -> None:
        runtime = self.mark_mysql_repository_unavailable(
            self.build_runtime(
                serial_manager=FakeSerialManager(),
                initial_assets={"AS-MY-1001": AssetStatus.IN_STOCK},
            ),
            startup_error="mysql repository unavailable: connection refused",
        )
        app = create_app(runtime)

        request_cases = [
            ("post", "/borrow-requests", {"asset_id": "AS-MY-1001", "user_id": "U-1001", "user_name": "Borrow User"}),
            (
                "post",
                "/borrow-requests/req-mysql/approve",
                {"reviewer_user_id": "U-ADMIN", "reviewer_user_name": "Admin", "review_comment": "ok"},
            ),
            (
                "post",
                "/borrow-requests/req-mysql/start-borrow",
                {"timeout_ms": 100},
            ),
            (
                "post",
                "/return-acceptances",
                {
                    "asset_id": "AS-MY-1001",
                    "accepted_by_user_id": "U-ADMIN",
                    "accepted_by_user_name": "Admin",
                    "acceptance_result": AcceptanceResult.NORMAL.value,
                    "note": "checked",
                },
            ),
            (
                "post",
                "/transactions/borrow",
                {"asset_id": "AS-MY-1001", "user_id": "U-1001", "user_name": "Borrow User", "timeout_ms": 100},
            ),
            (
                "post",
                "/transactions/return",
                {"asset_id": "AS-MY-1001", "user_id": "U-1001", "user_name": "Borrow User", "timeout_ms": 100},
            ),
            (
                "post",
                "/transactions/inbound",
                {
                    "asset_id": "AS-MY-2001",
                    "user_id": "U-ADMIN",
                    "user_name": "Admin",
                    "asset_name": "New Asset",
                    "category_id": 1,
                    "location": "Shelf A",
                    "timeout_ms": 100,
                },
            ),
        ]

        with TestClient(app) as client:
            for method, path, payload in request_cases:
                response = getattr(client, method)(path, json=payload)
                self.assertEqual(response.status_code, 503, path)
                self.assertIn("requested mysql repository is not ready for formal writes", response.json()["detail"])
                self.assertIn("connection refused", response.json()["detail"])

    def test_mysql_requested_and_probe_not_ready_blocks_writes_without_fallback(self) -> None:
        runtime = self.mark_mysql_repository_unavailable(
            self.build_runtime(
                serial_manager=FakeSerialManager(),
                initial_assets={"AS-MY-3001": AssetStatus.IN_STOCK},
            ),
            repository_mode="mysql",
            repository_fallback=False,
            repository_ready=False,
            repository_status="error",
            startup_error="mysql repository probe failed: missing tables",
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            health_response = client.get("/health")
            write_response = client.post(
                "/transactions/borrow",
                json={"asset_id": "AS-MY-3001", "user_id": "U-3001", "user_name": "User A", "timeout_ms": 100},
            )

        self.assertEqual(health_response.status_code, 200)
        health_payload = health_response.json()
        self.assertEqual(health_payload["requested_repository_mode"], "mysql")
        self.assertEqual(health_payload["repository_mode"], "mysql")
        self.assertFalse(health_payload["repository_fallback"])
        self.assertFalse(health_payload["repository_ready"])
        self.assertEqual(health_payload["repository_status"], "error")
        self.assertEqual(write_response.status_code, 503)
        self.assertIn("missing tables", write_response.json()["detail"])

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
                    health = client.get("/health").json()
                    with client.websocket_connect("/ws/status") as websocket:
                        response = client.post(
                            "/transactions/borrow",
                            json={"asset_id": "AS-1092", "user_id": "U-1092", "user_name": "Demo User", "timeout_ms": 300},
                        )
                        messages = [websocket.receive_json(), websocket.receive_json(), websocket.receive_json()]
                    post_borrow_health = client.get("/health").json()
        finally:
            mock_server.stop()

        self.assertIn(health["status"], {"warning", "ok"})
        self.assertIn(health["device_status"], {DeviceStatus.UNKNOWN.value, DeviceStatus.ONLINE.value})
        self.assertTrue(health["serial_open"])
        self.assertEqual(health["serial_details"]["transport"], "socket")
        self.assertIn(health["serial_details"]["diagnosis"], {"waiting_for_heartbeat", "connected"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual([message["code"] for message in messages], ["WAITING_ACK", "WAITING_HW", "CONFIRMED"])
        self.assertTrue(response.json()["success"])
        self.assertEqual(response.json()["transaction_state"], "COMPLETED")
        self.assertEqual(post_borrow_health["status"], "ok")
        self.assertEqual(post_borrow_health["device_status"], DeviceStatus.ONLINE.value)
        self.assertEqual(post_borrow_health["serial_details"]["diagnosis"], "connected")

    def test_get_asset_returns_actions_and_scan_result_matches_vision_contract(self) -> None:
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
            {"success", "code", "message", "asset_id", "extra"},
        )
        self.assertTrue(scan_payload["success"])
        self.assertEqual(scan_payload["code"], "SCAN_ACCEPTED")
        self.assertTrue(scan_payload["message"])
        self.assertEqual(scan_payload["asset_id"], "AS-1002")
        self.assertEqual(
            scan_payload["extra"],
            {
                "exists": True,
                "asset_status": AssetStatus.BORROWED.value,
                "device_status": DeviceStatus.ONLINE.value,
            },
        )
        self.assertFalse(missing_response.json()["exists"])

    def test_scan_latest_empty_and_missing_asset_scan_are_readable_for_inbound_prefill(self) -> None:
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), initial_assets={})
        app = create_app(runtime)

        with TestClient(app) as client:
            empty_response = client.get("/scan/latest")
            scan_response = client.post(
                "/scan/result",
                json={
                    "asset_id": "AS-9020",
                    "raw_text": "AS-9020",
                    "symbology": "QR",
                    "source_id": "webcam-0",
                    "frame_time": 1700009020,
                },
            )
            latest_response = client.get("/scan/latest")
            assets_response = client.get("/assets")
            records_response = client.get("/records?action_type=INBOUND&time_range=all")

        self.assertEqual(empty_response.status_code, 200)
        self.assertEqual(empty_response.json()["code"], "NO_SCAN_RESULT")
        self.assertFalse(empty_response.json()["success"])

        self.assertEqual(scan_response.status_code, 200)
        self.assertEqual(scan_response.json()["code"], "ASSET_NOT_FOUND")
        self.assertFalse(scan_response.json()["success"])

        self.assertEqual(latest_response.status_code, 200)
        latest_payload = latest_response.json()
        self.assertTrue(latest_payload["success"])
        self.assertEqual(latest_payload["code"], "SCAN_RESULT_AVAILABLE")
        self.assertEqual(latest_payload["asset_id"], "AS-9020")
        self.assertEqual(latest_payload["raw_text"], "AS-9020")
        self.assertEqual(latest_payload["symbology"], "QR")
        self.assertEqual(latest_payload["source_id"], "webcam-0")
        self.assertEqual(latest_payload["frame_time"], 1700009020)
        self.assertTrue(latest_payload["received_at"])
        self.assertEqual(latest_payload["extra"]["exists"], False)
        self.assertEqual(latest_payload["extra"]["scan_code"], "ASSET_NOT_FOUND")

        self.assertNotIn("AS-9020", assets_response.json())
        self.assertEqual(records_response.json()["total"], 0)

    def test_get_assets_returns_frontend_asset_status_map(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-1001": AssetStatus.IN_STOCK, "AS-1002": AssetStatus.BORROWED},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/assets")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "AS-1001": AssetStatus.IN_STOCK.value,
                "AS-1002": AssetStatus.BORROWED.value,
            },
        )

    def test_frontend_home_contains_inbound_tab_and_binding_hooks(self) -> None:
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), initial_assets={})
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/")

        self.assertEqual(response.status_code, 200)
        html = response.text
        for snippet in (
            'data-tab="inbound"',
            'id="inbound-asset-id"',
            'id="inbound-asset-name"',
            'id="inbound-category-id"',
            'id="inbound-location"',
            'id="inbound-user-id"',
            'id="inbound-user-name"',
            'id="inbound-scan-status"',
            'id="inbound-latest-scan-detail"',
            'onclick="submitInbound()"',
            'loadLatestScanResult',
            'useLatestScanResult',
            '/scan/latest',
            'bindScanResultToInbound',
            'refreshInboundRelatedViews',
        ):
            self.assertIn(snippet, html)

    def test_frontend_home_contains_borrow_request_and_approval_hooks(self) -> None:
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), initial_assets={})
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/")

        self.assertEqual(response.status_code, 200)
        html = response.text
        for snippet in (
            'setupBorrowRequestPages',
            'submitBorrowRequest',
            'loadBorrowRequests',
            'approveBorrowRequest',
            'rejectBorrowRequest',
            'startBorrowFromRequest',
            '/borrow-requests',
            '/start-borrow',
            'data-tab="borrow"',
            'id="borrow-request-days"',
            'requested_days: requestedDaysRaw ? Number(requestedDaysRaw) : null,',
            '<strong>借用天数:</strong>',
        ):
            self.assertIn(snippet, html)

    def test_frontend_home_borrow_request_uses_result_success_and_requested_days_wiring(self) -> None:
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), initial_assets={})
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/")

        self.assertEqual(response.status_code, 200)
        html = response.text
        for snippet in (
            'formatApiResultMessage',
            'const isSuccess = result && result.success === true;',
            "setStatusText('borrow-status', `${isSuccess ? '借用申请成功' : '借用申请失败'}：${message}`, isSuccess ? 'success' : 'error');",
            'id="borrow-request-days"',
        ):
            self.assertIn(snippet, html)

    def test_frontend_home_contains_return_acceptance_hooks_and_export_entry(self) -> None:
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), initial_assets={})
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/")

        self.assertEqual(response.status_code, 200)
        html = response.text
        for snippet in (
            'setupReturnAcceptancePage',
            'data-tab="return-acceptances"',
            'id="return-acceptance-asset-id"',
            'id="return-acceptance-result"',
            'id="return-acceptance-note"',
            'id="return-acceptance-user-id"',
            'id="return-acceptance-user-name"',
            'id="return-acceptances-list"',
            'submitReturnAcceptance',
            'loadReturnAcceptances',
            'downloadFilteredReturnAcceptancesExport',
            '/return-acceptances',
            '/export/return-acceptances.csv',
            'DAMAGED / MISSING_PARTS',
        ):
            self.assertIn(snippet, html)

    def test_frontend_home_return_acceptance_uses_result_success_and_readable_error_helpers(self) -> None:
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), initial_assets={})
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/")

        self.assertEqual(response.status_code, 200)
        html = response.text
        for snippet in (
            'formatApiValidationDetail',
            'formatApiResultMessage',
            'const isSuccess = result && result.success === true;',
            '参数校验失败',
            "setStatusText('return-acceptance-status', `验收提交失败：${message}`, 'error');",
        ):
            self.assertIn(snippet, html)

    def test_frontend_home_contains_overdue_exception_filter_and_dashboard_hook(self) -> None:
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), initial_assets={})
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/")

        self.assertEqual(response.status_code, 200)
        html = response.text
        for snippet in (
            '<option value="OVERDUE">OVERDUE</option>',
            "const exceptionStats = dashboard.exception_stats || {};",
            "${exceptionStats.overdue_count || 0}",
        ):
            self.assertIn(snippet, html)

    def test_scan_result_remains_separate_from_inbound_transaction_creation(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={})
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
        app = create_app(runtime)

        with TestClient(app) as client:
            scan_response = client.post("/scan/result", json={"asset_id": "AS-IN-NEW"})
            assets_response = client.get("/assets")
            records_response = client.get("/records?action_type=INBOUND&time_range=all")

        self.assertEqual(scan_response.status_code, 200)
        self.assertEqual(scan_response.json()["code"], "ASSET_NOT_FOUND")
        self.assertEqual(scan_response.json()["asset_id"], "AS-IN-NEW")
        self.assertEqual(assets_response.json(), {})
        self.assertEqual(records_response.json()["total"], 0)
        self.assertEqual(repository.records, [])

    def test_borrow_request_api_create_approve_start_and_list_flow(self) -> None:
        with patch.dict("os.environ", {"BACKEND_MAX_BORROW_DAYS": "30"}, clear=False):
            repository = InMemoryTransactionRepository(initial_assets={"AS-BR-1001": AssetStatus.IN_STOCK})
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
                create_response = client.post(
                    "/borrow-requests",
                    json={
                        "asset_id": "AS-BR-1001",
                        "user_id": "U-BR-1001",
                        "user_name": "Borrow User",
                        "reason": "Project demo",
                        "requested_days": 7,
                    },
                )
                request_id = create_response.json()["item"]["request_id"]
                pending_response = client.get("/borrow-requests?status=PENDING&asset_id=AS-BR-1001")
                approve_response = client.post(
                    f"/borrow-requests/{request_id}/approve",
                    json={
                        "reviewer_user_id": "U-ADMIN",
                        "reviewer_user_name": "Admin User",
                        "review_comment": "approved",
                    },
                )
                approved_response = client.get("/borrow-requests?status=APPROVED&applicant_user_id=U-BR-1001")
                start_response = client.post(
                    f"/borrow-requests/{request_id}/start-borrow",
                    json={"timeout_ms": 300},
                )
                consumed_response = client.get("/borrow-requests?status=CONSUMED&asset_id=AS-BR-1001")
                assets_response = client.get("/assets")

        self.assertEqual(create_response.status_code, 200)
        self.assertEqual(create_response.json()["code"], "REQUEST_CREATED")
        self.assertEqual(create_response.json()["item"]["status"], "PENDING")
        self.assertEqual(create_response.json()["item"]["requested_days"], 7)
        self.assertEqual(pending_response.status_code, 200)
        self.assertEqual(pending_response.json()["total"], 1)
        self.assertEqual(pending_response.json()["items"][0]["request_id"], request_id)
        self.assertEqual(pending_response.json()["items"][0]["requested_days"], 7)
        self.assertEqual(approve_response.status_code, 200)
        self.assertEqual(approve_response.json()["code"], "REQUEST_APPROVED")
        self.assertEqual(approve_response.json()["item"]["status"], "APPROVED")
        self.assertEqual(approved_response.status_code, 200)
        self.assertEqual(approved_response.json()["total"], 1)
        self.assertEqual(approved_response.json()["items"][0]["request_id"], request_id)
        self.assertEqual(start_response.status_code, 200)
        self.assertTrue(start_response.json()["success"])
        self.assertEqual(start_response.json()["code"], ConfirmResult.CONFIRMED.value)
        self.assertEqual(start_response.json()["extra"]["borrow_request_id"], request_id)
        self.assertEqual(start_response.json()["extra"]["requested_days"], 7)
        self.assertTrue(start_response.json()["extra"]["due_time"])
        self.assertEqual(consumed_response.status_code, 200)
        self.assertEqual(consumed_response.json()["total"], 1)
        self.assertEqual(consumed_response.json()["items"][0]["status"], "CONSUMED")
        self.assertEqual(consumed_response.json()["items"][0]["requested_days"], 7)
        self.assertEqual(assets_response.json()["AS-BR-1001"], AssetStatus.BORROWED.value)

    def test_borrow_request_api_accepts_requested_days_at_max_boundary(self) -> None:
        with patch.dict("os.environ", {"BACKEND_MAX_BORROW_DAYS": "30"}, clear=False):
            repository = InMemoryTransactionRepository(initial_assets={"AS-BR-1012": AssetStatus.IN_STOCK})
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
                create_response = client.post(
                    "/borrow-requests",
                    json={
                        "asset_id": "AS-BR-1012",
                        "user_id": "U-BR-1012",
                        "user_name": "Borrow User",
                        "reason": "Max boundary",
                        "requested_days": 30,
                    },
                )
                request_id = create_response.json()["item"]["request_id"]
                approve_response = client.post(
                    f"/borrow-requests/{request_id}/approve",
                    json={
                        "reviewer_user_id": "U-ADMIN",
                        "reviewer_user_name": "Admin User",
                        "review_comment": "approved",
                    },
                )
                start_response = client.post(
                    f"/borrow-requests/{request_id}/start-borrow",
                    json={"timeout_ms": 300},
                )

        self.assertEqual(create_response.status_code, 200)
        self.assertTrue(create_response.json()["success"])
        self.assertEqual(create_response.json()["item"]["requested_days"], 30)
        self.assertEqual(approve_response.status_code, 200)
        self.assertTrue(approve_response.json()["success"])
        self.assertEqual(start_response.status_code, 200)
        self.assertTrue(start_response.json()["success"])
        self.assertEqual(start_response.json()["extra"]["requested_days"], 30)
        self.assertTrue(start_response.json()["extra"]["due_time"])

    def test_borrow_request_api_non_admin_cannot_approve_and_request_stays_pending(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-BR-1003": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            create_response = client.post(
                "/borrow-requests",
                json={
                    "asset_id": "AS-BR-1003",
                    "user_id": "U-BR-1003",
                    "user_name": "Borrow User",
                    "reason": "Project demo",
                },
            )
            request_id = create_response.json()["item"]["request_id"]
            approve_response = client.post(
                f"/borrow-requests/{request_id}/approve",
                json={
                    "reviewer_user_id": "U-NORMAL",
                    "reviewer_user_name": "Normal User",
                    "review_comment": "not allowed",
                },
            )
            pending_response = client.get("/borrow-requests?status=PENDING&asset_id=AS-BR-1003")

        self.assertEqual(approve_response.status_code, 200)
        self.assertFalse(approve_response.json()["success"])
        self.assertEqual(approve_response.json()["code"], ConfirmResult.PERMISSION_DENIED.value)
        self.assertEqual(pending_response.status_code, 200)
        self.assertEqual(pending_response.json()["total"], 1)
        self.assertEqual(pending_response.json()["items"][0]["request_id"], request_id)
        self.assertEqual(pending_response.json()["items"][0]["status"], "PENDING")

    def test_borrow_request_api_reject_and_repeat_review_are_blocked(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-BR-1004": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            create_response = client.post(
                "/borrow-requests",
                json={
                    "asset_id": "AS-BR-1004",
                    "user_id": "U-BR-1004",
                    "user_name": "Borrow User",
                    "reason": "Project demo",
                },
            )
            request_id = create_response.json()["item"]["request_id"]
            reject_response = client.post(
                f"/borrow-requests/{request_id}/reject",
                json={
                    "reviewer_user_id": "U-ADMIN",
                    "reviewer_user_name": "Admin User",
                    "review_comment": "rejected",
                },
            )
            repeat_review_response = client.post(
                f"/borrow-requests/{request_id}/approve",
                json={
                    "reviewer_user_id": "U-ADMIN",
                    "reviewer_user_name": "Admin User",
                    "review_comment": "second review",
                },
            )
            rejected_response = client.get("/borrow-requests?status=REJECTED&asset_id=AS-BR-1004")

        self.assertEqual(reject_response.status_code, 200)
        self.assertTrue(reject_response.json()["success"])
        self.assertEqual(reject_response.json()["code"], "REQUEST_REJECTED")
        self.assertEqual(reject_response.json()["item"]["status"], "REJECTED")
        self.assertEqual(repeat_review_response.status_code, 200)
        self.assertFalse(repeat_review_response.json()["success"])
        self.assertEqual(repeat_review_response.json()["code"], ConfirmResult.STATE_INVALID.value)
        self.assertEqual(repeat_review_response.json()["item"]["status"], "REJECTED")
        self.assertEqual(rejected_response.status_code, 200)
        self.assertEqual(rejected_response.json()["total"], 1)
        self.assertEqual(rejected_response.json()["items"][0]["status"], "REJECTED")

    def test_borrow_request_api_only_approved_can_start_borrow(self) -> None:
        approved_runtime = self.build_runtime(
            serial_manager=FakeSerialManager(
                response_factory=lambda payload, seq_id: self.build_event_frame(
                    payload,
                    confirm_result=ConfirmResult.CONFIRMED.value,
                )
            ),
            initial_assets={"AS-BR-1005": AssetStatus.IN_STOCK, "AS-BR-1006": AssetStatus.IN_STOCK},
        )
        approved_app = create_app(approved_runtime)

        with TestClient(approved_app) as client:
            pending_create = client.post(
                "/borrow-requests",
                json={
                    "asset_id": "AS-BR-1005",
                    "user_id": "U-BR-1005",
                    "user_name": "Borrow User",
                    "reason": "pending path",
                },
            )
            pending_request_id = pending_create.json()["item"]["request_id"]
            pending_start = client.post(
                f"/borrow-requests/{pending_request_id}/start-borrow",
                json={"timeout_ms": 300},
            )

            rejected_create = client.post(
                "/borrow-requests",
                json={
                    "asset_id": "AS-BR-1006",
                    "user_id": "U-BR-1006",
                    "user_name": "Borrow User",
                    "reason": "rejected path",
                },
            )
            rejected_request_id = rejected_create.json()["item"]["request_id"]
            client.post(
                f"/borrow-requests/{rejected_request_id}/reject",
                json={
                    "reviewer_user_id": "U-ADMIN",
                    "reviewer_user_name": "Admin User",
                    "review_comment": "rejected",
                },
            )
            rejected_start = client.post(
                f"/borrow-requests/{rejected_request_id}/start-borrow",
                json={"timeout_ms": 300},
            )

        self.assertEqual(pending_start.status_code, 200)
        self.assertFalse(pending_start.json()["success"])
        self.assertEqual(pending_start.json()["code"], ConfirmResult.STATE_INVALID.value)
        self.assertEqual(pending_start.json()["extra"]["borrow_request_id"], pending_request_id)
        self.assertEqual(pending_start.json()["extra"]["borrow_request_status"], "PENDING")
        self.assertEqual(rejected_start.status_code, 200)
        self.assertFalse(rejected_start.json()["success"])
        self.assertEqual(rejected_start.json()["code"], ConfirmResult.STATE_INVALID.value)
        self.assertEqual(rejected_start.json()["extra"]["borrow_request_id"], rejected_request_id)
        self.assertEqual(rejected_start.json()["extra"]["borrow_request_status"], "REJECTED")

    def test_borrow_request_api_consumed_request_cannot_start_borrow_again(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(
                response_factory=lambda payload, seq_id: self.build_event_frame(
                    payload,
                    confirm_result=ConfirmResult.CONFIRMED.value,
                )
            ),
            initial_assets={"AS-BR-1007": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            create_response = client.post(
                "/borrow-requests",
                json={
                    "asset_id": "AS-BR-1007",
                    "user_id": "U-BR-1007",
                    "user_name": "Borrow User",
                    "reason": "consume path",
                },
            )
            request_id = create_response.json()["item"]["request_id"]
            client.post(
                f"/borrow-requests/{request_id}/approve",
                json={
                    "reviewer_user_id": "U-ADMIN",
                    "reviewer_user_name": "Admin User",
                    "review_comment": "approved",
                },
            )
            first_start = client.post(
                f"/borrow-requests/{request_id}/start-borrow",
                json={"timeout_ms": 300},
            )
            second_start = client.post(
                f"/borrow-requests/{request_id}/start-borrow",
                json={"timeout_ms": 300},
            )

        self.assertEqual(first_start.status_code, 200)
        self.assertTrue(first_start.json()["success"])
        self.assertEqual(second_start.status_code, 200)
        self.assertFalse(second_start.json()["success"])
        self.assertEqual(second_start.json()["code"], ConfirmResult.STATE_INVALID.value)
        self.assertEqual(second_start.json()["extra"]["borrow_request_id"], request_id)
        self.assertEqual(second_start.json()["extra"]["borrow_request_status"], "CONSUMED")

    def test_borrow_request_api_start_borrow_failure_does_not_consume_request(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-BR-1008": AssetStatus.IN_STOCK})
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(
                response_factory=lambda payload, seq_id: self.build_event_frame(
                    payload,
                    confirm_result=ConfirmResult.CANCELLED.value,
                )
            ),
            repository=repository,
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            create_response = client.post(
                "/borrow-requests",
                json={
                    "asset_id": "AS-BR-1008",
                    "user_id": "U-BR-1008",
                    "user_name": "Borrow User",
                    "reason": "failure path",
                },
            )
            request_id = create_response.json()["item"]["request_id"]
            client.post(
                f"/borrow-requests/{request_id}/approve",
                json={
                    "reviewer_user_id": "U-ADMIN",
                    "reviewer_user_name": "Admin User",
                    "review_comment": "approved",
                },
            )
            start_response = client.post(
                f"/borrow-requests/{request_id}/start-borrow",
                json={"timeout_ms": 300},
            )
            approved_response = client.get("/borrow-requests?status=APPROVED&asset_id=AS-BR-1008")
            assets_response = client.get("/assets")

        self.assertEqual(start_response.status_code, 200)
        self.assertFalse(start_response.json()["success"])
        self.assertEqual(start_response.json()["code"], ConfirmResult.CANCELLED.value)
        self.assertEqual(start_response.json()["extra"]["borrow_request_id"], request_id)
        self.assertEqual(approved_response.status_code, 200)
        self.assertEqual(approved_response.json()["total"], 1)
        self.assertEqual(approved_response.json()["items"][0]["status"], "APPROVED")
        self.assertEqual(assets_response.json()["AS-BR-1008"], AssetStatus.IN_STOCK.value)
        self.assertEqual(len(repository.records), 0)

    def test_borrow_request_api_create_is_blocked_by_pending_transaction(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-BR-1002": AssetStatus.IN_STOCK},
        )
        runtime.service.transaction_manager.create_transaction(
            asset_id="AS-BR-1002",
            user_id="U-OTHER",
            user_name="Other User",
            action_type=ActionType.BORROW,
            request_id="req-pending-api",
            request_seq=701,
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.post(
                "/borrow-requests",
                json={
                    "asset_id": "AS-BR-1002",
                    "user_id": "U-BR-1002",
                    "user_name": "Borrow User",
                    "reason": "Need asset",
                },
            )
            list_response = client.get("/borrow-requests?asset_id=AS-BR-1002")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["code"], ConfirmResult.BUSY.value)
        self.assertEqual(list_response.status_code, 200)
        self.assertEqual(list_response.json()["total"], 0)

    def test_borrow_request_api_rejects_requested_days_exceeding_max(self) -> None:
        with patch.dict("os.environ", {"BACKEND_MAX_BORROW_DAYS": "30"}, clear=False):
            runtime = self.build_runtime(
                serial_manager=FakeSerialManager(),
                initial_assets={"AS-BR-1010": AssetStatus.IN_STOCK},
            )
            app = create_app(runtime)

            with TestClient(app) as client:
                response = client.post(
                    "/borrow-requests",
                    json={
                        "asset_id": "AS-BR-1010",
                        "user_id": "U-BR-1010",
                        "user_name": "Borrow User",
                        "reason": "Too long",
                        "requested_days": 31,
                    },
                )
                list_response = client.get("/borrow-requests?asset_id=AS-BR-1010")

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["success"])
        self.assertEqual(response.json()["code"], ConfirmResult.PARAM_INVALID.value)
        self.assertIn("MAX_BORROW_DAYS", response.json()["message"])
        self.assertEqual(list_response.json()["total"], 0)

    def test_borrow_request_api_rejects_non_positive_requested_days_with_422(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-BR-1011": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.post(
                "/borrow-requests",
                json={
                    "asset_id": "AS-BR-1011",
                    "user_id": "U-BR-1011",
                    "user_name": "Borrow User",
                    "reason": "Invalid days",
                    "requested_days": 0,
                },
            )

        self.assertEqual(response.status_code, 422)

    def test_dashboard_returns_summary_operation_stats_and_top_assets(self) -> None:
        repository = InMemoryTransactionRepository(
            initial_assets={
                "AS-1001": AssetStatus.IN_STOCK,
                "AS-1002": AssetStatus.BORROWED,
                "AS-1003": AssetStatus.MAINTENANCE,
                "AS-1004": AssetStatus.SCRAPPED,
            }
        )
        repository.records.extend(
            [
                self.build_operation_record(
                    asset_id="AS-1002",
                    user_id="U-1001",
                    action_type=ActionType.BORROW,
                    request_seq=1,
                ),
                self.build_operation_record(
                    asset_id="AS-1002",
                    user_id="U-1002",
                    action_type=ActionType.BORROW,
                    request_seq=2,
                ),
                self.build_operation_record(
                    asset_id="AS-1001",
                    user_id="U-1003",
                    action_type=ActionType.RETURN,
                    request_seq=3,
                ),
            ]
        )
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/dashboard")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["filters"], {"time_range": "all", "category": None})
        self.assertEqual(
            payload["summary"],
            {
                "in_stock": 1,
                "borrowed": 1,
                "maintenance": 1,
                "scrapped": 1,
            },
        )
        self.assertEqual(
            payload["operation_stats"],
            {
                "borrow_count": 2,
                "return_count": 1,
            },
        )
        self.assertEqual(payload["exception_stats"], {"overdue_count": 0})
        self.assertEqual(payload["borrow_top_assets"], [{"asset_id": "AS-1002", "count": 2}])
        self.assertEqual(payload["available_filters"]["time_ranges"], ["all", "today", "7d", "30d"])
        self.assertEqual(payload["available_filters"]["categories"], [])

    def test_dashboard_time_range_filter_works_for_sqlite_repository(self) -> None:
        handle, db_path = tempfile.mkstemp(dir=os.getcwd(), suffix=".sqlite3")
        os.close(handle)
        os.unlink(db_path)
        recent_time = (datetime.now() - timedelta(days=2)).isoformat(sep=" ", timespec="seconds")
        old_time = (datetime.now() - timedelta(days=40)).isoformat(sep=" ", timespec="seconds")

        try:
            connection = sqlite3.connect(db_path)
            try:
                connection.executescript(
                    """
                    CREATE TABLE assets (
                        id INTEGER PRIMARY KEY,
                        qr_code TEXT,
                        status INTEGER
                    );

                    CREATE TABLE users (
                        user_id INTEGER PRIMARY KEY,
                        student_id TEXT
                    );

                    CREATE TABLE operation_records (
                        op_id INTEGER PRIMARY KEY,
                        asset_id INTEGER,
                        user_id INTEGER,
                        op_type TEXT,
                        op_time TEXT,
                        hw_seq TEXT,
                        hw_result TEXT,
                        due_time TEXT
                    );
                    """
                )
                connection.executemany(
                    "INSERT INTO assets (id, qr_code, status) VALUES (?, ?, ?)",
                    [
                        (1, "AS-9001", 1),
                        (2, "AS-9002", 0),
                    ],
                )
                connection.executemany(
                    "INSERT INTO users (user_id, student_id) VALUES (?, ?)",
                    [
                        (1, "U-9001"),
                        (2, "U-9002"),
                    ],
                )
                connection.executemany(
                    """
                    INSERT INTO operation_records (op_id, asset_id, user_id, op_type, op_time, hw_seq, hw_result, due_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (1, 1, 1, ActionType.BORROW.value, recent_time, "101", ConfirmResult.CONFIRMED.value, None),
                        (2, 2, 2, ActionType.BORROW.value, old_time, "102", ConfirmResult.CONFIRMED.value, None),
                        (3, 1, 1, ActionType.RETURN.value, recent_time, "103", ConfirmResult.CONFIRMED.value, None),
                    ],
                )
                connection.commit()
            finally:
                connection.close()

            repository = SQLiteTransactionRepository(db_path)
            runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
            app = create_app(runtime)

            with TestClient(app) as client:
                response = client.get("/dashboard?time_range=7d")

            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["filters"]["time_range"], "7d")
            self.assertEqual(payload["summary"]["in_stock"], 1)
            self.assertEqual(payload["summary"]["borrowed"], 1)
            self.assertEqual(
                payload["operation_stats"],
                {
                    "borrow_count": 1,
                    "return_count": 1,
                },
            )
            self.assertEqual(payload["borrow_top_assets"], [{"asset_id": "AS-9001", "count": 1}])
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)

    def test_records_returns_filtered_items_with_hardware_trace_fields(self) -> None:
        repository = InMemoryTransactionRepository(
            initial_assets={"AS-1001": AssetStatus.IN_STOCK, "AS-1002": AssetStatus.BORROWED}
        )
        repository.records.extend(
            [
                self.build_operation_record(
                    asset_id="AS-1001",
                    user_id="U-1001",
                    action_type=ActionType.BORROW,
                    request_seq=1,
                ),
                self.build_operation_record(
                    asset_id="AS-1002",
                    user_id="U-1002",
                    action_type=ActionType.RETURN,
                    request_seq=2,
                ),
            ]
        )
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/records?asset_id=AS-1001&action_type=BORROW&time_range=all")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(
            payload["filters"],
            {"asset_id": "AS-1001", "action_type": "BORROW", "time_range": "all"},
        )
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["available_filters"]["action_types"], ["BORROW", "RETURN", "INBOUND"])
        self.assertEqual(payload["available_filters"]["time_ranges"], ["all", "today", "7d", "30d"])
        self.assertEqual(
            payload["items"][0],
            {
                "asset_id": "AS-1001",
                "action_type": "BORROW",
                "user_id": "U-1001",
                "user_name": "Dashboard Tester",
                "op_time": "",
                "hw_seq": 2147483649,
                "hw_result": "CONFIRMED",
                "hw_sn": "STM32F103-A23",
            },
        )

    def test_records_time_range_filter_works_for_sqlite_repository(self) -> None:
        handle, db_path = tempfile.mkstemp(dir=os.getcwd(), suffix=".sqlite3")
        os.close(handle)
        os.unlink(db_path)
        recent_time = (datetime.now() - timedelta(days=1)).isoformat(sep=" ", timespec="seconds")
        old_time = (datetime.now() - timedelta(days=45)).isoformat(sep=" ", timespec="seconds")

        try:
            connection = sqlite3.connect(db_path)
            try:
                connection.executescript(
                    """
                    CREATE TABLE assets (
                        id INTEGER PRIMARY KEY,
                        qr_code TEXT,
                        status INTEGER
                    );

                    CREATE TABLE users (
                        user_id INTEGER PRIMARY KEY,
                        user_name TEXT,
                        student_id TEXT
                    );

                    CREATE TABLE operation_records (
                        op_id INTEGER PRIMARY KEY,
                        asset_id INTEGER,
                        user_id INTEGER,
                        op_type TEXT,
                        op_time TEXT,
                        hw_seq TEXT,
                        hw_result TEXT,
                        hw_sn TEXT,
                        due_time TEXT
                    );
                    """
                )
                connection.executemany(
                    "INSERT INTO assets (id, qr_code, status) VALUES (?, ?, ?)",
                    [
                        (1, "AS-9101", 1),
                        (2, "AS-9102", 0),
                    ],
                )
                connection.executemany(
                    "INSERT INTO users (user_id, user_name, student_id) VALUES (?, ?, ?)",
                    [
                        (1, "Alice", "U-9101"),
                        (2, "Bob", "U-9102"),
                    ],
                )
                connection.executemany(
                    """
                    INSERT INTO operation_records (op_id, asset_id, user_id, op_type, op_time, hw_seq, hw_result, hw_sn, due_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (1, 1, 1, ActionType.BORROW.value, recent_time, "201", ConfirmResult.CONFIRMED.value, "STM32-A", None),
                        (2, 2, 2, ActionType.RETURN.value, old_time, "202", ConfirmResult.CONFIRMED.value, "STM32-B", None),
                    ],
                )
                connection.commit()
            finally:
                connection.close()

            repository = SQLiteTransactionRepository(db_path)
            runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
            app = create_app(runtime)

            with TestClient(app) as client:
                response = client.get("/records?time_range=7d")

            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["filters"]["time_range"], "7d")
            self.assertEqual(payload["total"], 1)
            self.assertEqual(payload["items"][0]["asset_id"], "AS-9101")
            self.assertEqual(payload["items"][0]["action_type"], "BORROW")
            self.assertEqual(payload["items"][0]["user_id"], "U-9101")
            self.assertEqual(payload["items"][0]["user_name"], "Alice")
            self.assertEqual(payload["items"][0]["hw_seq"], "201")
            self.assertEqual(payload["items"][0]["hw_result"], ConfirmResult.CONFIRMED.value)
            self.assertEqual(payload["items"][0]["hw_sn"], "STM32-A")
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)

    def test_asset_changes_returns_filtered_items_with_status_transitions(self) -> None:
        repository = InMemoryTransactionRepository(
            initial_assets={"AS-1001": AssetStatus.IN_STOCK, "AS-1002": AssetStatus.BORROWED}
        )
        repository.records.extend(
            [
                self.build_operation_record(
                    asset_id="AS-1001",
                    user_id="U-1001",
                    action_type=ActionType.BORROW,
                    request_seq=1,
                ),
                self.build_operation_record(
                    asset_id="AS-1002",
                    user_id="U-1002",
                    action_type=ActionType.RETURN,
                    request_seq=2,
                ),
            ]
        )
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/asset-changes?asset_id=AS-1002&action_type=RETURN&time_range=all")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(
            payload["filters"],
            {"asset_id": "AS-1002", "action_type": "RETURN", "time_range": "all"},
        )
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["available_filters"]["action_types"], ["BORROW", "RETURN", "INBOUND"])
        self.assertEqual(payload["available_filters"]["time_ranges"], ["all", "today", "7d", "30d"])
        self.assertEqual(
            payload["items"][0],
            {
                "asset_id": "AS-1002",
                "action_type": "RETURN",
                "from_status": "借出",
                "to_status": "在库",
                "user_id": "U-1002",
                "user_name": "Dashboard Tester",
                "op_time": "",
                "hw_seq": 2147483650,
                "hw_result": "CONFIRMED",
                "hw_sn": "STM32F103-A23",
            },
        )

    def test_asset_changes_time_range_filter_works_for_sqlite_repository(self) -> None:
        handle, db_path = tempfile.mkstemp(dir=os.getcwd(), suffix=".sqlite3")
        os.close(handle)
        os.unlink(db_path)
        recent_time = (datetime.now() - timedelta(days=1)).isoformat(sep=" ", timespec="seconds")
        old_time = (datetime.now() - timedelta(days=45)).isoformat(sep=" ", timespec="seconds")

        try:
            connection = sqlite3.connect(db_path)
            try:
                connection.executescript(
                    """
                    CREATE TABLE assets (
                        id INTEGER PRIMARY KEY,
                        qr_code TEXT,
                        status INTEGER
                    );

                    CREATE TABLE users (
                        user_id INTEGER PRIMARY KEY,
                        user_name TEXT,
                        student_id TEXT
                    );

                    CREATE TABLE operation_records (
                        op_id INTEGER PRIMARY KEY,
                        asset_id INTEGER,
                        user_id INTEGER,
                        op_type TEXT,
                        op_time TEXT,
                        hw_seq TEXT,
                        hw_result TEXT,
                        hw_sn TEXT,
                        due_time TEXT
                    );
                    """
                )
                connection.executemany(
                    "INSERT INTO assets (id, qr_code, status) VALUES (?, ?, ?)",
                    [
                        (1, "AS-9301", 1),
                        (2, "AS-9302", 0),
                    ],
                )
                connection.executemany(
                    "INSERT INTO users (user_id, user_name, student_id) VALUES (?, ?, ?)",
                    [
                        (1, "Alice", "U-9301"),
                        (2, "Bob", "U-9302"),
                    ],
                )
                connection.executemany(
                    """
                    INSERT INTO operation_records (op_id, asset_id, user_id, op_type, op_time, hw_seq, hw_result, hw_sn, due_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (1, 1, 1, ActionType.BORROW.value, recent_time, "401", ConfirmResult.CONFIRMED.value, "STM32-E", None),
                        (2, 2, 2, ActionType.RETURN.value, old_time, "402", ConfirmResult.CONFIRMED.value, "STM32-F", None),
                    ],
                )
                connection.commit()
            finally:
                connection.close()

            repository = SQLiteTransactionRepository(db_path)
            runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
            app = create_app(runtime)

            with TestClient(app) as client:
                response = client.get("/asset-changes?time_range=7d")

            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["filters"]["time_range"], "7d")
            self.assertEqual(payload["total"], 1)
            self.assertEqual(payload["items"][0]["asset_id"], "AS-9301")
            self.assertEqual(payload["items"][0]["action_type"], "BORROW")
            self.assertEqual(payload["items"][0]["from_status"], "在库")
            self.assertEqual(payload["items"][0]["to_status"], "借出")
            self.assertEqual(payload["items"][0]["user_id"], "U-9301")
            self.assertEqual(payload["items"][0]["user_name"], "Alice")
            self.assertEqual(payload["items"][0]["hw_seq"], "401")
            self.assertEqual(payload["items"][0]["hw_result"], ConfirmResult.CONFIRMED.value)
            self.assertEqual(payload["items"][0]["hw_sn"], "STM32-E")
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)

    def test_exceptions_returns_runtime_failure_items_with_filters(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(open_status=DeviceStatus.OFFLINE),
            initial_assets={"AS-9401": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            failure_response = client.post(
                "/transactions/borrow",
                json={"asset_id": "AS-9401", "user_id": "U-9401", "user_name": "User X", "timeout_ms": 100},
            )
            response = client.get("/exceptions?asset_id=AS-9401&action_type=BORROW&code=DEVICE_OFFLINE&time_range=all")

        self.assertEqual(failure_response.status_code, 200)
        self.assertEqual(failure_response.json()["code"], ConfirmResult.DEVICE_OFFLINE.value)
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(
            payload["filters"],
            {
                "asset_id": "AS-9401",
                "action_type": "BORROW",
                "code": "DEVICE_OFFLINE",
                "time_range": "all",
            },
        )
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["available_filters"]["action_types"], ["BORROW", "RETURN", "INBOUND"])
        self.assertEqual(payload["available_filters"]["time_ranges"], ["all", "today", "7d", "30d"])
        self.assertIn(ConfirmResult.DEVICE_OFFLINE.value, payload["available_filters"]["codes"])
        item = payload["items"][0]
        self.assertEqual(item["asset_id"], "AS-9401")
        self.assertEqual(item["action_type"], ActionType.BORROW.value)
        self.assertEqual(item["user_id"], "U-9401")
        self.assertEqual(item["user_name"], "User X")
        self.assertEqual(item["code"], ConfirmResult.DEVICE_OFFLINE.value)
        self.assertTrue(item["message"])
        self.assertTrue(item["event_time"])
        self.assertIn("request_seq", item)
        self.assertIn("hw_seq", item)
        self.assertIn("hw_result", item)

    def test_borrow_busy_failure_is_recorded_in_exceptions_feed(self) -> None:
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
            initial_assets={"AS-9402": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            failure_response = client.post(
                "/transactions/borrow",
                json={"asset_id": "AS-9402", "user_id": "U-9402", "user_name": "User Busy", "timeout_ms": 100},
            )
            response = client.get("/exceptions?asset_id=AS-9402&action_type=BORROW&code=BUSY&time_range=all")

        self.assertEqual(failure_response.status_code, 200)
        self.assertEqual(failure_response.json()["code"], ConfirmResult.BUSY.value)
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["items"][0]["asset_id"], "AS-9402")
        self.assertEqual(payload["items"][0]["action_type"], ActionType.BORROW.value)
        self.assertEqual(payload["items"][0]["code"], ConfirmResult.BUSY.value)
        self.assertIn(ConfirmResult.BUSY.value, payload["available_filters"]["codes"])

    def test_inbound_state_invalid_failure_is_recorded_in_exceptions_feed(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-9403": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            failure_response = client.post(
                "/transactions/inbound",
                json={
                    "asset_id": "AS-9403",
                    "user_id": "U-ADMIN",
                    "user_name": "Admin",
                    "asset_name": "Existing Monitor",
                    "category_id": 1,
                    "location": "Shelf A",
                    "timeout_ms": 100,
                },
            )
            response = client.get("/exceptions?asset_id=AS-9403&action_type=INBOUND&code=STATE_INVALID&time_range=all")

        self.assertEqual(failure_response.status_code, 200)
        self.assertEqual(failure_response.json()["code"], ConfirmResult.STATE_INVALID.value)
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["items"][0]["asset_id"], "AS-9403")
        self.assertEqual(payload["items"][0]["action_type"], ActionType.INBOUND.value)
        self.assertEqual(payload["items"][0]["code"], ConfirmResult.STATE_INVALID.value)
        self.assertIn(ConfirmResult.STATE_INVALID.value, payload["available_filters"]["codes"])

    def test_overdue_assets_are_visible_in_exceptions_export_and_dashboard(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-9404": AssetStatus.BORROWED})
        borrow_record = self.build_operation_record(
            asset_id="AS-9404",
            user_id="U-9404",
            action_type=ActionType.BORROW,
            request_seq=404,
        )
        borrow_record.due_time = (datetime.now() - timedelta(days=2)).isoformat(sep=" ", timespec="seconds")
        repository.records.append(borrow_record)

        runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
        app = create_app(runtime)

        with TestClient(app) as client:
            exceptions_response = client.get("/exceptions?asset_id=AS-9404&action_type=BORROW&code=OVERDUE&time_range=all")
            export_response = client.get("/export/exceptions.csv?asset_id=AS-9404&code=OVERDUE&time_range=all")
            dashboard_response = client.get("/dashboard?time_range=all")

        self.assertEqual(exceptions_response.status_code, 200)
        exceptions_payload = exceptions_response.json()
        self.assertEqual(exceptions_payload["total"], 1)
        self.assertIn("OVERDUE", exceptions_payload["available_filters"]["codes"])
        overdue_item = exceptions_payload["items"][0]
        self.assertEqual(overdue_item["asset_id"], "AS-9404")
        self.assertEqual(overdue_item["action_type"], ActionType.BORROW.value)
        self.assertEqual(overdue_item["user_id"], "U-9404")
        self.assertEqual(overdue_item["code"], "OVERDUE")
        self.assertEqual(overdue_item["message"], "borrow due_time exceeded")
        self.assertEqual(overdue_item["request_seq"], 404)
        self.assertEqual(overdue_item["hw_result"], ConfirmResult.CONFIRMED.value)

        self.assertEqual(export_response.status_code, 200)
        csv_text = self.decode_csv_body(export_response)
        self.assertIn(
            "AS-9404,BORROW,U-9404,Dashboard Tester,OVERDUE,borrow due_time exceeded",
            csv_text,
        )

        self.assertEqual(dashboard_response.status_code, 200)
        dashboard_payload = dashboard_response.json()
        self.assertEqual(dashboard_payload["exception_stats"], {"overdue_count": 1})

    def test_exceptions_time_range_filter_works_for_runtime_records(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-9501": AssetStatus.IN_STOCK},
        )
        recent_time = (datetime.now() - timedelta(days=1)).isoformat(sep=" ", timespec="seconds")
        old_time = (datetime.now() - timedelta(days=45)).isoformat(sep=" ", timespec="seconds")

        with runtime.exception_records_lock:
            runtime.exception_records.extend(
                [
                    {
                        "asset_id": "AS-9501",
                        "action_type": ActionType.BORROW.value,
                        "user_id": "U-9501",
                        "user_name": "Alice",
                        "code": ConfirmResult.INTERNAL_ERROR.value,
                        "message": "recent error",
                        "event_time": recent_time,
                        "request_seq": 100,
                        "hw_seq": None,
                        "hw_result": None,
                    },
                    {
                        "asset_id": "AS-9502",
                        "action_type": ActionType.RETURN.value,
                        "user_id": "U-9502",
                        "user_name": "Bob",
                        "code": ConfirmResult.STATE_INVALID.value,
                        "message": "old error",
                        "event_time": old_time,
                        "request_seq": None,
                        "hw_seq": None,
                        "hw_result": None,
                    },
                ]
            )

        app = create_app(runtime)
        with TestClient(app) as client:
            response = client.get("/exceptions?time_range=7d")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["filters"]["time_range"], "7d")
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["items"][0]["asset_id"], "AS-9501")
        self.assertEqual(payload["items"][0]["action_type"], ActionType.BORROW.value)
        self.assertEqual(payload["items"][0]["code"], ConfirmResult.INTERNAL_ERROR.value)
        self.assertEqual(payload["items"][0]["message"], "recent error")

    def test_export_assets_csv_is_accessible(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-1001": AssetStatus.IN_STOCK, "AS-1002": AssetStatus.BORROWED},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/export/assets.csv")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.headers["content-type"].startswith("text/csv"))
        self.assertIn("assets_export.csv", response.headers.get("content-disposition", ""))
        csv_text = self.decode_csv_body(response)
        self.assertIn("asset_id,asset_status,asset_name,category,location", csv_text)
        self.assertIn("AS-1001,在库", csv_text)
        self.assertIn("AS-1002,借出", csv_text)

    def test_export_operations_csv_is_accessible(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-1001": AssetStatus.IN_STOCK})
        repository.records.extend(
            [
                self.build_operation_record(
                    asset_id="AS-1001",
                    user_id="U-1001",
                    action_type=ActionType.BORROW,
                    request_seq=1,
                ),
                self.build_operation_record(
                    asset_id="AS-1001",
                    user_id="U-1001",
                    action_type=ActionType.RETURN,
                    request_seq=2,
                ),
            ]
        )
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/export/operations.csv")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.headers["content-type"].startswith("text/csv"))
        self.assertIn("operation_records_export.csv", response.headers.get("content-disposition", ""))
        csv_text = self.decode_csv_body(response)
        self.assertIn("asset_id,action_type,user_id,user_name,op_time,hw_seq,hw_result", csv_text)
        self.assertIn("AS-1001,BORROW,U-1001,Dashboard Tester", csv_text)
        self.assertIn("AS-1001,RETURN,U-1001,Dashboard Tester", csv_text)

    def test_export_records_csv_respects_asset_and_action_filters(self) -> None:
        repository = InMemoryTransactionRepository(
            initial_assets={"AS-1001": AssetStatus.IN_STOCK, "AS-1002": AssetStatus.BORROWED}
        )
        repository.records.extend(
            [
                self.build_operation_record(
                    asset_id="AS-1001",
                    user_id="U-1001",
                    action_type=ActionType.BORROW,
                    request_seq=1,
                ),
                self.build_operation_record(
                    asset_id="AS-1002",
                    user_id="U-1002",
                    action_type=ActionType.RETURN,
                    request_seq=2,
                ),
            ]
        )
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/export/records.csv?asset_id=AS-1001&action_type=BORROW&time_range=all")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.headers["content-type"].startswith("text/csv"))
        self.assertIn("records_export.csv", response.headers.get("content-disposition", ""))
        csv_text = self.decode_csv_body(response)
        self.assertIn("asset_id,action_type,user_id,user_name,op_time,hw_seq,hw_result,hw_sn", csv_text)
        self.assertIn("AS-1001,BORROW,U-1001,Dashboard Tester,,2147483649,CONFIRMED,STM32F103-A23", csv_text)
        self.assertNotIn("AS-1002,RETURN", csv_text)

    def test_export_records_csv_respects_time_range_filter(self) -> None:
        handle, db_path = tempfile.mkstemp(dir=os.getcwd(), suffix=".sqlite3")
        os.close(handle)
        os.unlink(db_path)
        recent_time = (datetime.now() - timedelta(days=1)).isoformat(sep=" ", timespec="seconds")
        old_time = (datetime.now() - timedelta(days=45)).isoformat(sep=" ", timespec="seconds")

        try:
            connection = sqlite3.connect(db_path)
            try:
                connection.executescript(
                    """
                    CREATE TABLE assets (
                        id INTEGER PRIMARY KEY,
                        qr_code TEXT,
                        status INTEGER
                    );

                    CREATE TABLE users (
                        user_id INTEGER PRIMARY KEY,
                        user_name TEXT,
                        student_id TEXT
                    );

                    CREATE TABLE operation_records (
                        op_id INTEGER PRIMARY KEY,
                        asset_id INTEGER,
                        user_id INTEGER,
                        op_type TEXT,
                        op_time TEXT,
                        hw_seq TEXT,
                        hw_result TEXT,
                        hw_sn TEXT,
                        due_time TEXT
                    );
                    """
                )
                connection.executemany(
                    "INSERT INTO assets (id, qr_code, status) VALUES (?, ?, ?)",
                    [
                        (1, "AS-9201", 1),
                        (2, "AS-9202", 0),
                    ],
                )
                connection.executemany(
                    "INSERT INTO users (user_id, user_name, student_id) VALUES (?, ?, ?)",
                    [
                        (1, "Alice", "U-9201"),
                        (2, "Bob", "U-9202"),
                    ],
                )
                connection.executemany(
                    """
                    INSERT INTO operation_records (op_id, asset_id, user_id, op_type, op_time, hw_seq, hw_result, hw_sn, due_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (1, 1, 1, ActionType.BORROW.value, recent_time, "301", ConfirmResult.CONFIRMED.value, "STM32-C", None),
                        (2, 2, 2, ActionType.RETURN.value, old_time, "302", ConfirmResult.CONFIRMED.value, "STM32-D", None),
                    ],
                )
                connection.commit()
            finally:
                connection.close()

            repository = SQLiteTransactionRepository(db_path)
            runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
            app = create_app(runtime)

            with TestClient(app) as client:
                response = client.get("/export/records.csv?time_range=7d")

            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.headers["content-type"].startswith("text/csv"))
            csv_text = self.decode_csv_body(response)
            self.assertIn("asset_id,action_type,user_id,user_name,op_time,hw_seq,hw_result,hw_sn", csv_text)
            self.assertIn("AS-9201,BORROW,U-9201,Alice", csv_text)
            self.assertIn("301,CONFIRMED,STM32-C", csv_text)
            self.assertNotIn("AS-9202,RETURN,U-9202,Bob", csv_text)
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)

    def test_return_acceptance_create_list_export_and_duplicate_rejection(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-RA-1001": AssetStatus.IN_STOCK})
        repository.records.append(
            self.build_operation_record(
                asset_id="AS-RA-1001",
                user_id="U-1001",
                action_type=ActionType.RETURN,
                request_seq=9101,
            )
        )
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
        app = create_app(runtime)

        with TestClient(app) as client:
            create_response = client.post(
                "/return-acceptances",
                json={
                    "asset_id": "AS-RA-1001",
                    "accepted_by_user_id": "U-ADMIN",
                    "accepted_by_user_name": "Admin",
                    "acceptance_result": AcceptanceResult.NORMAL.value,
                    "note": "checked",
                },
            )
            duplicate_response = client.post(
                "/return-acceptances",
                json={
                    "asset_id": "AS-RA-1001",
                    "accepted_by_user_id": "U-ADMIN",
                    "accepted_by_user_name": "Admin",
                    "acceptance_result": AcceptanceResult.NORMAL.value,
                },
            )
            list_response = client.get(
                "/return-acceptances?asset_id=AS-RA-1001&acceptance_result=NORMAL&accepted_by_user_id=U-ADMIN&time_range=all"
            )
            export_response = client.get("/export/return-acceptances.csv?asset_id=AS-RA-1001&time_range=all")

        self.assertEqual(create_response.status_code, 200)
        self.assertTrue(create_response.json()["success"])
        self.assertEqual(create_response.json()["code"], "ACCEPTANCE_CREATED")
        self.assertEqual(create_response.json()["item"]["related_return_request_seq"], 9101)
        self.assertEqual(create_response.json()["item"]["related_return_hw_seq"], 0x80000000 + 9101)
        self.assertEqual(duplicate_response.status_code, 200)
        self.assertFalse(duplicate_response.json()["success"])
        self.assertEqual(duplicate_response.json()["code"], ConfirmResult.STATE_INVALID.value)
        self.assertEqual(list_response.status_code, 200)
        self.assertEqual(list_response.json()["total"], 1)
        self.assertEqual(list_response.json()["items"][0]["acceptance_result"], AcceptanceResult.NORMAL.value)
        self.assertEqual(export_response.status_code, 200)
        self.assertIn("return_acceptances_export.csv", export_response.headers.get("content-disposition", ""))
        csv_text = self.decode_csv_body(export_response)
        self.assertIn(
            "asset_id,acceptance_result,note,accepted_by_user_id,accepted_by_user_name,accepted_at,related_return_request_seq,related_return_request_id,related_return_hw_seq",
            csv_text,
        )
        self.assertIn("AS-RA-1001,NORMAL,checked,U-ADMIN,Admin", csv_text)

    def test_return_acceptance_api_supports_damaged_and_missing_parts_results(self) -> None:
        repository = InMemoryTransactionRepository(
            initial_assets={
                "AS-RA-1002": AssetStatus.IN_STOCK,
                "AS-RA-1003": AssetStatus.IN_STOCK,
            }
        )
        repository.records.extend(
            [
                self.build_operation_record(
                    asset_id="AS-RA-1002",
                    user_id="U-1002",
                    action_type=ActionType.RETURN,
                    request_seq=9102,
                ),
                self.build_operation_record(
                    asset_id="AS-RA-1003",
                    user_id="U-1003",
                    action_type=ActionType.RETURN,
                    request_seq=9103,
                ),
            ]
        )
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
        app = create_app(runtime)

        with TestClient(app) as client:
            damaged_response = client.post(
                "/return-acceptances",
                json={
                    "asset_id": "AS-RA-1002",
                    "accepted_by_user_id": "U-ADMIN",
                    "accepted_by_user_name": "Admin",
                    "acceptance_result": AcceptanceResult.DAMAGED.value,
                },
            )
            missing_parts_response = client.post(
                "/return-acceptances",
                json={
                    "asset_id": "AS-RA-1003",
                    "accepted_by_user_id": "U-ADMIN",
                    "accepted_by_user_name": "Admin",
                    "acceptance_result": AcceptanceResult.MISSING_PARTS.value,
                },
            )
            damaged_list = client.get("/return-acceptances?acceptance_result=DAMAGED&time_range=all")
            missing_parts_list = client.get("/return-acceptances?acceptance_result=MISSING_PARTS&time_range=all")

        self.assertTrue(damaged_response.json()["success"])
        self.assertEqual(damaged_response.json()["item"]["acceptance_result"], AcceptanceResult.DAMAGED.value)
        self.assertTrue(missing_parts_response.json()["success"])
        self.assertEqual(
            missing_parts_response.json()["item"]["acceptance_result"],
            AcceptanceResult.MISSING_PARTS.value,
        )
        self.assertEqual(damaged_list.json()["total"], 1)
        self.assertEqual(missing_parts_list.json()["total"], 1)

    def test_return_acceptance_api_rejects_non_admin_no_return_trace_and_invalid_payload(self) -> None:
        repository = InMemoryTransactionRepository(
            initial_assets={
                "AS-RA-1004": AssetStatus.IN_STOCK,
                "AS-RA-1005": AssetStatus.IN_STOCK,
            }
        )
        repository.records.append(
            self.build_operation_record(
                asset_id="AS-RA-1004",
                user_id="U-1004",
                action_type=ActionType.RETURN,
                request_seq=9104,
            )
        )
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
        app = create_app(runtime)

        with TestClient(app) as client:
            non_admin_response = client.post(
                "/return-acceptances",
                json={
                    "asset_id": "AS-RA-1004",
                    "accepted_by_user_id": "U-1004",
                    "accepted_by_user_name": "Borrower",
                    "acceptance_result": AcceptanceResult.NORMAL.value,
                },
            )
            no_return_response = client.post(
                "/return-acceptances",
                json={
                    "asset_id": "AS-RA-1005",
                    "accepted_by_user_id": "U-ADMIN",
                    "accepted_by_user_name": "Admin",
                    "acceptance_result": AcceptanceResult.NORMAL.value,
                },
            )
            invalid_result_response = client.post(
                "/return-acceptances",
                json={
                    "asset_id": "AS-RA-1004",
                    "accepted_by_user_id": "U-ADMIN",
                    "accepted_by_user_name": "Admin",
                    "acceptance_result": "BROKEN",
                },
            )
            blank_asset_response = client.post(
                "/return-acceptances",
                json={
                    "asset_id": "   ",
                    "accepted_by_user_id": "U-ADMIN",
                    "accepted_by_user_name": "Admin",
                    "acceptance_result": AcceptanceResult.NORMAL.value,
                },
            )

        self.assertEqual(non_admin_response.status_code, 200)
        self.assertFalse(non_admin_response.json()["success"])
        self.assertEqual(non_admin_response.json()["code"], ConfirmResult.PERMISSION_DENIED.value)
        self.assertEqual(no_return_response.status_code, 200)
        self.assertFalse(no_return_response.json()["success"])
        self.assertEqual(no_return_response.json()["code"], ConfirmResult.STATE_INVALID.value)
        self.assertEqual(invalid_result_response.status_code, 422)
        self.assertEqual(blank_asset_response.status_code, 422)

    def test_export_exceptions_csv_respects_asset_action_and_code_filters(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-9601": AssetStatus.IN_STOCK},
        )
        with runtime.exception_records_lock:
            runtime.exception_records.extend(
                [
                    {
                        "asset_id": "AS-9601",
                        "action_type": ActionType.BORROW.value,
                        "user_id": "U-9601",
                        "user_name": "Alice",
                        "code": ConfirmResult.DEVICE_OFFLINE.value,
                        "message": "device offline",
                        "event_time": datetime.now().isoformat(sep=" ", timespec="seconds"),
                        "request_seq": 101,
                        "hw_seq": None,
                        "hw_result": None,
                    },
                    {
                        "asset_id": "AS-9602",
                        "action_type": ActionType.RETURN.value,
                        "user_id": "U-9602",
                        "user_name": "Bob",
                        "code": ConfirmResult.STATE_INVALID.value,
                        "message": "state invalid",
                        "event_time": datetime.now().isoformat(sep=" ", timespec="seconds"),
                        "request_seq": 102,
                        "hw_seq": None,
                        "hw_result": None,
                    },
                ]
            )

        app = create_app(runtime)
        with TestClient(app) as client:
            response = client.get(
                "/export/exceptions.csv?asset_id=AS-9601&action_type=BORROW&code=DEVICE_OFFLINE&time_range=all"
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.headers["content-type"].startswith("text/csv"))
        self.assertIn("exceptions_export.csv", response.headers.get("content-disposition", ""))
        csv_text = self.decode_csv_body(response)
        self.assertIn(
            "asset_id,action_type,user_id,user_name,code,message,event_time,request_seq,hw_seq,hw_result",
            csv_text,
        )
        self.assertIn("AS-9601,BORROW,U-9601,Alice,DEVICE_OFFLINE,device offline", csv_text)
        self.assertNotIn("AS-9602,RETURN,U-9602,Bob,STATE_INVALID,state invalid", csv_text)

    def test_export_exceptions_csv_respects_time_range_filter(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-9701": AssetStatus.IN_STOCK},
        )
        recent_time = (datetime.now() - timedelta(days=1)).isoformat(sep=" ", timespec="seconds")
        old_time = (datetime.now() - timedelta(days=45)).isoformat(sep=" ", timespec="seconds")

        with runtime.exception_records_lock:
            runtime.exception_records.extend(
                [
                    {
                        "asset_id": "AS-9701",
                        "action_type": ActionType.BORROW.value,
                        "user_id": "U-9701",
                        "user_name": "Alice",
                        "code": ConfirmResult.ACK_TIMEOUT.value,
                        "message": "recent ack timeout",
                        "event_time": recent_time,
                        "request_seq": 201,
                        "hw_seq": None,
                        "hw_result": None,
                    },
                    {
                        "asset_id": "AS-9702",
                        "action_type": ActionType.RETURN.value,
                        "user_id": "U-9702",
                        "user_name": "Bob",
                        "code": ConfirmResult.INTERNAL_ERROR.value,
                        "message": "old internal error",
                        "event_time": old_time,
                        "request_seq": 202,
                        "hw_seq": None,
                        "hw_result": None,
                    },
                ]
            )

        app = create_app(runtime)
        with TestClient(app) as client:
            response = client.get("/export/exceptions.csv?time_range=7d")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.headers["content-type"].startswith("text/csv"))
        csv_text = self.decode_csv_body(response)
        self.assertIn("AS-9701,BORROW,U-9701,Alice,ACK_TIMEOUT,recent ack timeout", csv_text)
        self.assertNotIn("AS-9702,RETURN,U-9702,Bob,INTERNAL_ERROR,old internal error", csv_text)

    def test_export_dashboard_json_is_accessible(self) -> None:
        repository = InMemoryTransactionRepository(
            initial_assets={"AS-1001": AssetStatus.IN_STOCK, "AS-1002": AssetStatus.BORROWED}
        )
        repository.records.append(
            self.build_operation_record(
                asset_id="AS-1002",
                user_id="U-1001",
                action_type=ActionType.BORROW,
                request_seq=1,
            )
        )
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), repository=repository)
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.get("/export/dashboard.json?time_range=all")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.headers["content-type"].startswith("application/json"))
        self.assertIn("dashboard_report.json", response.headers.get("content-disposition", ""))
        payload = response.json()
        self.assertEqual(payload["filters"]["time_range"], "all")
        self.assertEqual(payload["summary"]["in_stock"], 1)
        self.assertEqual(payload["summary"]["borrowed"], 1)
        self.assertEqual(payload["operation_stats"]["borrow_count"], 1)
        self.assertEqual(payload["borrow_top_assets"], [{"asset_id": "AS-1002", "count": 1}])

    def test_scan_result_returns_asset_not_found_contract(self) -> None:
        runtime = self.build_runtime(
            serial_manager=FakeSerialManager(),
            initial_assets={"AS-1001": AssetStatus.IN_STOCK},
        )
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.post("/scan/result", json={"asset_id": "AS-404"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["success"], False)
        self.assertEqual(payload["code"], "ASSET_NOT_FOUND")
        self.assertTrue(payload["message"])
        self.assertEqual(payload["asset_id"], "AS-404")
        self.assertEqual(
            payload["extra"],
            {
                "exists": False,
                "asset_status": None,
                "device_status": DeviceStatus.ONLINE.value,
            },
        )

    def test_inbound_api_success_with_confirmed_creates_asset_and_record(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={})
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
            response = client.post(
                "/transactions/inbound",
                json={
                    "asset_id": "AS-8001",
                    "user_id": "U-ADMIN",
                    "user_name": "管理员",
                    "asset_name": "ThinkPad X1",
                    "category_id": 1,
                    "location": "Cabinet A",
                    "raw_text": "AS-8001",
                    "symbology": "QR_CODE",
                    "timeout_ms": 300,
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["success"])
        self.assertEqual(payload["code"], ConfirmResult.CONFIRMED.value)
        self.assertEqual(payload["action_type"], ActionType.INBOUND.value)
        self.assertEqual(payload["transaction_state"], "COMPLETED")
        self.assertEqual(repository.assets["AS-8001"], AssetStatus.IN_STOCK)
        self.assertEqual(repository.asset_details["AS-8001"]["asset_name"], "ThinkPad X1")
        self.assertEqual(repository.asset_details["AS-8001"]["category_id"], 1)
        self.assertEqual(repository.asset_details["AS-8001"]["location"], "Cabinet A")
        self.assertEqual(repository.records[0].action_type, ActionType.INBOUND)
        self.assertEqual(repository.records[0].hw_result, ConfirmResult.CONFIRMED.value)

    def test_inbound_api_param_invalid_when_asset_name_blank(self) -> None:
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), initial_assets={})
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.post(
                "/transactions/inbound",
                json={
                    "asset_id": "AS-8002",
                    "user_id": "U-ADMIN",
                    "user_name": "管理员",
                    "asset_name": "",
                    "category_id": 1,
                    "location": "Cabinet B",
                    "timeout_ms": 300,
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["success"])
        self.assertEqual(payload["code"], ConfirmResult.PARAM_INVALID.value)

    def test_inbound_api_permission_denied_for_non_admin(self) -> None:
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), initial_assets={})
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.post(
                "/transactions/inbound",
                json={
                    "asset_id": "AS-8003",
                    "user_id": "U-1001",
                    "user_name": "普通用户",
                    "asset_name": "Dell Dock",
                    "category_id": 1,
                    "location": "Desk 1",
                    "timeout_ms": 300,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["code"], ConfirmResult.PERMISSION_DENIED.value)

    def test_inbound_api_offline_and_busy_failures(self) -> None:
        offline_runtime = self.build_runtime(
            serial_manager=FakeSerialManager(open_status=DeviceStatus.OFFLINE),
            initial_assets={},
        )
        offline_app = create_app(offline_runtime)
        with TestClient(offline_app) as client:
            offline_response = client.post(
                "/transactions/inbound",
                json={
                    "asset_id": "AS-8004",
                    "user_id": "U-ADMIN",
                    "user_name": "管理员",
                    "asset_name": "USB Hub",
                    "category_id": 1,
                    "location": "Shelf A",
                    "timeout_ms": 300,
                },
            )

        busy_runtime = self.build_runtime(serial_manager=FakeSerialManager(), initial_assets={})
        busy_runtime.service.transaction_manager.create_transaction(
            asset_id="AS-8005",
            user_id="U-ADMIN",
            user_name="管理员",
            action_type=ActionType.INBOUND,
            request_id="req-busy",
            request_seq=999,
        )
        busy_app = create_app(busy_runtime)
        with TestClient(busy_app) as client:
            busy_response = client.post(
                "/transactions/inbound",
                json={
                    "asset_id": "AS-8005",
                    "user_id": "U-ADMIN",
                    "user_name": "管理员",
                    "asset_name": "USB Hub",
                    "category_id": 1,
                    "location": "Shelf B",
                    "timeout_ms": 300,
                },
            )

        self.assertEqual(offline_response.status_code, 200)
        self.assertEqual(offline_response.json()["code"], ConfirmResult.DEVICE_OFFLINE.value)
        self.assertEqual(busy_response.status_code, 200)
        self.assertEqual(busy_response.json()["code"], ConfirmResult.BUSY.value)

    def test_inbound_api_ack_failures_map_to_business_result_codes(self) -> None:
        cases = [
            (MsgType.ACK_BUSY, "DEVICE_BUSY", ConfirmResult.BUSY.value),
            (MsgType.ACK_INVALID, "INVALID_REQUEST", ConfirmResult.ACK_INVALID.value),
            (MsgType.ACK_ERROR, "CRC_CHECK_FAIL", ConfirmResult.ACK_ERROR.value),
            (None, "ACK timeout after 3 retries", ConfirmResult.ACK_TIMEOUT.value),
        ]

        for ack_type, message, expected_code in cases:
            with self.subTest(expected_code=expected_code):
                runtime = self.build_runtime(
                    serial_manager=FakeSerialManager(
                        send_result=SendResult(
                            success=False,
                            seq_id=100,
                            ack_type=ack_type,
                            message=message,
                            ack_payload=None if ack_type is None else {"detail": message},
                        )
                    ),
                    initial_assets={},
                )
                app = create_app(runtime)
                with TestClient(app) as client:
                    response = client.post(
                        "/transactions/inbound",
                        json={
                            "asset_id": f"AS-{expected_code}",
                            "user_id": "U-ADMIN",
                            "user_name": "管理员",
                            "asset_name": "Adapter",
                            "category_id": 1,
                            "location": "Rack X",
                            "timeout_ms": 300,
                        },
                    )

                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["code"], expected_code)

    def test_inbound_api_hw_failures_and_commit_rollback(self) -> None:
        for confirm_result in (ConfirmResult.CANCELLED.value, ConfirmResult.TIMEOUT.value):
            with self.subTest(confirm_result=confirm_result):
                runtime = self.build_runtime(
                    serial_manager=FakeSerialManager(
                        response_factory=lambda payload, seq_id, confirm_result=confirm_result: self.build_event_frame(
                            payload,
                            confirm_result=confirm_result,
                        )
                    ),
                    initial_assets={},
                )
                app = create_app(runtime)
                with TestClient(app) as client:
                    response = client.post(
                        "/transactions/inbound",
                        json={
                            "asset_id": f"AS-HW-{confirm_result}",
                            "user_id": "U-ADMIN",
                            "user_name": "管理员",
                            "asset_name": "Keyboard",
                            "category_id": 1,
                            "location": "Rack Y",
                            "timeout_ms": 300,
                        },
                    )

                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["code"], confirm_result)

        repository = FailingCommitRepository(asset_id="AS-8010", initial_status=None, failure=RuntimeError("db down"))
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
            rollback_response = client.post(
                "/transactions/inbound",
                json={
                    "asset_id": "AS-8010",
                    "user_id": "U-ADMIN",
                    "user_name": "管理员",
                    "asset_name": "Mouse",
                    "category_id": 1,
                    "location": "Rack Z",
                    "timeout_ms": 300,
                },
            )

        self.assertEqual(rollback_response.status_code, 200)
        self.assertEqual(rollback_response.json()["code"], ConfirmResult.INTERNAL_ERROR.value)
        self.assertEqual(repository.rollback_calls, [("AS-8010", "db down")])

    def test_inbound_success_updates_websocket_assets_records_asset_changes_and_dashboard(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-EXISTING": AssetStatus.IN_STOCK})
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
            dashboard_before = client.get("/dashboard?time_range=all").json()
            with client.websocket_connect("/ws/status") as websocket:
                response = client.post(
                    "/transactions/inbound",
                    json={
                        "asset_id": "AS-8020",
                        "user_id": "U-ADMIN",
                        "user_name": "管理员",
                        "asset_name": "Surface Pro",
                        "category_id": 1,
                        "location": "Cabinet C",
                        "raw_text": "AS-8020",
                        "timeout_ms": 300,
                    },
                )
                messages = [websocket.receive_json(), websocket.receive_json(), websocket.receive_json()]

            assets_payload = client.get("/assets").json()
            records_payload = client.get("/records?action_type=INBOUND&time_range=all").json()
            asset_changes_payload = client.get("/asset-changes?action_type=INBOUND&time_range=all").json()
            dashboard_after = client.get("/dashboard?time_range=all").json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual([message["code"] for message in messages], ["WAITING_ACK", "WAITING_HW", "CONFIRMED"])
        self.assertTrue(response.json()["success"])
        self.assertEqual(response.json()["action_type"], ActionType.INBOUND.value)
        self.assertEqual(assets_payload["AS-8020"], AssetStatus.IN_STOCK.value)
        self.assertEqual(records_payload["total"], 1)
        self.assertEqual(records_payload["items"][0]["action_type"], ActionType.INBOUND.value)
        self.assertEqual(records_payload["items"][0]["asset_id"], "AS-8020")
        self.assertEqual(asset_changes_payload["total"], 1)
        self.assertEqual(asset_changes_payload["items"][0]["from_status"], "未建档")
        self.assertEqual(asset_changes_payload["items"][0]["to_status"], AssetStatus.IN_STOCK.value)
        self.assertIn(ActionType.INBOUND.value, records_payload["available_filters"]["action_types"])
        self.assertIn(ActionType.INBOUND.value, asset_changes_payload["available_filters"]["action_types"])
        self.assertEqual(dashboard_before["summary"]["in_stock"], 1)
        self.assertEqual(dashboard_after["summary"]["in_stock"], 2)

    def test_inbound_failures_are_visible_in_exceptions_feed_with_inbound_filter(self) -> None:
        runtime = self.build_runtime(serial_manager=FakeSerialManager(), initial_assets={})
        app = create_app(runtime)

        with TestClient(app) as client:
            response = client.post(
                "/transactions/inbound",
                json={
                    "asset_id": "AS-8021",
                    "user_id": "U-1001",
                    "user_name": "普通用户",
                    "asset_name": "Mini PC",
                    "category_id": 1,
                    "location": "Cabinet D",
                    "timeout_ms": 300,
                },
            )
            exceptions_payload = client.get(
                "/exceptions?action_type=INBOUND&code=PERMISSION_DENIED&time_range=all"
            ).json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["code"], ConfirmResult.PERMISSION_DENIED.value)
        self.assertEqual(exceptions_payload["total"], 1)
        self.assertEqual(exceptions_payload["items"][0]["action_type"], ActionType.INBOUND.value)
        self.assertEqual(exceptions_payload["items"][0]["code"], ConfirmResult.PERMISSION_DENIED.value)
        self.assertIn(ActionType.INBOUND.value, exceptions_payload["available_filters"]["action_types"])

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

    def test_scan_latest_prefill_value_can_inbound_with_mock_mcu_confirmed(self) -> None:
        mock_server = MockMCUServer(host="127.0.0.1", port=9303, mode="confirmed", confirm_delay=0.05)
        mock_server.start()
        time.sleep(0.2)
        runtime = self.build_runtime(
            serial_manager=SerialManager(port="socket://127.0.0.1:9303", ack_timeout=0.1, max_retries=3),
            initial_assets={},
        )
        app = create_app(runtime)

        try:
            with TestClient(app) as client:
                scan_response = client.post(
                    "/scan/result",
                    json={
                        "asset_id": "AS-9020",
                        "raw_text": "AS-9020",
                        "symbology": "QR",
                        "source_id": "webcam-0",
                        "frame_time": 1700009020,
                    },
                )
                latest_payload = client.get("/scan/latest").json()
                inbound_response = client.post(
                    "/transactions/inbound",
                    json={
                        "asset_id": latest_payload["asset_id"],
                        "user_id": "U-ADMIN",
                        "user_name": "管理员",
                        "asset_name": "扫码入库资产",
                        "category_id": 1,
                        "location": "Cabinet Z",
                        "raw_text": latest_payload["raw_text"],
                        "symbology": latest_payload["symbology"],
                        "timeout_ms": 300,
                    },
                )
                assets_payload = client.get("/assets").json()
                records_payload = client.get("/records?action_type=INBOUND&asset_id=AS-9020&time_range=all").json()
                asset_changes_payload = client.get(
                    "/asset-changes?action_type=INBOUND&asset_id=AS-9020&time_range=all"
                ).json()
                dashboard_payload = client.get("/dashboard?time_range=all").json()
        finally:
            mock_server.stop()

        self.assertEqual(scan_response.status_code, 200)
        self.assertEqual(scan_response.json()["code"], "ASSET_NOT_FOUND")
        self.assertEqual(latest_payload["asset_id"], "AS-9020")
        self.assertEqual(inbound_response.status_code, 200)
        inbound_payload = inbound_response.json()
        self.assertTrue(inbound_payload["success"])
        self.assertEqual(inbound_payload["code"], ConfirmResult.CONFIRMED.value)
        self.assertEqual(inbound_payload["hw_result"], ConfirmResult.CONFIRMED.value)
        self.assertEqual(inbound_payload["transaction_state"], "COMPLETED")
        self.assertEqual(assets_payload["AS-9020"], AssetStatus.IN_STOCK.value)
        self.assertEqual(records_payload["total"], 1)
        self.assertEqual(records_payload["items"][0]["asset_id"], "AS-9020")
        self.assertEqual(asset_changes_payload["total"], 1)
        self.assertEqual(dashboard_payload["summary"]["in_stock"], 1)

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
