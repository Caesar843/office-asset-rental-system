from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Callable
from unittest.mock import patch

from api_app import build_default_runtime
from db_repository import SQLiteTransactionRepository
from models import (
    ActionType,
    AssetStatus,
    BorrowRequestCreateInput,
    BorrowRequestReviewInput,
    BorrowRequestStatus,
    ConfirmResult,
    InboundCommitInput,
    OperationRecordInput,
)
from protocol import Frame, MsgType
from repository import InMemoryTransactionRepository
from serial_manager import SendResult
from service import AssetConfirmService


class FakeSerialManager:
    def __init__(
        self,
        *,
        send_result: SendResult | None = None,
        response_factory: Callable[[dict[str, object], int], Frame | None] | None = None,
        response_delay: float = 0.01,
    ) -> None:
        self._send_result = send_result
        self._response_factory = response_factory
        self._response_delay = response_delay
        self._next_seq = 100
        self._frame_handler: Callable[[Frame], None] | None = None
        self._status_handler = None

    def set_frame_handler(self, handler: Callable[[Frame], None]) -> None:
        self._frame_handler = handler

    def set_status_handler(self, handler) -> None:
        self._status_handler = handler

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

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


class FailingSQLiteTransactionRepository(SQLiteTransactionRepository):
    def _insert_operation_record(
        self,
        cursor,
        *,
        operation_id: int,
        asset_db_id: int,
        user_db_id: int,
        record: OperationRecordInput,
        insert_context=None,
    ) -> None:
        raise RuntimeError("record insert failed")


class SQLiteTransactionRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        handle, path = tempfile.mkstemp(dir=str(Path.cwd()), suffix=".sqlite3")
        os.close(handle)
        os.unlink(path)
        self.db_path = path
        self._create_schema(self.db_path)
        self._seed_data(self.db_path)

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _create_schema(self, db_path: str) -> None:
        connection = sqlite3.connect(db_path)
        try:
            connection.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE categories (
                    id INTEGER PRIMARY KEY,
                    cat_name TEXT,
                    description TEXT
                );

                CREATE TABLE assets (
                    id INTEGER PRIMARY KEY,
                    asset_name TEXT,
                    category_id INTEGER,
                    qr_code TEXT,
                    status INTEGER,
                    location TEXT,
                    FOREIGN KEY (category_id) REFERENCES categories(id)
                );

                CREATE TABLE users (
                    user_id INTEGER PRIMARY KEY,
                    user_name TEXT,
                    student_id TEXT,
                    credit_score INTEGER DEFAULT 100,
                    status INTEGER
                );

                CREATE TABLE operation_records (
                    op_id INTEGER PRIMARY KEY,
                    asset_id INTEGER,
                    user_id INTEGER,
                    op_type TEXT,
                    op_time TEXT,
                    hw_seq TEXT,
                    hw_result TEXT,
                    due_time TEXT,
                    FOREIGN KEY (asset_id) REFERENCES assets(id),
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );
                """
            )
            connection.commit()
        finally:
            connection.close()

    def _seed_data(self, db_path: str) -> None:
        connection = sqlite3.connect(db_path)
        try:
            connection.executemany(
                "INSERT INTO categories (id, cat_name, description) VALUES (?, ?, ?)",
                [(1, "Laptop", "Portable computer")],
            )
            connection.executemany(
                "INSERT INTO assets (id, asset_name, category_id, qr_code, status, location) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (1, "ThinkPad X1", 1, "AS-5001", 0, "Cabinet A"),
                    (2, "MacBook Pro", 1, "AS-5002", 1, "Cabinet B"),
                ],
            )
            connection.executemany(
                "INSERT INTO users (user_id, user_name, student_id, credit_score, status) VALUES (?, ?, ?, ?, ?)",
                [
                    (1001, "Alice", "U-5001", 100, 1),
                    (1002, "Bob", "U-5002", 100, 1),
                ],
            )
            connection.commit()
        finally:
            connection.close()

    def _build_record(self, *, asset_id: str, user_id: str, action_type: ActionType) -> OperationRecordInput:
        return OperationRecordInput(
            asset_id=asset_id,
            user_id=user_id,
            user_name="Tester",
            action_type=action_type,
            request_seq=501,
            request_id="req-5001",
            hw_seq=0x80000021,
            hw_result=ConfirmResult.CONFIRMED.value,
            hw_sn="STM32F103-A23",
            due_time=None,
        )

    def _build_borrow_request(
        self,
        *,
        request_id: str = "br-5001",
        asset_id: str = "AS-5001",
        user_id: str = "U-5001",
        status: BorrowRequestStatus = BorrowRequestStatus.PENDING,
    ) -> BorrowRequestCreateInput:
        return BorrowRequestCreateInput(
            request_id=request_id,
            asset_id=asset_id,
            applicant_user_id=user_id,
            applicant_user_name="Tester",
            reason="Need laptop",
            status=status,
            requested_at="2026-04-15 09:00:00",
        )

    def _build_inbound_commit(self, *, asset_id: str, user_id: str, category_id: int | None = 1) -> InboundCommitInput:
        return InboundCommitInput(
            asset_id=asset_id,
            asset_name="New Laptop",
            category_id=category_id,
            location="Inbound Shelf",
            user_id=user_id,
            user_name="管理员",
            request_seq=601,
            request_id="req-6001",
            hw_seq=0x80000041,
            hw_result=ConfirmResult.CONFIRMED.value,
            hw_sn="STM32F103-A23",
            op_time="2026-04-15 11:00:00",
        )

    def _fetch_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute(sql, params).fetchone()
            return row
        finally:
            connection.close()

    def test_sqlite_repository_borrow_commit_updates_asset_and_record(self) -> None:
        repository = SQLiteTransactionRepository(self.db_path)

        new_status = repository.apply_operation_atomically(
            self._build_record(asset_id="AS-5001", user_id="U-5001", action_type=ActionType.BORROW)
        )

        self.assertEqual(new_status, AssetStatus.BORROWED)
        asset_row = self._fetch_one("SELECT status FROM assets WHERE qr_code = ?", ("AS-5001",))
        record_row = self._fetch_one(
            "SELECT asset_id, user_id, op_type, hw_seq, hw_result, due_time FROM operation_records WHERE op_id = 1"
        )
        self.assertEqual(asset_row["status"], 1)
        self.assertEqual(record_row["asset_id"], 1)
        self.assertEqual(record_row["user_id"], 1001)
        self.assertEqual(record_row["op_type"], ActionType.BORROW.value)
        self.assertEqual(record_row["hw_seq"], str(0x80000021))
        self.assertEqual(record_row["hw_result"], ConfirmResult.CONFIRMED.value)
        self.assertIsNone(record_row["due_time"])

    def test_sqlite_repository_return_commit_restores_in_stock(self) -> None:
        repository = SQLiteTransactionRepository(self.db_path)

        new_status = repository.apply_operation_atomically(
            self._build_record(asset_id="AS-5002", user_id="U-5002", action_type=ActionType.RETURN)
        )

        self.assertEqual(new_status, AssetStatus.IN_STOCK)
        asset_row = self._fetch_one("SELECT status FROM assets WHERE qr_code = ?", ("AS-5002",))
        record_row = self._fetch_one("SELECT op_type FROM operation_records WHERE op_id = 1")
        self.assertEqual(asset_row["status"], 0)
        self.assertEqual(record_row["op_type"], ActionType.RETURN.value)

    def test_sqlite_repository_probe_reports_schema_and_sequence_strategy(self) -> None:
        repository = SQLiteTransactionRepository(self.db_path)

        probe = repository.probe()

        self.assertTrue(probe.ready)
        self.assertEqual(probe.status, "ok")
        self.assertEqual(probe.backend, "sqlite")
        self.assertEqual(
            probe.details["tables_present"],
            ["assets", "users", "categories", "operation_records", "borrow_requests"],
        )
        self.assertEqual(probe.details["missing_tables"], [])
        self.assertEqual(probe.details["operation_record_id_strategy"], "sqlite_manual_max_plus_one")
        self.assertTrue(probe.details["foreign_keys_enabled"])

    def test_sqlite_repository_invalid_state_does_not_write_dirty_data(self) -> None:
        repository = SQLiteTransactionRepository(self.db_path)

        with self.assertRaises(ValueError):
            repository.apply_operation_atomically(
                self._build_record(asset_id="AS-5002", user_id="U-5002", action_type=ActionType.BORROW)
            )

        asset_row = self._fetch_one("SELECT status FROM assets WHERE qr_code = ?", ("AS-5002",))
        record_count = self._fetch_one("SELECT COUNT(*) AS count FROM operation_records")
        self.assertEqual(asset_row["status"], 1)
        self.assertEqual(record_count["count"], 0)

    def test_sqlite_repository_rolls_back_when_record_insert_fails(self) -> None:
        repository = FailingSQLiteTransactionRepository(self.db_path)

        with self.assertRaises(RuntimeError):
            repository.apply_operation_atomically(
                self._build_record(asset_id="AS-5001", user_id="U-5001", action_type=ActionType.BORROW)
            )

        asset_row = self._fetch_one("SELECT status FROM assets WHERE qr_code = ?", ("AS-5001",))
        record_count = self._fetch_one("SELECT COUNT(*) AS count FROM operation_records")
        self.assertEqual(asset_row["status"], 0)
        self.assertEqual(record_count["count"], 0)

    def test_sqlite_repository_inbound_commit_creates_asset_and_record(self) -> None:
        repository = SQLiteTransactionRepository(self.db_path)

        new_status = repository.apply_inbound_atomically(
            self._build_inbound_commit(asset_id="AS-6001", user_id="U-5001")
        )

        self.assertEqual(new_status, AssetStatus.IN_STOCK)
        asset_row = self._fetch_one(
            "SELECT asset_name, category_id, qr_code, status, location FROM assets WHERE qr_code = ?",
            ("AS-6001",),
        )
        record_row = self._fetch_one(
            "SELECT op_type, hw_seq, hw_result, due_time FROM operation_records WHERE op_id = 1"
        )
        self.assertEqual(asset_row["asset_name"], "New Laptop")
        self.assertEqual(asset_row["category_id"], 1)
        self.assertEqual(asset_row["status"], 0)
        self.assertEqual(asset_row["location"], "Inbound Shelf")
        self.assertEqual(record_row["op_type"], ActionType.INBOUND.value)
        self.assertEqual(record_row["hw_seq"], str(0x80000041))
        self.assertEqual(record_row["hw_result"], ConfirmResult.CONFIRMED.value)
        self.assertIsNone(record_row["due_time"])

    def test_sqlite_repository_inbound_rolls_back_when_record_insert_fails(self) -> None:
        repository = FailingSQLiteTransactionRepository(self.db_path)

        with self.assertRaises(RuntimeError):
            repository.apply_inbound_atomically(
                self._build_inbound_commit(asset_id="AS-6002", user_id="U-5001")
            )

        asset_row = self._fetch_one("SELECT qr_code FROM assets WHERE qr_code = ?", ("AS-6002",))
        record_count = self._fetch_one("SELECT COUNT(*) AS count FROM operation_records")
        self.assertIsNone(asset_row)
        self.assertEqual(record_count["count"], 0)

    def test_service_can_commit_to_sqlite_repository(self) -> None:
        repository = SQLiteTransactionRepository(self.db_path)
        serial_manager = FakeSerialManager(
            response_factory=lambda payload, seq_id: Frame.build(
                MsgType.EVT_USER_ACTION,
                seq_id=0x80000031,
                payload={
                    "asset_id": payload["asset_id"],
                    "request_seq": payload["request_seq"],
                    "request_id": payload["request_id"],
                    "action_type": payload["action_type"],
                    "confirm_result": ConfirmResult.CONFIRMED.value,
                    "hw_sn": "STM32F103-A23",
                },
            )
        )
        service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

        result = service.request_asset_borrow_confirm(
            asset_id="AS-5001",
            user_id="U-5001",
            user_name="Alice",
            timeout_ms=100,
        )

        self.assertTrue(result.success)
        self.assertEqual(result.code, ConfirmResult.CONFIRMED.value)
        asset_row = self._fetch_one("SELECT status FROM assets WHERE qr_code = ?", ("AS-5001",))
        record_row = self._fetch_one("SELECT hw_result FROM operation_records WHERE op_id = 1")
        self.assertEqual(asset_row["status"], 1)
        self.assertEqual(record_row["hw_result"], ConfirmResult.CONFIRMED.value)

    def test_build_default_runtime_uses_sqlite_repository_when_configured(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "BACKEND_REPOSITORY_KIND": "sqlite",
                "BACKEND_SQLITE_PATH": self.db_path,
            },
            clear=False,
        ):
            runtime = build_default_runtime()

        self.assertIsInstance(runtime.repository, SQLiteTransactionRepository)
        self.assertEqual(runtime.requested_repository_mode, "sqlite")
        self.assertEqual(runtime.repository_mode, "sqlite")
        self.assertFalse(runtime.repository_fallback)
        self.assertTrue(runtime.repository_ready)
        self.assertEqual(runtime.repository_status, "ok")
        self.assertEqual(runtime.repository_details["backend"], "sqlite")

    def test_build_default_runtime_falls_back_to_inmemory_when_mysql_is_unavailable(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "BACKEND_REPOSITORY_KIND": "mysql",
                "BACKEND_DB_HOST": "127.0.0.1",
                "BACKEND_DB_PORT": "3306",
                "BACKEND_DB_USER": "root",
                "BACKEND_DB_PASSWORD": "secret",
                "BACKEND_DB_NAME": "asset_system",
            },
            clear=False,
        ):
            runtime = build_default_runtime()

        self.assertIsInstance(runtime.repository, InMemoryTransactionRepository)
        self.assertEqual(runtime.requested_repository_mode, "mysql")
        self.assertEqual(runtime.repository_mode, "inmemory")
        self.assertTrue(runtime.repository_fallback)
        self.assertTrue(runtime.repository_ready)
        self.assertEqual(runtime.repository_status, "fallback")
        self.assertIsNotNone(runtime.startup_error)
        self.assertIn("fallback to in-memory", runtime.startup_error)
        self.assertEqual(runtime.repository_details["backend"], "inmemory")
        self.assertEqual(runtime.repository_details["details"]["requested_mode"], "mysql")


if __name__ == "__main__":
    unittest.main()
