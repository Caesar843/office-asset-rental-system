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
    AcceptanceResult,
    ActionType,
    AssetStatus,
    BorrowRequestCreateInput,
    BorrowRequestReviewInput,
    BorrowRequestStatus,
    ConfirmResult,
    InboundCommand,
    InboundCommitInput,
    OperationRecordInput,
    TransactionState,
    ReturnAcceptanceCreateInput,
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
        self.sent_requests: list[tuple[MsgType, dict[str, object], int]] = []

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
        self.sent_requests.append((msg_type, payload, actual_seq_id))
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
                    user_name TEXT,
                    request_seq INTEGER,
                    request_id TEXT,
                    hw_sn TEXT,
                    borrow_request_id TEXT,
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
                    (2001, "Admin Alias", "ADMIN", 100, 1),
                    (2002, "Demo Admin", "U-ADMIN", 100, 1),
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

    def _build_return_acceptance(self, *, asset_id: str) -> ReturnAcceptanceCreateInput:
        return ReturnAcceptanceCreateInput(
            asset_id=asset_id,
            acceptance_result=AcceptanceResult.NORMAL,
            note="checked",
            accepted_by_user_id="U-ADMIN",
            accepted_by_user_name="Admin",
            accepted_at="2026-04-15 12:00:00",
            related_return_request_seq=501,
            related_return_request_id="req-5001",
            related_return_hw_seq=0x80000021,
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
            requested_days=7,
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

    def test_formal_schema_operation_records_contains_required_trace_columns(self) -> None:
        schema_path = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"
        schema_text = schema_path.read_text(encoding="utf-8").lower()

        self.assertIn("create table asset_system.operation_records", schema_text)
        for column_name in ("request_seq", "request_id", "hw_seq", "hw_result", "hw_sn"):
            self.assertIn(column_name, schema_text)

    def test_sqlite_repository_borrow_commit_updates_asset_and_record(self) -> None:
        repository = SQLiteTransactionRepository(self.db_path)
        record = self._build_record(asset_id="AS-5001", user_id="U-5001", action_type=ActionType.BORROW)
        record.due_time = "2026-04-22 09:00:00"

        new_status = repository.apply_operation_atomically(record)

        self.assertEqual(new_status, AssetStatus.BORROWED)
        asset_row = self._fetch_one("SELECT status FROM assets WHERE qr_code = ?", ("AS-5001",))
        record_row = self._fetch_one(
            """
            SELECT asset_id, user_id, op_type, request_seq, request_id, hw_seq, hw_result, hw_sn, due_time
            FROM operation_records
            WHERE op_id = 1
            """
        )
        self.assertEqual(asset_row["status"], 1)
        self.assertEqual(record_row["asset_id"], 1)
        self.assertEqual(record_row["user_id"], 1001)
        self.assertEqual(record_row["op_type"], ActionType.BORROW.value)
        self.assertEqual(record_row["request_seq"], 501)
        self.assertEqual(record_row["request_id"], "req-5001")
        self.assertEqual(record_row["hw_seq"], str(0x80000021))
        self.assertEqual(record_row["hw_result"], ConfirmResult.CONFIRMED.value)
        self.assertEqual(record_row["hw_sn"], "STM32F103-A23")
        self.assertEqual(record_row["due_time"], "2026-04-22 09:00:00")

    def test_sqlite_repository_can_create_and_list_borrow_requests_with_requested_days(self) -> None:
        repository = SQLiteTransactionRepository(self.db_path)

        created = repository.create_borrow_request(self._build_borrow_request())
        listed = repository.list_borrow_requests(asset_id="AS-5001")
        stored = repository.get_borrow_request(created.request_id)

        self.assertEqual(created.requested_days, 7)
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0].requested_days, 7)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.requested_days, 7)

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

    def test_sqlite_repository_can_create_list_and_trace_return_acceptances(self) -> None:
        repository = SQLiteTransactionRepository(self.db_path)
        repository.apply_operation_atomically(
            self._build_record(asset_id="AS-5002", user_id="U-5002", action_type=ActionType.RETURN)
        )

        latest = repository.get_latest_operation_record("AS-5002")
        created = repository.create_return_acceptance(
            self._build_return_acceptance(asset_id="AS-5002")
        )
        listed = repository.list_return_acceptances(asset_id="AS-5002")
        found = repository.get_return_acceptance_by_related_return(
            asset_id="AS-5002",
            related_return_request_seq=501,
            related_return_hw_seq=0x80000021,
        )

        self.assertIsNotNone(latest)
        self.assertEqual(latest.action_type, ActionType.RETURN)
        self.assertEqual(latest.asset_id, "AS-5002")
        self.assertEqual(latest.user_id, "U-5002")
        self.assertEqual(latest.request_seq, 501)
        self.assertEqual(latest.request_id, "req-5001")
        self.assertEqual(latest.hw_seq, 0x80000021)
        self.assertEqual(created.acceptance_result, AcceptanceResult.NORMAL)
        self.assertEqual(len(listed), 1)
        self.assertIsNotNone(found)
        self.assertEqual(found.id, created.id)

    def test_sqlite_repository_numeric_like_asset_id_does_not_fallback_to_internal_asset_pk(self) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute(
                "INSERT INTO assets (id, asset_name, category_id, qr_code, status, location) VALUES (?, ?, ?, ?, ?, ?)",
                (123, "Legacy Asset", 1, "AS-PK-123", 0, "Cabinet C"),
            )
            connection.commit()
        finally:
            connection.close()

        repository = SQLiteTransactionRepository(self.db_path)

        self.assertIsNone(repository.get_asset_status("123"))

        with self.assertRaises(LookupError):
            repository.apply_operation_atomically(
                self._build_record(asset_id="123", user_id="U-5001", action_type=ActionType.BORROW)
            )

        new_status = repository.apply_inbound_atomically(self._build_inbound_commit(asset_id="123", user_id="U-5001"))

        self.assertEqual(new_status, AssetStatus.IN_STOCK)
        asset_row = self._fetch_one("SELECT id, qr_code, status FROM assets WHERE qr_code = ?", ("123",))
        self.assertIsNotNone(asset_row)
        self.assertNotEqual(asset_row["id"], 123)
        self.assertEqual(asset_row["qr_code"], "123")
        self.assertEqual(asset_row["status"], 0)

        latest = repository.get_latest_operation_record("123")
        self.assertIsNotNone(latest)
        self.assertEqual(latest.asset_id, "123")
        self.assertEqual(latest.user_id, "U-5001")

    def test_sqlite_repository_numeric_like_user_id_does_not_fallback_to_internal_user_pk(self) -> None:
        repository = SQLiteTransactionRepository(self.db_path)

        with self.assertRaisesRegex(RuntimeError, "user not found: 1001"):
            repository.apply_operation_atomically(
                self._build_record(asset_id="AS-5001", user_id="1001", action_type=ActionType.BORROW)
            )

        asset_row = self._fetch_one("SELECT status FROM assets WHERE qr_code = ?", ("AS-5001",))
        record_count = self._fetch_one("SELECT COUNT(*) AS count FROM operation_records")
        self.assertEqual(asset_row["status"], 0)
        self.assertEqual(record_count["count"], 0)

    def test_sqlite_repository_rejects_duplicate_return_acceptance_for_same_return(self) -> None:
        repository = SQLiteTransactionRepository(self.db_path)
        repository.apply_operation_atomically(
            self._build_record(asset_id="AS-5002", user_id="U-5002", action_type=ActionType.RETURN)
        )
        repository.create_return_acceptance(self._build_return_acceptance(asset_id="AS-5002"))

        with self.assertRaises(Exception):
            repository.create_return_acceptance(self._build_return_acceptance(asset_id="AS-5002"))

    def test_sqlite_repository_probe_reports_schema_and_sequence_strategy(self) -> None:
        repository = SQLiteTransactionRepository(self.db_path)

        probe = repository.probe()

        self.assertTrue(probe.ready)
        self.assertEqual(probe.status, "ok")
        self.assertEqual(probe.backend, "sqlite")
        self.assertEqual(
            probe.details["tables_present"],
            ["assets", "users", "categories", "operation_records", "borrow_requests", "return_acceptance_records"],
        )
        self.assertEqual(probe.details["missing_tables"], [])
        self.assertEqual(probe.details["missing_operation_record_trace_columns"], [])
        self.assertEqual(probe.details["missing_inbound_asset_columns"], [])
        self.assertEqual(probe.details["missing_business_user_columns"], [])
        self.assertEqual(probe.details["missing_inbound_admin_user_ids"], [])
        self.assertGreaterEqual(probe.details["resolvable_business_user_count"], 1)
        self.assertEqual(probe.details["operation_record_id_strategy"], "sqlite_manual_max_plus_one")
        self.assertTrue(probe.details["foreign_keys_enabled"])

    def test_sqlite_repository_probe_reports_missing_inbound_asset_columns(self) -> None:
        handle, legacy_db_path = tempfile.mkstemp(dir=str(Path.cwd()), suffix=".sqlite3")
        os.close(handle)
        os.unlink(legacy_db_path)

        try:
            connection = sqlite3.connect(legacy_db_path)
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
                        user_name TEXT,
                        request_seq INTEGER,
                        request_id TEXT,
                        hw_sn TEXT,
                        borrow_request_id TEXT,
                        FOREIGN KEY (asset_id) REFERENCES assets(id),
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    );
                    """
                )
                connection.executemany(
                    "INSERT INTO categories (id, cat_name, description) VALUES (?, ?, ?)",
                    [(1, "Laptop", "Portable computer")],
                )
                connection.executemany(
                    "INSERT INTO users (user_id, user_name, student_id, credit_score, status) VALUES (?, ?, ?, ?, ?)",
                    [
                        (2001, "Admin Alias", "ADMIN", 100, 1),
                        (2002, "Demo Admin", "U-ADMIN", 100, 1),
                    ],
                )
                connection.commit()
            finally:
                connection.close()

            probe = SQLiteTransactionRepository(legacy_db_path).probe()

            self.assertFalse(probe.ready)
            self.assertIn("location", probe.details["missing_inbound_asset_columns"])
            self.assertIn("assets table missing inbound required columns", "; ".join(probe.errors))
        finally:
            if os.path.exists(legacy_db_path):
                os.remove(legacy_db_path)

    def test_sqlite_repository_probe_reports_missing_business_user_resolution_columns(self) -> None:
        handle, legacy_db_path = tempfile.mkstemp(dir=str(Path.cwd()), suffix=".sqlite3")
        os.close(handle)
        os.unlink(legacy_db_path)

        try:
            connection = sqlite3.connect(legacy_db_path)
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
                        user_name TEXT,
                        request_seq INTEGER,
                        request_id TEXT,
                        hw_sn TEXT,
                        borrow_request_id TEXT,
                        FOREIGN KEY (asset_id) REFERENCES assets(id),
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    );
                    """
                )
                connection.commit()
            finally:
                connection.close()

            probe = SQLiteTransactionRepository(legacy_db_path).probe()

            self.assertFalse(probe.ready)
            self.assertIn("student_id", probe.details["missing_business_user_columns"])
            self.assertIn("users table missing business user resolution columns", "; ".join(probe.errors))
        finally:
            if os.path.exists(legacy_db_path):
                os.remove(legacy_db_path)

    def test_sqlite_repository_probe_reports_missing_default_inbound_admin_user_ids(self) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("DELETE FROM users WHERE student_id IN (?, ?)", ("ADMIN", "U-ADMIN"))
            connection.commit()
        finally:
            connection.close()

        repository = SQLiteTransactionRepository(self.db_path)
        probe = repository.probe()

        self.assertFalse(probe.ready)
        self.assertEqual(probe.details["missing_inbound_admin_user_ids"], ["ADMIN", "U-ADMIN"])
        self.assertIn("configured inbound admin business user ids not resolvable", "; ".join(probe.errors))

    def test_sqlite_repository_probe_reports_legacy_operation_records_schema_gap(self) -> None:
        handle, legacy_db_path = tempfile.mkstemp(dir=str(Path.cwd()), suffix=".sqlite3")
        os.close(handle)
        os.unlink(legacy_db_path)

        try:
            connection = sqlite3.connect(legacy_db_path)
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

            probe = SQLiteTransactionRepository(legacy_db_path).probe()

            self.assertFalse(probe.ready)
            self.assertIn("request_seq", probe.details["missing_operation_record_trace_columns"])
            self.assertIn("request_id", probe.details["missing_operation_record_trace_columns"])
            self.assertIn("hw_sn", probe.details["missing_operation_record_trace_columns"])
        finally:
            if os.path.exists(legacy_db_path):
                os.remove(legacy_db_path)

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
            """
            SELECT op_type, request_seq, request_id, hw_seq, hw_result, hw_sn, due_time
            FROM operation_records
            WHERE op_id = 1
            """
        )
        self.assertEqual(asset_row["asset_name"], "New Laptop")
        self.assertEqual(asset_row["category_id"], 1)
        self.assertEqual(asset_row["status"], 0)
        self.assertEqual(asset_row["location"], "Inbound Shelf")
        self.assertEqual(record_row["op_type"], ActionType.INBOUND.value)
        self.assertEqual(record_row["request_seq"], 601)
        self.assertEqual(record_row["request_id"], "req-6001")
        self.assertEqual(record_row["hw_seq"], str(0x80000041))
        self.assertEqual(record_row["hw_result"], ConfirmResult.CONFIRMED.value)
        self.assertEqual(record_row["hw_sn"], "STM32F103-A23")
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

    def test_service_blocks_inbound_before_hardware_when_sql_admin_user_is_not_resolvable(self) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute("DELETE FROM users WHERE student_id IN (?, ?)", ("ADMIN", "U-ADMIN"))
            connection.commit()
        finally:
            connection.close()

        repository = SQLiteTransactionRepository(self.db_path)
        serial_manager = FakeSerialManager()
        service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

        result = service.request_inbound(
            InboundCommand(
                asset_id="AS-7001",
                user_id="U-ADMIN",
                user_name="Admin",
                asset_name="New Laptop",
                category_id=1,
                location="Inbound Shelf",
                timeout_ms=100,
                request_source="api",
            )
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.INTERNAL_ERROR.value)
        self.assertIn("configured inbound admin business user ids not resolvable", result.message)
        self.assertEqual(result.transaction_state, TransactionState.FAILED)
        self.assertEqual(serial_manager.sent_requests, [])
        self.assertIsNone(self._fetch_one("SELECT qr_code FROM assets WHERE qr_code = ?", ("AS-7001",)))

    def test_service_inbound_with_sqlite_repository_succeeds_when_sql_preflight_is_ready(self) -> None:
        repository = SQLiteTransactionRepository(self.db_path)
        serial_manager = FakeSerialManager(
            response_factory=lambda payload, seq_id: Frame.build(
                MsgType.EVT_USER_ACTION,
                seq_id=0x80000041,
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

        result = service.request_inbound(
            InboundCommand(
                asset_id="AS-7002",
                user_id="U-ADMIN",
                user_name="Admin",
                asset_name="New Laptop",
                category_id=1,
                location="Inbound Shelf",
                timeout_ms=100,
                request_source="api",
            )
        )

        self.assertTrue(result.success)
        self.assertEqual(result.code, ConfirmResult.CONFIRMED.value)
        self.assertEqual(result.transaction_state, TransactionState.COMPLETED)
        self.assertEqual(len(serial_manager.sent_requests), 1)
        asset_row = self._fetch_one("SELECT qr_code, status FROM assets WHERE qr_code = ?", ("AS-7002",))
        self.assertEqual(asset_row["qr_code"], "AS-7002")
        self.assertEqual(asset_row["status"], 0)

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

    def test_build_default_runtime_blocks_formal_mysql_writes_when_mysql_is_unavailable(self) -> None:
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
        self.assertFalse(runtime.repository_ready)
        self.assertEqual(runtime.repository_status, "error")
        self.assertIsNotNone(runtime.startup_error)
        self.assertIn("mysql repository unavailable", runtime.startup_error)
        self.assertEqual(runtime.repository_details["backend"], "inmemory")
        self.assertEqual(runtime.repository_details["details"]["requested_mode"], "mysql")
        self.assertTrue(runtime.repository_details["details"]["write_blocked"])


if __name__ == "__main__":
    unittest.main()
