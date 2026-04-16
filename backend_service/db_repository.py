from __future__ import annotations

import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from asset_lifecycle import next_asset_status_for_action, validate_asset_transition
from models import (
    AssetStatus,
    BorrowRequestCreateInput,
    BorrowRequestRecord,
    BorrowRequestReviewInput,
    BorrowRequestStatus,
    InboundCommitInput,
    OperationRecordInput,
)

LOGGER = logging.getLogger(__name__)

ASSET_STATUS_TO_DB = {
    AssetStatus.IN_STOCK: 0,
    AssetStatus.BORROWED: 1,
    AssetStatus.MAINTENANCE: 2,
    AssetStatus.SCRAPPED: 3,
}
DB_STATUS_TO_ASSET = {value: key for key, value in ASSET_STATUS_TO_DB.items()}
REQUIRED_TABLES = ("assets", "users", "categories", "operation_records", "borrow_requests")
BORROW_REQUEST_COLUMNS = (
    "request_id",
    "asset_id",
    "applicant_user_id",
    "applicant_user_name",
    "reason",
    "status",
    "reviewer_user_id",
    "reviewer_user_name",
    "review_comment",
    "requested_at",
    "reviewed_at",
    "consumed_at",
)


@dataclass(slots=True)
class RepositoryProbeResult:
    ready: bool
    status: str
    backend: str
    database: str | None = None
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "status": self.status,
            "backend": self.backend,
            "database": self.database,
            "warnings": list(self.warnings),
            "errors": list(self.errors),
            "details": dict(self.details),
        }


@dataclass(slots=True)
class MySQLRepositoryConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"

    @classmethod
    def from_env(cls) -> MySQLRepositoryConfig:
        database = os.getenv("BACKEND_DB_NAME", "").strip()
        user = os.getenv("BACKEND_DB_USER", "").strip()
        password = os.getenv("BACKEND_DB_PASSWORD", "")
        host = os.getenv("BACKEND_DB_HOST", "127.0.0.1").strip() or "127.0.0.1"
        port = int(os.getenv("BACKEND_DB_PORT", "3306"))
        charset = os.getenv("BACKEND_DB_CHARSET", "utf8mb4").strip() or "utf8mb4"

        missing = [
            name
            for name, value in (
                ("BACKEND_DB_NAME", database),
                ("BACKEND_DB_USER", user),
                ("BACKEND_DB_PASSWORD", password),
            )
            if not value
        ]
        if missing:
            raise ValueError(f"missing MySQL config: {', '.join(missing)}")

        return cls(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset=charset,
        )


class SqlTransactionRepository:
    """
    DB-API based repository that owns the atomic commit boundary.

    The repository resolves business identifiers to database keys, validates the
    asset transition inside the transaction, updates assets.status and inserts a
    matching operation_records row in the same transaction.
    """

    def __init__(
        self,
        *,
        connect: Callable[[], Any],
        placeholder: str,
        int_as_text_expr: Callable[[str], str],
        backend_name: str,
        database_name: str | None = None,
        select_for_update_clause: str = "",
        begin_immediate: bool = False,
    ) -> None:
        self._connect = connect
        self._placeholder = placeholder
        self._int_as_text_expr = int_as_text_expr
        self._backend_name = backend_name
        self._database_name = database_name
        self._select_for_update_clause = select_for_update_clause
        self._begin_immediate = begin_immediate
        self._table_columns_cache: dict[str, tuple[str, ...]] = {}

    def get_asset_status(self, asset_id: str) -> AssetStatus | None:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            asset_row = self._resolve_asset_row(cursor, asset_id, for_update=False)
            if asset_row is None:
                return None
            return self._db_status_to_asset_status(asset_row[1])
        finally:
            connection.close()

    def category_exists(self, category_id: int) -> bool:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            return self._resolve_category_row(cursor, category_id) is not None
        finally:
            connection.close()

    def create_borrow_request(self, request: BorrowRequestCreateInput) -> BorrowRequestRecord:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            placeholder = self._placeholder
            sql = (
                "INSERT INTO borrow_requests ("
                + ", ".join(BORROW_REQUEST_COLUMNS[:6] + ("requested_at",))
                + f") VALUES ({', '.join(placeholder for _ in range(7))})"
            )
            cursor.execute(
                sql,
                (
                    request.request_id,
                    request.asset_id,
                    request.applicant_user_id,
                    request.applicant_user_name,
                    request.reason,
                    request.status.value,
                    request.requested_at,
                ),
            )
            connection.commit()
            return BorrowRequestRecord(
                request_id=request.request_id,
                asset_id=request.asset_id,
                applicant_user_id=request.applicant_user_id,
                applicant_user_name=request.applicant_user_name,
                reason=request.reason,
                status=request.status,
                requested_at=request.requested_at,
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def list_borrow_requests(
        self,
        *,
        status: BorrowRequestStatus | None = None,
        applicant_user_id: str | None = None,
        asset_id: str | None = None,
    ) -> list[BorrowRequestRecord]:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            placeholder = self._placeholder
            sql = "SELECT " + ", ".join(BORROW_REQUEST_COLUMNS) + " FROM borrow_requests"
            clauses: list[str] = []
            params: list[Any] = []
            if status is not None:
                clauses.append(f"status = {placeholder}")
                params.append(status.value)
            if applicant_user_id:
                clauses.append(f"applicant_user_id = {placeholder}")
                params.append(applicant_user_id)
            if asset_id:
                clauses.append(f"asset_id = {placeholder}")
                params.append(asset_id)
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            sql += " ORDER BY requested_at DESC, request_id DESC"
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
            return [self._borrow_request_tuple_to_record(row) for row in rows]
        finally:
            connection.close()

    def get_borrow_request(self, request_id: str) -> BorrowRequestRecord | None:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            row = self._resolve_borrow_request_row(cursor, request_id, for_update=False)
            return None if row is None else self._borrow_request_tuple_to_record(row)
        finally:
            connection.close()

    def review_borrow_request(self, review: BorrowRequestReviewInput) -> BorrowRequestRecord:
        connection = self._connect()
        self._begin_transaction(connection)
        try:
            cursor = connection.cursor()
            current = self._resolve_borrow_request_row(cursor, review.request_id, for_update=True)
            if current is None:
                raise LookupError(f"borrow request not found: {review.request_id}")

            current_record = self._borrow_request_tuple_to_record(current)
            if current_record.status != BorrowRequestStatus.PENDING:
                raise ValueError("借用申请当前不是待审批状态，不能重复审批")

            placeholder = self._placeholder
            cursor.execute(
                (
                    "UPDATE borrow_requests SET status = "
                    f"{placeholder}, reviewer_user_id = {placeholder}, reviewer_user_name = {placeholder}, "
                    f"review_comment = {placeholder}, reviewed_at = {placeholder} WHERE request_id = {placeholder}"
                ),
                (
                    review.status.value,
                    review.reviewer_user_id,
                    review.reviewer_user_name,
                    review.review_comment,
                    review.reviewed_at,
                    review.request_id,
                ),
            )
            connection.commit()
            return BorrowRequestRecord(
                request_id=current_record.request_id,
                asset_id=current_record.asset_id,
                applicant_user_id=current_record.applicant_user_id,
                applicant_user_name=current_record.applicant_user_name,
                reason=current_record.reason,
                status=review.status,
                reviewer_user_id=review.reviewer_user_id,
                reviewer_user_name=review.reviewer_user_name,
                review_comment=review.review_comment,
                requested_at=current_record.requested_at,
                reviewed_at=review.reviewed_at,
                consumed_at=current_record.consumed_at,
            )
        except Exception:
            self._rollback_connection(connection, reason="borrow request review failed")
            raise
        finally:
            connection.close()

    def probe(self) -> RepositoryProbeResult:
        connection = None
        try:
            connection = self._connect()
            cursor = connection.cursor()
            tables = sorted(self._list_table_names(cursor))
            missing_tables = sorted(set(REQUIRED_TABLES) - set(tables))
            warnings = self._probe_warnings(cursor)
            errors = []
            if missing_tables:
                errors.append(f"missing tables: {', '.join(missing_tables)}")

            details = {
                "required_tables": list(REQUIRED_TABLES),
                "tables_present": [name for name in REQUIRED_TABLES if name in tables],
                "missing_tables": missing_tables,
            }
            details.update(self._probe_backend_details(cursor))

            status = "error" if errors else ("warning" if warnings else "ok")
            return RepositoryProbeResult(
                ready=not errors,
                status=status,
                backend=self._backend_name,
                database=self._database_name,
                warnings=warnings,
                errors=errors,
                details=details,
            )
        except Exception as exc:
            LOGGER.exception("repository probe failed: backend=%s database=%s", self._backend_name, self._database_name)
            return RepositoryProbeResult(
                ready=False,
                status="error",
                backend=self._backend_name,
                database=self._database_name,
                errors=[str(exc)],
                details={"exception_type": type(exc).__name__},
            )
        finally:
            if connection is not None:
                connection.close()

    def apply_operation_atomically(self, record: OperationRecordInput) -> AssetStatus:
        connection = self._connect()
        self._begin_transaction(connection)
        cursor = None
        insert_context: Any | None = None
        try:
            cursor = connection.cursor()
            asset_row = self._resolve_asset_row(cursor, record.asset_id, for_update=True)
            if asset_row is None:
                raise LookupError(f"asset not found: {record.asset_id}")

            current_status = self._db_status_to_asset_status(asset_row[1])
            invalid_reason = validate_asset_transition(current_status, record.action_type)
            if invalid_reason is not None:
                raise ValueError(invalid_reason)

            user_row = self._resolve_user_row(cursor, record.user_id)
            if user_row is None:
                raise RuntimeError(f"user not found: {record.user_id}")

            if record.borrow_request_id is not None:
                self._validate_borrow_request_for_consume(cursor, record)

            new_status = next_asset_status_for_action(record.action_type)
            self._update_asset_status(cursor, asset_db_id=asset_row[0], new_status=new_status)
            insert_context = self._prepare_operation_record_insert(cursor, record)
            self._insert_operation_record(
                cursor,
                operation_id=0,
                asset_db_id=asset_row[0],
                user_db_id=user_row[0],
                record=record,
                insert_context=insert_context,
            )
            if record.borrow_request_id is not None:
                self._mark_borrow_request_consumed(cursor, record.borrow_request_id)
            connection.commit()
            LOGGER.info(
                "sql repository commit success: asset_id=%s action=%s request_seq=%s hw_seq=%s hw_result=%s",
                record.asset_id,
                record.action_type.value,
                record.request_seq,
                record.hw_seq,
                record.hw_result,
            )
            return new_status
        except Exception as exc:
            self._rollback_connection(connection, reason=str(exc))
            raise
        finally:
            if cursor is not None:
                self._cleanup_operation_record_insert(cursor, insert_context)
            connection.close()

    def apply_inbound_atomically(self, commit: InboundCommitInput) -> AssetStatus:
        connection = self._connect()
        self._begin_transaction(connection)
        cursor = None
        insert_context: Any | None = None
        try:
            cursor = connection.cursor()
            asset_row = self._resolve_asset_row(cursor, commit.asset_id, for_update=True)
            if asset_row is not None:
                raise ValueError(f"资产已存在，不允许重复入库: {commit.asset_id}")

            if commit.category_id is not None and self._resolve_category_row(cursor, commit.category_id) is None:
                raise ValueError(f"分类不存在: {commit.category_id}")

            user_row = self._resolve_user_row(cursor, commit.user_id)
            if user_row is None:
                raise RuntimeError(f"user not found: {commit.user_id}")

            asset_db_id = self._insert_inbound_asset(cursor, commit)
            insert_context = self._prepare_operation_record_insert(cursor, commit)
            self._insert_operation_record(
                cursor,
                operation_id=0,
                asset_db_id=asset_db_id,
                user_db_id=user_row[0],
                record=commit,
                insert_context=insert_context,
            )
            connection.commit()
            LOGGER.info(
                "sql repository inbound commit success: asset_id=%s request_seq=%s hw_seq=%s hw_result=%s",
                commit.asset_id,
                commit.request_seq,
                commit.hw_seq,
                commit.hw_result,
            )
            return AssetStatus.IN_STOCK
        except Exception as exc:
            self._rollback_connection(connection, reason=str(exc))
            raise
        finally:
            if cursor is not None:
                self._cleanup_operation_record_insert(cursor, insert_context)
            connection.close()

    def rollback_transaction(self, asset_id: str, reason: str) -> None:
        LOGGER.warning(
            "sql repository rollback hook called after transactional failure: asset_id=%s reason=%s",
            asset_id,
            reason,
        )

    def _begin_transaction(self, connection: Any) -> None:
        if self._begin_immediate:
            connection.execute("BEGIN IMMEDIATE")

    def _rollback_connection(self, connection: Any, reason: str) -> None:
        try:
            connection.rollback()
        except Exception:
            LOGGER.warning("sql repository rollback failed: reason=%s", reason, exc_info=True)
            return
        LOGGER.warning("sql repository rollback success: reason=%s", reason)

    def _resolve_asset_row(self, cursor: Any, asset_id: str, *, for_update: bool) -> tuple[Any, ...] | None:
        placeholder = self._placeholder
        lock_clause = self._select_for_update_clause if for_update else ""
        sql = (
            "SELECT id, status, qr_code "
            f"FROM assets WHERE qr_code = {placeholder} OR {self._int_as_text_expr('id')} = {placeholder} "
            f"ORDER BY CASE WHEN qr_code = {placeholder} THEN 0 ELSE 1 END LIMIT 1{lock_clause}"
        )
        cursor.execute(sql, (asset_id, asset_id, asset_id))
        row = cursor.fetchone()
        return None if row is None else tuple(row)

    def _resolve_user_row(self, cursor: Any, user_id: str) -> tuple[Any, ...] | None:
        placeholder = self._placeholder
        sql = (
            "SELECT user_id, user_name, student_id "
            f"FROM users WHERE student_id = {placeholder} OR {self._int_as_text_expr('user_id')} = {placeholder} "
            f"ORDER BY CASE WHEN student_id = {placeholder} THEN 0 ELSE 1 END LIMIT 1"
        )
        cursor.execute(sql, (user_id, user_id, user_id))
        row = cursor.fetchone()
        return None if row is None else tuple(row)

    def _resolve_category_row(self, cursor: Any, category_id: int) -> tuple[Any, ...] | None:
        placeholder = self._placeholder
        cursor.execute(f"SELECT id FROM categories WHERE id = {placeholder} LIMIT 1", (category_id,))
        row = cursor.fetchone()
        return None if row is None else tuple(row)

    def _resolve_borrow_request_row(self, cursor: Any, request_id: str, *, for_update: bool) -> tuple[Any, ...] | None:
        placeholder = self._placeholder
        lock_clause = self._select_for_update_clause if for_update else ""
        sql = (
            "SELECT " + ", ".join(BORROW_REQUEST_COLUMNS) + " FROM borrow_requests "
            f"WHERE request_id = {placeholder} LIMIT 1{lock_clause}"
        )
        cursor.execute(sql, (request_id,))
        row = cursor.fetchone()
        return None if row is None else tuple(row)

    def _borrow_request_tuple_to_record(self, row: Any) -> BorrowRequestRecord:
        values = tuple(row)
        return BorrowRequestRecord(
            request_id=str(values[0]),
            asset_id=str(values[1]),
            applicant_user_id=str(values[2]),
            applicant_user_name=str(values[3]),
            reason=values[4],
            status=BorrowRequestStatus(str(values[5])),
            reviewer_user_id=values[6],
            reviewer_user_name=values[7],
            review_comment=values[8],
            requested_at=values[9],
            reviewed_at=values[10],
            consumed_at=values[11],
        )

    def _validate_borrow_request_for_consume(self, cursor: Any, record: OperationRecordInput) -> BorrowRequestRecord:
        if record.borrow_request_id is None:
            raise ValueError("borrow_request_id is required")

        row = self._resolve_borrow_request_row(cursor, record.borrow_request_id, for_update=True)
        if row is None:
            raise ValueError("借用申请不存在")

        borrow_request = self._borrow_request_tuple_to_record(row)
        if borrow_request.status == BorrowRequestStatus.PENDING:
            raise ValueError("借用申请尚未审批通过，不能发起借出确认")
        if borrow_request.status == BorrowRequestStatus.REJECTED:
            raise ValueError("借用申请已被拒绝，不能发起借出确认")
        if borrow_request.status == BorrowRequestStatus.CONSUMED:
            raise ValueError("借用申请已被使用，不能重复借出")
        if borrow_request.asset_id != record.asset_id:
            raise ValueError("借用申请与借出资产不匹配")
        if borrow_request.applicant_user_id != record.user_id:
            raise ValueError("借用申请与借用人不匹配")
        return borrow_request

    def _mark_borrow_request_consumed(self, cursor: Any, request_id: str) -> None:
        placeholder = self._placeholder
        cursor.execute(
            f"UPDATE borrow_requests SET status = {placeholder}, consumed_at = {placeholder} WHERE request_id = {placeholder}",
            (
                BorrowRequestStatus.CONSUMED.value,
                datetime.now().isoformat(sep=" ", timespec="seconds"),
                request_id,
            ),
        )

    def _update_asset_status(self, cursor: Any, *, asset_db_id: int, new_status: AssetStatus) -> None:
        placeholder = self._placeholder
        sql = f"UPDATE assets SET status = {placeholder} WHERE id = {placeholder}"
        cursor.execute(sql, (self._asset_status_to_db(new_status), asset_db_id))

    def _insert_inbound_asset(self, cursor: Any, commit: InboundCommitInput) -> int:
        columns = set(self._get_table_columns(cursor, "assets"))
        required_columns = {"asset_name", "qr_code", "status", "location"}
        missing_columns = sorted(required_columns - columns)
        if missing_columns:
            raise RuntimeError(f"assets table missing required columns: {', '.join(missing_columns)}")

        values: dict[str, Any] = {
            "asset_name": commit.asset_name,
            "qr_code": commit.asset_id,
            "status": self._asset_status_to_db(AssetStatus.IN_STOCK),
            "location": commit.location,
        }
        if "category_id" in columns:
            values["category_id"] = commit.category_id

        placeholder = self._placeholder
        sql = (
            f"INSERT INTO assets ({', '.join(values.keys())}) "
            f"VALUES ({', '.join(placeholder for _ in values)})"
        )
        cursor.execute(sql, tuple(values.values()))
        asset_db_id = getattr(cursor, "lastrowid", None)
        if asset_db_id is None:
            asset_row = self._resolve_asset_row(cursor, commit.asset_id, for_update=False)
            if asset_row is None:
                raise RuntimeError(f"failed to resolve inbound asset row after insert: {commit.asset_id}")
            asset_db_id = asset_row[0]
        return int(asset_db_id)

    def _next_operation_id(self, cursor: Any) -> int:
        cursor.execute("SELECT COALESCE(MAX(op_id), 0) + 1 FROM operation_records")
        row = cursor.fetchone()
        return int(tuple(row)[0])

    def _prepare_operation_record_insert(self, cursor: Any, record: OperationRecordInput) -> Any | None:
        return None

    def _cleanup_operation_record_insert(self, cursor: Any, insert_context: Any | None) -> None:
        return None

    def _get_table_columns(self, cursor: Any, table_name: str) -> tuple[str, ...]:
        cached = self._table_columns_cache.get(table_name)
        if cached is not None:
            return cached

        cursor.execute(f"SELECT * FROM {table_name} WHERE 1 = 0")
        columns = tuple(str(column[0]) for column in (cursor.description or ()))
        self._table_columns_cache[table_name] = columns
        return columns

    def _build_operation_record_row(
        self,
        cursor: Any,
        *,
        operation_id: int,
        asset_db_id: int,
        user_db_id: int,
        record: OperationRecordInput | InboundCommitInput,
        include_operation_id: bool,
    ) -> dict[str, Any]:
        columns = set(self._get_table_columns(cursor, "operation_records"))
        op_time = getattr(record, "op_time", None) or datetime.now().isoformat(sep=" ", timespec="seconds")
        row: dict[str, Any] = {}

        if include_operation_id and "op_id" in columns:
            row["op_id"] = operation_id
        if "asset_id" in columns:
            row["asset_id"] = asset_db_id
        if "user_id" in columns:
            row["user_id"] = user_db_id
        if "op_type" in columns:
            row["op_type"] = record.action_type.value
        if "op_time" in columns:
            row["op_time"] = op_time
        if "hw_seq" in columns:
            row["hw_seq"] = str(record.hw_seq)
        if "hw_result" in columns:
            row["hw_result"] = record.hw_result
        if "due_time" in columns:
            row["due_time"] = getattr(record, "due_time", None)
        if "user_name" in columns:
            row["user_name"] = record.user_name
        if "request_seq" in columns:
            row["request_seq"] = record.request_seq
        if "request_id" in columns:
            row["request_id"] = record.request_id
        if "hw_sn" in columns:
            row["hw_sn"] = getattr(record, "hw_sn", None)
        if "borrow_request_id" in columns:
            row["borrow_request_id"] = getattr(record, "borrow_request_id", None)
        return row

    def _insert_operation_record(
        self,
        cursor: Any,
        *,
        operation_id: int,
        asset_db_id: int,
        user_db_id: int,
        record: OperationRecordInput | InboundCommitInput,
        insert_context: Any | None = None,
    ) -> None:
        actual_operation_id = operation_id if operation_id > 0 else self._next_operation_id(cursor)
        row = self._build_operation_record_row(
            cursor,
            operation_id=actual_operation_id,
            asset_db_id=asset_db_id,
            user_db_id=user_db_id,
            record=record,
            include_operation_id=True,
        )
        placeholder = self._placeholder
        sql = (
            f"INSERT INTO operation_records ({', '.join(row.keys())}) "
            f"VALUES ({', '.join(placeholder for _ in row)})"
        )
        cursor.execute(sql, tuple(row.values()))

    @staticmethod
    def _asset_status_to_db(status: AssetStatus) -> int:
        return ASSET_STATUS_TO_DB[status]

    @staticmethod
    def _db_status_to_asset_status(status_code: int | None) -> AssetStatus:
        if status_code not in DB_STATUS_TO_ASSET:
            raise ValueError(f"unsupported asset status code: {status_code}")
        return DB_STATUS_TO_ASSET[int(status_code)]

    def _list_table_names(self, cursor: Any) -> list[str]:
        raise NotImplementedError

    def _probe_backend_details(self, cursor: Any) -> dict[str, Any]:
        return {}

    def _probe_warnings(self, cursor: Any) -> list[str]:
        return []

    def _ensure_borrow_requests_table(self) -> None:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            if self._backend_name == "sqlite":
                self._create_borrow_requests_table_sqlite(cursor)
            else:
                self._create_borrow_requests_table_mysql(cursor)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _create_borrow_requests_table_sqlite(self, cursor: Any) -> None:
        cursor.executescript(
            """
            CREATE TABLE IF NOT EXISTS borrow_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL UNIQUE,
                asset_id TEXT NOT NULL,
                applicant_user_id TEXT NOT NULL,
                applicant_user_name TEXT NOT NULL,
                reason TEXT,
                status TEXT NOT NULL,
                reviewer_user_id TEXT,
                reviewer_user_name TEXT,
                review_comment TEXT,
                requested_at TEXT NOT NULL,
                reviewed_at TEXT,
                consumed_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_borrow_requests_status ON borrow_requests(status);
            CREATE INDEX IF NOT EXISTS idx_borrow_requests_asset_id ON borrow_requests(asset_id);
            CREATE INDEX IF NOT EXISTS idx_borrow_requests_applicant_user_id
                ON borrow_requests(applicant_user_id);
            """
        )

    def _create_borrow_requests_table_mysql(self, cursor: Any) -> None:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS borrow_requests (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                request_id VARCHAR(64) NOT NULL UNIQUE,
                asset_id VARCHAR(64) NOT NULL,
                applicant_user_id VARCHAR(64) NOT NULL,
                applicant_user_name VARCHAR(128) NOT NULL,
                reason TEXT NULL,
                status VARCHAR(32) NOT NULL,
                reviewer_user_id VARCHAR(64) NULL,
                reviewer_user_name VARCHAR(128) NULL,
                review_comment TEXT NULL,
                requested_at VARCHAR(32) NOT NULL,
                reviewed_at VARCHAR(32) NULL,
                consumed_at VARCHAR(32) NULL,
                INDEX idx_borrow_requests_status (status),
                INDEX idx_borrow_requests_asset_id (asset_id),
                INDEX idx_borrow_requests_applicant_user_id (applicant_user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )


class SQLiteTransactionRepository(SqlTransactionRepository):
    def __init__(self, database_path: str) -> None:
        path = Path(database_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        def connect() -> sqlite3.Connection:
            connection = sqlite3.connect(str(path), detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            return connection

        super().__init__(
            connect=connect,
            placeholder="?",
            int_as_text_expr=lambda column: f"CAST({column} AS TEXT)",
            backend_name="sqlite",
            database_name=str(path),
            begin_immediate=True,
        )
        self.database_path = str(path)
        self._ensure_borrow_requests_table()

    def _list_table_names(self, cursor: Any) -> list[str]:
        cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        rows = cursor.fetchall()
        return [str(tuple(row)[0]) for row in rows]

    def _probe_backend_details(self, cursor: Any) -> dict[str, Any]:
        cursor.execute("PRAGMA foreign_keys")
        row = cursor.fetchone()
        foreign_keys_on = bool(tuple(row)[0]) if row is not None else False
        return {
            "database_path": self.database_path,
            "foreign_keys_enabled": foreign_keys_on,
            "operation_record_id_strategy": "sqlite_manual_max_plus_one",
        }


class MySQLTransactionRepository(SqlTransactionRepository):
    def __init__(self, config: MySQLRepositoryConfig) -> None:
        try:
            import pymysql
        except ModuleNotFoundError as exc:
            raise RuntimeError("PyMySQL is required for MySQLTransactionRepository") from exc

        def connect():
            return pymysql.connect(
                host=config.host,
                port=config.port,
                user=config.user,
                password=config.password,
                database=config.database,
                charset=config.charset,
                autocommit=False,
                cursorclass=pymysql.cursors.Cursor,
            )

        super().__init__(
            connect=connect,
            placeholder="%s",
            int_as_text_expr=lambda column: f"CAST({column} AS CHAR)",
            backend_name="mysql",
            database_name=config.database,
            select_for_update_clause=" FOR UPDATE",
        )
        self.config = config
        self._operation_record_auto_increment: bool | None = None
        self._operation_record_lock_name = f"{config.database}.operation_records.op_id"
        self._operation_record_lock_timeout_seconds = int(os.getenv("BACKEND_DB_OP_ID_LOCK_TIMEOUT", "5"))
        self._ensure_borrow_requests_table()

    @classmethod
    def from_env(cls) -> MySQLTransactionRepository:
        return cls(MySQLRepositoryConfig.from_env())

    def _list_table_names(self, cursor: Any) -> list[str]:
        cursor.execute("SHOW TABLES")
        rows = cursor.fetchall()
        return [str(tuple(row)[0]) for row in rows]

    def _probe_backend_details(self, cursor: Any) -> dict[str, Any]:
        cursor.execute("SELECT VERSION()")
        version_row = cursor.fetchone()
        auto_increment = self._operation_records_has_auto_increment(cursor)
        return {
            "server_version": None if version_row is None else tuple(version_row)[0],
            "operation_record_id_auto_increment": auto_increment,
            "operation_record_id_strategy": (
                "mysql_auto_increment" if auto_increment else "mysql_named_lock_max_plus_one"
            ),
            "operation_record_lock_name": None if auto_increment else self._operation_record_lock_name,
        }

    def _probe_warnings(self, cursor: Any) -> list[str]:
        if self._operation_records_has_auto_increment(cursor):
            return []
        return [
            "operation_records.op_id is not AUTO_INCREMENT; repository will serialize op_id allocation "
            "with a MySQL named lock. This is safe for app-managed writes but external writers bypassing "
            "the lock can still cause conflicts."
        ]

    def _prepare_operation_record_insert(self, cursor: Any, record: OperationRecordInput) -> Any | None:
        if self._operation_records_has_auto_increment(cursor):
            return {"use_auto_increment": True}

        self._acquire_operation_id_lock(cursor)
        return {
            "use_auto_increment": False,
            "lock_name": self._operation_record_lock_name,
            "operation_id": self._next_operation_id(cursor),
        }

    def _cleanup_operation_record_insert(self, cursor: Any, insert_context: Any | None) -> None:
        if not insert_context or insert_context.get("use_auto_increment", False):
            return

        lock_name = insert_context.get("lock_name")
        if not lock_name:
            return

        try:
            cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
        except Exception:
            LOGGER.warning("mysql repository failed to release named lock: %s", lock_name, exc_info=True)

    def _insert_operation_record(
        self,
        cursor: Any,
        *,
        operation_id: int,
        asset_db_id: int,
        user_db_id: int,
        record: OperationRecordInput | InboundCommitInput,
        insert_context: Any | None = None,
    ) -> None:
        if insert_context and insert_context.get("use_auto_increment", False):
            row = self._build_operation_record_row(
                cursor,
                operation_id=0,
                asset_db_id=asset_db_id,
                user_db_id=user_db_id,
                record=record,
                include_operation_id=False,
            )
            sql = (
                f"INSERT INTO operation_records ({', '.join(row.keys())}) "
                f"VALUES ({', '.join('%s' for _ in row)})"
            )
            cursor.execute(sql, tuple(row.values()))
            return

        actual_operation_id = operation_id
        if insert_context is not None:
            actual_operation_id = int(insert_context.get("operation_id", operation_id))
        super()._insert_operation_record(
            cursor,
            operation_id=actual_operation_id,
            asset_db_id=asset_db_id,
            user_db_id=user_db_id,
            record=record,
            insert_context=insert_context,
        )

    def _operation_records_has_auto_increment(self, cursor: Any) -> bool:
        if self._operation_record_auto_increment is not None:
            return self._operation_record_auto_increment

        cursor.execute(
            """
            SELECT EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'operation_records' AND COLUMN_NAME = 'op_id'
            """,
            (self.config.database,),
        )
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("operation_records.op_id column metadata not found")

        extra = str(tuple(row)[0] or "").lower()
        self._operation_record_auto_increment = "auto_increment" in extra
        return self._operation_record_auto_increment

    def _acquire_operation_id_lock(self, cursor: Any) -> None:
        cursor.execute(
            "SELECT GET_LOCK(%s, %s)",
            (self._operation_record_lock_name, self._operation_record_lock_timeout_seconds),
        )
        row = cursor.fetchone()
        lock_result = None if row is None else tuple(row)[0]
        if int(lock_result or 0) != 1:
            raise RuntimeError("failed to acquire MySQL named lock for operation_records.op_id allocation")
