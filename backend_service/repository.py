from __future__ import annotations

import logging
import threading
from dataclasses import replace
from datetime import datetime
from typing import Any, Protocol

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

DEFAULT_INMEMORY_CATEGORIES = {1: "默认分类"}


class TransactionRepository(Protocol):
    def get_asset_status(self, asset_id: str) -> AssetStatus | None: ...

    def category_exists(self, category_id: int) -> bool: ...

    def apply_operation_atomically(self, record: OperationRecordInput) -> AssetStatus: ...

    def apply_inbound_atomically(self, commit: InboundCommitInput) -> AssetStatus: ...

    def create_borrow_request(self, request: BorrowRequestCreateInput) -> BorrowRequestRecord: ...

    def list_borrow_requests(
        self,
        *,
        status: BorrowRequestStatus | None = None,
        applicant_user_id: str | None = None,
        asset_id: str | None = None,
    ) -> list[BorrowRequestRecord]: ...

    def get_borrow_request(self, request_id: str) -> BorrowRequestRecord | None: ...

    def review_borrow_request(self, review: BorrowRequestReviewInput) -> BorrowRequestRecord: ...

    def rollback_transaction(self, asset_id: str, reason: str) -> None: ...


class InMemoryTransactionRepository:
    """
    Demo repository with explicit atomic commit semantics.

    Borrow / Return keep using apply_operation_atomically().
    Inbound uses apply_inbound_atomically().
    Borrow request persistence is implemented in-memory for tests/demo, while
    SQL repositories provide the durable implementation used in sqlite/mysql mode.
    """

    def __init__(
        self,
        initial_assets: dict[str, AssetStatus] | None = None,
        *,
        initial_categories: dict[int, str] | None = None,
        initial_asset_details: dict[str, dict[str, Any]] | None = None,
        initial_borrow_requests: list[BorrowRequestRecord] | None = None,
    ) -> None:
        self.assets: dict[str, AssetStatus] = dict(initial_assets or {})
        self.categories: dict[int, str] = dict(initial_categories or DEFAULT_INMEMORY_CATEGORIES)
        self.asset_details: dict[str, dict[str, Any]] = {
            asset_id: dict(details) for asset_id, details in (initial_asset_details or {}).items()
        }
        self.records: list[OperationRecordInput | InboundCommitInput] = []
        self.borrow_requests: dict[str, BorrowRequestRecord] = {
            record.request_id: self._clone_borrow_request(record) for record in (initial_borrow_requests or [])
        }
        self._lock = threading.Lock()
        self._snapshots: dict[str, dict[str, Any]] = {}

    def get_asset_status(self, asset_id: str) -> AssetStatus | None:
        with self._lock:
            return self.assets.get(asset_id)

    def category_exists(self, category_id: int) -> bool:
        with self._lock:
            return category_id in self.categories

    def create_borrow_request(self, request: BorrowRequestCreateInput) -> BorrowRequestRecord:
        with self._lock:
            record = BorrowRequestRecord(
                request_id=request.request_id,
                asset_id=request.asset_id,
                applicant_user_id=request.applicant_user_id,
                applicant_user_name=request.applicant_user_name,
                reason=request.reason,
                status=request.status,
                requested_at=request.requested_at,
            )
            self.borrow_requests[record.request_id] = self._clone_borrow_request(record)
            return self._clone_borrow_request(record)

    def list_borrow_requests(
        self,
        *,
        status: BorrowRequestStatus | None = None,
        applicant_user_id: str | None = None,
        asset_id: str | None = None,
    ) -> list[BorrowRequestRecord]:
        with self._lock:
            items = list(self.borrow_requests.values())

        filtered: list[BorrowRequestRecord] = []
        for item in items:
            if status is not None and item.status != status:
                continue
            if applicant_user_id and item.applicant_user_id != applicant_user_id:
                continue
            if asset_id and item.asset_id != asset_id:
                continue
            filtered.append(self._clone_borrow_request(item))

        return sorted(filtered, key=lambda item: (item.requested_at or "", item.request_id), reverse=True)

    def get_borrow_request(self, request_id: str) -> BorrowRequestRecord | None:
        with self._lock:
            record = self.borrow_requests.get(request_id)
            return None if record is None else self._clone_borrow_request(record)

    def review_borrow_request(self, review: BorrowRequestReviewInput) -> BorrowRequestRecord:
        with self._lock:
            current = self.borrow_requests.get(review.request_id)
            if current is None:
                raise LookupError(f"borrow request not found: {review.request_id}")
            if current.status != BorrowRequestStatus.PENDING:
                raise ValueError("借用申请当前不是待审批状态，不能重复审批")

            updated = replace(
                current,
                status=review.status,
                reviewer_user_id=review.reviewer_user_id,
                reviewer_user_name=review.reviewer_user_name,
                review_comment=review.review_comment,
                reviewed_at=review.reviewed_at,
            )
            self.borrow_requests[review.request_id] = updated
            return self._clone_borrow_request(updated)

    def apply_operation_atomically(self, record: OperationRecordInput) -> AssetStatus:
        with self._lock:
            current_status = self.assets.get(record.asset_id)
            if current_status is None:
                raise LookupError(f"资产不存在: {record.asset_id}")

            invalid_reason = validate_asset_transition(current_status, record.action_type)
            if invalid_reason is not None:
                raise ValueError(invalid_reason)

            if record.borrow_request_id is not None:
                self._validate_borrow_request_for_consume_locked(record.borrow_request_id, record)

            self._snapshots[record.asset_id] = self._snapshot_locked(record.asset_id, record.borrow_request_id)

            try:
                new_status = next_asset_status_for_action(record.action_type)
                self.assets[record.asset_id] = new_status
                self.records.append(record)
                if record.borrow_request_id is not None:
                    borrow_request = self.borrow_requests[record.borrow_request_id]
                    self.borrow_requests[record.borrow_request_id] = replace(
                        borrow_request,
                        status=BorrowRequestStatus.CONSUMED,
                        consumed_at=self._now_string(),
                    )
            except Exception:
                self._restore_snapshot_locked(record.asset_id)
                raise

            self._snapshots.pop(record.asset_id, None)
            LOGGER.info(
                "repository commit success: asset_id=%s action=%s request_seq=%s hw_seq=%s hw_result=%s",
                record.asset_id,
                record.action_type.value,
                record.request_seq,
                record.hw_seq,
                record.hw_result,
            )
            return new_status

    def apply_inbound_atomically(self, commit: InboundCommitInput) -> AssetStatus:
        with self._lock:
            if commit.asset_id in self.assets:
                raise ValueError(f"资产已存在，不允许重复入库: {commit.asset_id}")
            if commit.category_id is not None and commit.category_id not in self.categories:
                raise ValueError(f"分类不存在: {commit.category_id}")

            self._snapshots[commit.asset_id] = self._snapshot_locked(commit.asset_id, None)

            try:
                self.assets[commit.asset_id] = AssetStatus.IN_STOCK
                self.asset_details[commit.asset_id] = {
                    "asset_name": commit.asset_name,
                    "category_id": commit.category_id,
                    "location": commit.location,
                }
                self.records.append(commit)
            except Exception:
                self._restore_snapshot_locked(commit.asset_id)
                raise

            self._snapshots.pop(commit.asset_id, None)
            LOGGER.info(
                "repository inbound commit success: asset_id=%s request_seq=%s hw_seq=%s hw_result=%s",
                commit.asset_id,
                commit.request_seq,
                commit.hw_seq,
                commit.hw_result,
            )
            return AssetStatus.IN_STOCK

    def rollback_transaction(self, asset_id: str, reason: str) -> None:
        with self._lock:
            restored = self._restore_snapshot_locked(asset_id)
        LOGGER.warning("repository rollback: asset_id=%s restored=%s reason=%s", asset_id, restored, reason)

    def _snapshot_locked(self, asset_id: str, borrow_request_id: str | None) -> dict[str, Any]:
        return {
            "asset_existed": asset_id in self.assets,
            "previous_status": self.assets.get(asset_id),
            "detail_existed": asset_id in self.asset_details,
            "previous_detail": None if asset_id not in self.asset_details else dict(self.asset_details[asset_id]),
            "records_len": len(self.records),
            "borrow_request_id": borrow_request_id,
            "borrow_request": None
            if borrow_request_id is None or borrow_request_id not in self.borrow_requests
            else self._clone_borrow_request(self.borrow_requests[borrow_request_id]),
        }

    def _restore_snapshot_locked(self, asset_id: str) -> bool:
        snapshot = self._snapshots.pop(asset_id, None)
        if snapshot is None:
            return False

        if snapshot["asset_existed"] and snapshot["previous_status"] is not None:
            self.assets[asset_id] = snapshot["previous_status"]
        else:
            self.assets.pop(asset_id, None)

        if snapshot["detail_existed"] and snapshot["previous_detail"] is not None:
            self.asset_details[asset_id] = dict(snapshot["previous_detail"])
        else:
            self.asset_details.pop(asset_id, None)

        if len(self.records) > snapshot["records_len"]:
            del self.records[snapshot["records_len"] :]

        borrow_request_id = snapshot["borrow_request_id"]
        if borrow_request_id is not None:
            borrow_request = snapshot["borrow_request"]
            if borrow_request is None:
                self.borrow_requests.pop(borrow_request_id, None)
            else:
                self.borrow_requests[borrow_request_id] = self._clone_borrow_request(borrow_request)

        return True

    def _validate_borrow_request_for_consume_locked(self, request_id: str, record: OperationRecordInput) -> None:
        borrow_request = self.borrow_requests.get(request_id)
        if borrow_request is None:
            raise ValueError("借用申请不存在")
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

    @staticmethod
    def _clone_borrow_request(record: BorrowRequestRecord) -> BorrowRequestRecord:
        return replace(record)

    @staticmethod
    def _now_string() -> str:
        return datetime.now().isoformat(sep=" ", timespec="seconds")
