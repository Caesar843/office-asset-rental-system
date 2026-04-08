from __future__ import annotations

import logging
import threading
from typing import Protocol

from asset_lifecycle import next_asset_status_for_action, validate_asset_transition
from models import AssetStatus, OperationRecordInput

LOGGER = logging.getLogger(__name__)


class TransactionRepository(Protocol):
    def get_asset_status(self, asset_id: str) -> AssetStatus | None: ...

    def apply_operation_atomically(self, record: OperationRecordInput) -> AssetStatus: ...

    def rollback_transaction(self, asset_id: str, reason: str) -> None: ...


class InMemoryTransactionRepository:
    """
    Demo repository with explicit atomic commit semantics.

    The method apply_operation_atomically owns the transaction boundary:
    state validation + asset update + operation_records append either all
    succeed or the in-memory snapshot is restored.
    """

    def __init__(self, initial_assets: dict[str, AssetStatus] | None = None) -> None:
        self.assets: dict[str, AssetStatus] = dict(initial_assets or {})
        self.records: list[OperationRecordInput] = []
        self._lock = threading.Lock()
        self._snapshots: dict[str, tuple[bool, AssetStatus | None, int]] = {}

    def get_asset_status(self, asset_id: str) -> AssetStatus | None:
        with self._lock:
            return self.assets.get(asset_id)

    def apply_operation_atomically(self, record: OperationRecordInput) -> AssetStatus:
        with self._lock:
            current_status = self.assets.get(record.asset_id)
            if current_status is None:
                raise LookupError(f"资产不存在: {record.asset_id}")

            invalid_reason = validate_asset_transition(current_status, record.action_type)
            if invalid_reason is not None:
                raise ValueError(invalid_reason)

            existed = record.asset_id in self.assets
            previous_status = self.assets.get(record.asset_id)
            snapshot = (existed, previous_status, len(self.records))
            self._snapshots[record.asset_id] = snapshot

            try:
                new_status = next_asset_status_for_action(record.action_type)
                self.assets[record.asset_id] = new_status
                self.records.append(record)
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

    def rollback_transaction(self, asset_id: str, reason: str) -> None:
        with self._lock:
            restored = self._restore_snapshot_locked(asset_id)
        LOGGER.warning("repository rollback: asset_id=%s restored=%s reason=%s", asset_id, restored, reason)

    def _restore_snapshot_locked(self, asset_id: str) -> bool:
        snapshot = self._snapshots.pop(asset_id, None)
        if snapshot is None:
            return False

        existed, previous_status, records_len = snapshot
        if existed and previous_status is not None:
            self.assets[asset_id] = previous_status
        else:
            self.assets.pop(asset_id, None)

        if len(self.records) > records_len:
            del self.records[records_len:]
        return True
