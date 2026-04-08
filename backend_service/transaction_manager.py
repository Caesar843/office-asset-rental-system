from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field

import runtime_paths  # noqa: F401
from models import (
    ActionType,
    ConfirmResult,
    HardwareUserActionEvent,
    PendingTransaction,
    TransactionState,
)
from protocol import Frame, MsgType

LOGGER = logging.getLogger(__name__)


class BusyTransactionError(RuntimeError):
    pass


@dataclass(slots=True)
class TransactionContext:
    pending: PendingTransaction
    event: threading.Event = field(default_factory=threading.Event)


@dataclass(slots=True)
class TransactionWaitResult:
    pending: PendingTransaction
    timed_out: bool = False


class TransactionManager:
    def __init__(self, hw_wait_grace_seconds: float = 5.0) -> None:
        self._pending_by_asset: dict[str, TransactionContext] = {}
        self._lock = threading.Lock()
        self._hw_wait_grace_seconds = hw_wait_grace_seconds

    def has_pending_transaction(self, asset_id: str) -> bool:
        with self._lock:
            return asset_id in self._pending_by_asset

    def get_transaction(self, asset_id: str) -> PendingTransaction | None:
        with self._lock:
            context = self._pending_by_asset.get(asset_id)
            return None if context is None else context.pending

    def create_transaction(
        self,
        asset_id: str,
        user_id: str,
        user_name: str,
        action_type: ActionType,
        request_id: str | None,
        request_seq: int,
    ) -> PendingTransaction:
        with self._lock:
            if asset_id in self._pending_by_asset:
                raise BusyTransactionError(f"asset already has pending transaction: {asset_id}")

            pending = PendingTransaction(
                asset_id=asset_id,
                user_id=user_id,
                user_name=user_name,
                action_type=action_type,
                request_id=request_id,
                request_seq=request_seq,
                state=TransactionState.WAIT_ACK,
            )
            self._pending_by_asset[asset_id] = TransactionContext(pending=pending)
            return pending

    def mark_ack_success(self, asset_id: str) -> PendingTransaction:
        with self._lock:
            context = self._get_context_locked(asset_id)
            pending = context.pending
            if pending.state != TransactionState.WAIT_ACK:
                raise ValueError(f"cannot move transaction to WAIT_HW from {pending.state.value}")
            pending.state = TransactionState.WAIT_HW
            return pending

    def mark_ack_failure(self, asset_id: str, reason: str) -> PendingTransaction:
        with self._lock:
            context = self._get_context_locked(asset_id)
            pending = context.pending
            if pending.state != TransactionState.WAIT_ACK:
                raise ValueError(f"cannot fail ack from {pending.state.value}")
            pending.state = TransactionState.FAILED
            pending.error_message = reason
            return pending

    def mark_failed(self, asset_id: str, reason: str) -> PendingTransaction:
        with self._lock:
            context = self._get_context_locked(asset_id)
            pending = context.pending
            if pending.state in (TransactionState.COMPLETED, TransactionState.FAILED):
                raise ValueError(f"cannot fail transaction from {pending.state.value}")
            pending.state = TransactionState.FAILED
            pending.error_message = reason
            return pending

    def wait_for_hw_result(self, asset_id: str, timeout_ms: int) -> TransactionWaitResult:
        with self._lock:
            context = self._get_context_locked(asset_id)

        wait_seconds = timeout_ms / 1000.0 + self._hw_wait_grace_seconds
        if context.event.wait(wait_seconds):
            return TransactionWaitResult(pending=context.pending, timed_out=False)

        with self._lock:
            current = self._pending_by_asset.get(asset_id)
            if current is context and context.pending.state == TransactionState.WAIT_HW:
                context.pending.state = TransactionState.FAILED
                context.pending.error_message = "等待 EVT_USER_ACTION 超时"

        LOGGER.error(
            "business hw result timeout: asset_id=%s request_seq=%s request_id=%s timeout_s=%.1f",
            context.pending.asset_id,
            context.pending.request_seq,
            context.pending.request_id,
            wait_seconds,
        )
        return TransactionWaitResult(pending=context.pending, timed_out=True)

    def handle_frame(self, frame: Frame) -> None:
        if frame.msg_type != MsgType.EVT_USER_ACTION:
            LOGGER.info("transaction manager ignored frame: msg_type=%s", frame.msg_type.name)
            return

        event = self._build_hw_event(frame)
        if event is None:
            return

        with self._lock:
            context = self._pending_by_asset.get(event.asset_id)
            if context is None:
                LOGGER.warning(
                    "late/orphan EVT_USER_ACTION ignored: asset_id=%s request_seq=%s action=%s hw_seq=%s request_id=%s",
                    event.asset_id,
                    event.request_seq,
                    event.action_type.value,
                    event.hw_seq,
                    event.request_id,
                )
                return

            pending = context.pending
            if pending.response_received or context.event.is_set():
                LOGGER.warning(
                    "duplicate EVT_USER_ACTION ignored: asset_id=%s expected_request_seq=%s hw_seq=%s",
                    pending.asset_id,
                    pending.request_seq,
                    event.hw_seq,
                )
                return

            if pending.state != TransactionState.WAIT_HW:
                LOGGER.warning(
                    "illegal timing EVT_USER_ACTION ignored: asset_id=%s state=%s request_seq=%s hw_seq=%s",
                    pending.asset_id,
                    pending.state.value,
                    pending.request_seq,
                    event.hw_seq,
                )
                return

            if event.request_seq != pending.request_seq:
                LOGGER.warning(
                    "mismatched EVT_USER_ACTION request_seq ignored: asset_id=%s expected=%s actual=%s hw_seq=%s",
                    pending.asset_id,
                    pending.request_seq,
                    event.request_seq,
                    event.hw_seq,
                )
                return

            if event.action_type != pending.action_type:
                LOGGER.warning(
                    "mismatched EVT_USER_ACTION action ignored: asset_id=%s expected=%s actual=%s request_seq=%s",
                    pending.asset_id,
                    pending.action_type.value,
                    event.action_type.value,
                    event.request_seq,
                )
                return

            if pending.request_id is not None:
                if event.request_id is None:
                    LOGGER.warning(
                        "missing EVT_USER_ACTION request_id ignored: asset_id=%s expected=%s request_seq=%s hw_seq=%s",
                        pending.asset_id,
                        pending.request_id,
                        event.request_seq,
                        event.hw_seq,
                    )
                    return
                if event.request_id != pending.request_id:
                    LOGGER.warning(
                        "mismatched EVT_USER_ACTION request_id ignored: asset_id=%s expected=%s actual=%s request_seq=%s hw_seq=%s",
                        pending.asset_id,
                        pending.request_id,
                        event.request_id,
                        event.request_seq,
                        event.hw_seq,
                    )
                    return

            pending.hw_seq = event.hw_seq
            pending.hw_result = event.confirm_result
            pending.hw_sn = event.hw_sn
            pending.response_received = True
            if event.confirm_result == ConfirmResult.CONFIRMED.value and pending.hw_seq is not None:
                pending.state = TransactionState.UPDATING
            else:
                pending.state = TransactionState.FAILED
                pending.error_message = self._hw_failure_message(event.confirm_result)

            LOGGER.info(
                "business hw result matched: asset_id=%s request_seq=%s request_id=%s hw_seq=%s hw_result=%s",
                pending.asset_id,
                pending.request_seq,
                pending.request_id,
                pending.hw_seq,
                pending.hw_result,
            )
            context.event.set()

    def mark_commit_success(self, asset_id: str) -> PendingTransaction:
        with self._lock:
            context = self._get_context_locked(asset_id)
            if context.pending.state != TransactionState.UPDATING:
                raise ValueError(f"cannot complete commit from {context.pending.state.value}")
            context.pending.state = TransactionState.COMPLETED
            return context.pending

    def mark_commit_failed(self, asset_id: str, reason: str) -> PendingTransaction:
        with self._lock:
            context = self._get_context_locked(asset_id)
            if context.pending.state != TransactionState.UPDATING:
                raise ValueError(f"cannot fail commit from {context.pending.state.value}")
            context.pending.state = TransactionState.FAILED
            context.pending.error_message = reason
            return context.pending

    def remove_transaction(self, asset_id: str) -> None:
        with self._lock:
            self._pending_by_asset.pop(asset_id, None)

    def _get_context_locked(self, asset_id: str) -> TransactionContext:
        context = self._pending_by_asset.get(asset_id)
        if context is None:
            raise LookupError(f"transaction not found: {asset_id}")
        return context

    def _build_hw_event(self, frame: Frame) -> HardwareUserActionEvent | None:
        payload = frame.payload if isinstance(frame.payload, dict) else {}
        asset_id = str(payload.get("asset_id", "")).strip()
        raw_request_seq = payload.get("request_seq")
        raw_action_type = str(payload.get("action_type", "")).strip()
        raw_request_id = payload.get("request_id")
        request_id = None if raw_request_id is None else str(raw_request_id).strip() or None

        if not asset_id or raw_request_seq is None or not raw_action_type:
            LOGGER.warning(
                "discard EVT_USER_ACTION with missing correlation fields: hw_seq=%s payload=%s",
                frame.seq_id,
                payload,
            )
            return None

        try:
            request_seq = int(raw_request_seq)
        except (TypeError, ValueError):
            LOGGER.warning(
                "discard EVT_USER_ACTION with invalid request_seq: hw_seq=%s payload=%s",
                frame.seq_id,
                payload,
            )
            return None

        try:
            action_type = ActionType(raw_action_type)
        except ValueError:
            LOGGER.warning(
                "discard EVT_USER_ACTION with invalid action_type: hw_seq=%s payload=%s",
                frame.seq_id,
                payload,
            )
            return None

        confirm_result = str(payload.get("confirm_result", "")).strip() or ConfirmResult.INTERNAL_ERROR.value
        hw_sn = str(payload.get("hw_sn", "")).strip() or None
        return HardwareUserActionEvent(
            hw_seq=frame.seq_id,
            asset_id=asset_id,
            request_seq=request_seq,
            request_id=request_id,
            action_type=action_type,
            confirm_result=confirm_result,
            hw_sn=hw_sn,
        )

    @staticmethod
    def _hw_failure_message(hw_result: str) -> str:
        if hw_result == ConfirmResult.CANCELLED.value:
            return "用户已在硬件端取消操作"
        if hw_result == ConfirmResult.TIMEOUT.value:
            return "用户在硬件端确认超时"
        if hw_result == ConfirmResult.BUSY.value:
            return "设备忙，请稍后重试"
        return "硬件返回未知结果"
