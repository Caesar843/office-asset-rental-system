from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from typing import Any, Protocol

from models import (
    ActionType,
    AssetStatus,
    BusinessResult,
    ConfirmResult,
    DeviceStatus,
    OperationRecordInput,
    PendingTransaction,
    TransactionState,
)
from protocol import Frame, MsgType
from serial_manager import SendResult, SerialManager

LOGGER = logging.getLogger(__name__)


class TransactionRepository(Protocol):
    def get_asset_status(self, asset_id: str) -> AssetStatus | None: ...

    def apply_operation_atomically(self, record: OperationRecordInput) -> AssetStatus: ...

    def rollback_transaction(self, asset_id: str, reason: str) -> None: ...


def validate_asset_transition(current_status: AssetStatus, action_type: ActionType) -> str | None:
    if action_type == ActionType.BORROW:
        if current_status == AssetStatus.IN_STOCK:
            return None
        if current_status == AssetStatus.BORROWED:
            return "资产当前处于借出状态，不允许再次发起借出"
        if current_status == AssetStatus.MAINTENANCE:
            return "资产当前处于维修状态，禁止发起借出"
        return "资产当前处于报废状态，禁止发起借出"

    if current_status == AssetStatus.BORROWED:
        return None
    if current_status == AssetStatus.IN_STOCK:
        return "资产当前处于在库状态，不允许发起归还"
    if current_status == AssetStatus.MAINTENANCE:
        return "资产当前处于维修状态，禁止发起归还"
    return "资产当前处于报废状态，禁止发起归还"


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
                new_status = AssetStatus.BORROWED if record.action_type == ActionType.BORROW else AssetStatus.IN_STOCK
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


@dataclass(slots=True)
class TransactionContext:
    pending: PendingTransaction
    event: threading.Event = field(default_factory=threading.Event)


class AssetConfirmService:
    """
    Business service layer suitable for CLI, FastAPI or Flask integration.

    Serial IO stays in SerialManager. This layer owns request correlation,
    business rules, and transaction commit policy.
    """

    def __init__(
        self,
        serial_manager: SerialManager,
        repository: TransactionRepository | None = None,
    ) -> None:
        self.serial_manager = serial_manager
        self.repository = repository or InMemoryTransactionRepository()

        self._pending_by_asset: dict[str, TransactionContext] = {}
        self._lock = threading.Lock()
        self._device_status = DeviceStatus.UNKNOWN

        self.serial_manager.set_frame_handler(self._on_frame)
        self.serial_manager.set_status_handler(self._on_status_changed)

    @property
    def device_status(self) -> DeviceStatus:
        return self._device_status

    def open(self) -> None:
        self.serial_manager.open()

    def close(self) -> None:
        self.serial_manager.close()

    def request_asset_borrow_confirm(
        self,
        asset_id: str,
        user_id: str,
        user_name: str,
        timeout_ms: int = 30000,
    ) -> BusinessResult:
        return self._request_action(
            asset_id=asset_id,
            user_id=user_id,
            user_name=user_name,
            action_type=ActionType.BORROW,
            timeout_ms=timeout_ms,
        )

    def request_asset_return_confirm(
        self,
        asset_id: str,
        user_id: str,
        user_name: str,
        timeout_ms: int = 30000,
    ) -> BusinessResult:
        return self._request_action(
            asset_id=asset_id,
            user_id=user_id,
            user_name=user_name,
            action_type=ActionType.RETURN,
            timeout_ms=timeout_ms,
        )

    def get_asset_status(self, asset_id: str) -> AssetStatus | None:
        return self.repository.get_asset_status(asset_id)

    def _request_action(
        self,
        asset_id: str,
        user_id: str,
        user_name: str,
        action_type: ActionType,
        timeout_ms: int,
    ) -> BusinessResult:
        if self.device_status == DeviceStatus.OFFLINE:
            return self._build_result(
                success=False,
                code=ConfirmResult.DEVICE_OFFLINE.value,
                message="设备离线，无法发起确认请求",
                asset_id=asset_id,
                action_type=action_type,
                user_id=user_id,
                user_name=user_name,
                seq_id=-1,
                request_seq=None,
                request_id=None,
                state=TransactionState.FAILED,
            )

        current_status = self.get_asset_status(asset_id)
        if current_status is None:
            LOGGER.warning("request blocked: asset not found asset_id=%s action=%s", asset_id, action_type.value)
            return self._build_result(
                success=False,
                code=ConfirmResult.ASSET_NOT_FOUND.value,
                message="资产不存在，无法发起确认请求",
                asset_id=asset_id,
                action_type=action_type,
                user_id=user_id,
                user_name=user_name,
                seq_id=-1,
                request_seq=None,
                request_id=None,
                state=TransactionState.FAILED,
            )
        invalid_reason = validate_asset_transition(current_status, action_type)
        if invalid_reason is not None:
            LOGGER.warning(
                "state validation blocked request: asset_id=%s action=%s status=%s reason=%s",
                asset_id,
                action_type.value,
                current_status.value,
                invalid_reason,
            )
            return self._build_result(
                success=False,
                code=ConfirmResult.STATE_INVALID.value,
                message=invalid_reason,
                asset_id=asset_id,
                action_type=action_type,
                user_id=user_id,
                user_name=user_name,
                seq_id=-1,
                request_seq=None,
                request_id=None,
                state=TransactionState.FAILED,
                extra={"asset_status": current_status.value},
            )

        with self._lock:
            if asset_id in self._pending_by_asset:
                return self._build_result(
                    success=False,
                    code=ConfirmResult.BUSY.value,
                    message="该资产已有待确认事务，请勿重复提交",
                    asset_id=asset_id,
                    action_type=action_type,
                    user_id=user_id,
                    user_name=user_name,
                    seq_id=-1,
                    request_seq=None,
                    request_id=None,
                    state=TransactionState.FAILED,
                    extra={"asset_status": current_status.value},
                )
            request_id = uuid.uuid4().hex
            request_seq = self.serial_manager.reserve_seq_id()
            tx = PendingTransaction(
                asset_id=asset_id,
                user_id=user_id,
                user_name=user_name,
                action_type=action_type,
                request_id=request_id,
                request_seq=request_seq,
                state=TransactionState.WAIT_ACK,
            )
            context = TransactionContext(pending=tx)
            self._pending_by_asset[asset_id] = context

        try:
            payload = {
                "asset_id": asset_id,
                "action_type": action_type.value,
                "user_id": user_id,
                "user_name": user_name,
                "request_id": request_id,
                "request_seq": request_seq,
                "wait_timeout": timeout_ms,
            }
            send_result: SendResult = self.serial_manager.send_request(MsgType.CMD_REQ_CONFIRM, payload, seq_id=request_seq)
            if not send_result.success:
                tx.state = TransactionState.FAILED
                return self._result_from_send_failure(tx, send_result, current_status)

            tx.state = TransactionState.WAIT_HW
            LOGGER.info(
                "business waiting hw result: asset_id=%s action=%s request_seq=%s request_id=%s timeout_ms=%s",
                asset_id,
                action_type.value,
                tx.request_seq,
                tx.request_id,
                timeout_ms,
            )
            wait_seconds = timeout_ms / 1000.0 + 5.0
            if not context.event.wait(wait_seconds):
                tx.state = TransactionState.FAILED
                tx.error_message = "等待 EVT_USER_ACTION 超时"
                LOGGER.error(
                    "business hw result timeout: asset_id=%s request_seq=%s request_id=%s timeout_s=%.1f",
                    asset_id,
                    tx.request_seq,
                    tx.request_id,
                    wait_seconds,
                )
                return self._build_result(
                    success=False,
                    code=ConfirmResult.HW_RESULT_TIMEOUT.value,
                    message="已收到 ACK，但等待硬件确认结果超时",
                    asset_id=asset_id,
                    action_type=action_type,
                    user_id=user_id,
                    user_name=user_name,
                    seq_id=tx.request_seq,
                    request_seq=tx.request_seq,
                    request_id=tx.request_id,
                    state=tx.state,
                    extra={"asset_status": current_status.value},
                )

            return self._finalize_transaction(tx)
        finally:
            with self._lock:
                self._pending_by_asset.pop(asset_id, None)

    def _on_frame(self, frame: Frame) -> None:
        if frame.msg_type != MsgType.EVT_USER_ACTION:
            LOGGER.info("business ignored frame: msg_type=%s", frame.msg_type.name)
            return

        payload = frame.payload if isinstance(frame.payload, dict) else {}
        asset_id = str(payload.get("asset_id", "")).strip()
        raw_request_seq = payload.get("request_seq")
        raw_action_type = str(payload.get("action_type", "")).strip()
        has_request_id = "request_id" in payload
        raw_request_id = payload.get("request_id") if has_request_id else None
        request_id = None if raw_request_id is None else str(raw_request_id).strip() or None

        if not asset_id or raw_request_seq is None or not raw_action_type:
            LOGGER.warning(
                "discard EVT_USER_ACTION with missing correlation fields: hw_seq=%s payload=%s",
                frame.seq_id,
                payload,
            )
            return

        try:
            request_seq = int(raw_request_seq)
        except (TypeError, ValueError):
            LOGGER.warning("discard EVT_USER_ACTION with invalid request_seq: hw_seq=%s payload=%s", frame.seq_id, payload)
            return

        try:
            action_type = ActionType(raw_action_type)
        except ValueError:
            LOGGER.warning("discard EVT_USER_ACTION with invalid action_type: hw_seq=%s payload=%s", frame.seq_id, payload)
            return

        with self._lock:
            context = self._pending_by_asset.get(asset_id)
        if context is None:
            LOGGER.warning(
                "late/orphan EVT_USER_ACTION ignored: asset_id=%s request_seq=%s action=%s hw_seq=%s request_id=%s",
                asset_id,
                request_seq,
                action_type.value,
                frame.seq_id,
                request_id,
            )
            return

        tx = context.pending
        if tx.response_received or context.event.is_set():
            LOGGER.warning(
                "duplicate EVT_USER_ACTION ignored: asset_id=%s expected_request_seq=%s hw_seq=%s",
                asset_id,
                tx.request_seq,
                frame.seq_id,
            )
            return

        if request_seq != tx.request_seq:
            LOGGER.warning(
                "mismatched EVT_USER_ACTION request_seq ignored: asset_id=%s expected=%s actual=%s hw_seq=%s",
                asset_id,
                tx.request_seq,
                request_seq,
                frame.seq_id,
            )
            return

        if action_type != tx.action_type:
            LOGGER.warning(
                "mismatched EVT_USER_ACTION action ignored: asset_id=%s expected=%s actual=%s request_seq=%s",
                asset_id,
                tx.action_type.value,
                action_type.value,
                request_seq,
            )
            return

        if tx.request_id is not None:
            if not has_request_id or raw_request_id is None:
                LOGGER.warning(
                    "missing EVT_USER_ACTION request_id ignored: asset_id=%s expected=%s request_seq=%s hw_seq=%s",
                    asset_id,
                    tx.request_id,
                    request_seq,
                    frame.seq_id,
                )
                return
            if request_id is None:
                LOGGER.warning(
                    "empty EVT_USER_ACTION request_id ignored: asset_id=%s expected=%s actual=%r request_seq=%s hw_seq=%s",
                    asset_id,
                    tx.request_id,
                    raw_request_id,
                    request_seq,
                    frame.seq_id,
                )
                return
            if request_id != tx.request_id:
                LOGGER.warning(
                    "mismatched EVT_USER_ACTION request_id ignored: asset_id=%s expected=%s actual=%s request_seq=%s hw_seq=%s",
                    asset_id,
                    tx.request_id,
                    request_id,
                    request_seq,
                    frame.seq_id,
                )
                return

        tx.hw_seq = frame.seq_id
        tx.hw_result = str(payload.get("confirm_result", "")).strip() or ConfirmResult.INTERNAL_ERROR.value
        tx.hw_sn = str(payload.get("hw_sn", "")).strip() or None
        tx.response_received = True
        LOGGER.info(
            "business hw result matched: asset_id=%s request_seq=%s request_id=%s hw_seq=%s hw_result=%s",
            asset_id,
            tx.request_seq,
            tx.request_id,
            tx.hw_seq,
            tx.hw_result,
        )
        context.event.set()

    def _on_status_changed(self, status: DeviceStatus) -> None:
        self._device_status = status
        if status == DeviceStatus.OFFLINE:
            LOGGER.warning("service observed device offline")

    def _finalize_transaction(self, tx: PendingTransaction) -> BusinessResult:
        current_status = self.get_asset_status(tx.asset_id)
        if current_status is None:
            tx.state = TransactionState.FAILED
            return self._build_result(
                success=False,
                code=ConfirmResult.ASSET_NOT_FOUND.value,
                message="资产不存在，无法提交业务结果",
                asset_id=tx.asset_id,
                action_type=tx.action_type,
                user_id=tx.user_id,
                user_name=tx.user_name,
                seq_id=tx.request_seq,
                request_seq=tx.request_seq,
                request_id=tx.request_id,
                hw_seq=tx.hw_seq,
                hw_result=tx.hw_result,
                hw_sn=tx.hw_sn,
                state=tx.state,
            )
        hw_result = tx.hw_result or ConfirmResult.INTERNAL_ERROR.value
        try:
            confirm_result = ConfirmResult(hw_result)
        except ValueError:
            confirm_result = ConfirmResult.INTERNAL_ERROR

        if confirm_result != ConfirmResult.CONFIRMED:
            tx.state = TransactionState.FAILED
            message_map = {
                ConfirmResult.CANCELLED: "用户已在硬件端取消操作",
                ConfirmResult.TIMEOUT: "用户在硬件端确认超时",
                ConfirmResult.BUSY: "设备忙，请稍后重试",
            }
            message = message_map.get(confirm_result, "硬件返回未知结果")
            LOGGER.warning(
                "business failed by hw result: asset_id=%s action=%s request_seq=%s hw_seq=%s hw_result=%s",
                tx.asset_id,
                tx.action_type.value,
                tx.request_seq,
                tx.hw_seq,
                hw_result,
            )
            return self._build_result(
                success=False,
                code=confirm_result.value,
                message=message,
                asset_id=tx.asset_id,
                action_type=tx.action_type,
                user_id=tx.user_id,
                user_name=tx.user_name,
                seq_id=tx.request_seq,
                request_seq=tx.request_seq,
                request_id=tx.request_id,
                hw_seq=tx.hw_seq,
                hw_result=hw_result,
                hw_sn=tx.hw_sn,
                state=tx.state,
                extra={"asset_status": current_status.value},
            )

        if tx.hw_seq is None:
            tx.state = TransactionState.FAILED
            return self._build_result(
                success=False,
                code=ConfirmResult.INTERNAL_ERROR.value,
                message="缺少硬件序列号，拒绝提交业务结果",
                asset_id=tx.asset_id,
                action_type=tx.action_type,
                user_id=tx.user_id,
                user_name=tx.user_name,
                seq_id=tx.request_seq,
                request_seq=tx.request_seq,
                request_id=tx.request_id,
                state=tx.state,
                extra={"asset_status": current_status.value},
            )

        tx.state = TransactionState.UPDATING
        record = OperationRecordInput(
            asset_id=tx.asset_id,
            user_id=tx.user_id,
            user_name=tx.user_name,
            action_type=tx.action_type,
            request_seq=tx.request_seq,
            request_id=tx.request_id,
            hw_seq=tx.hw_seq,
            hw_result=hw_result,
            hw_sn=tx.hw_sn,
            due_time=None,
        )

        try:
            new_status = self.repository.apply_operation_atomically(record)
        except LookupError as exc:
            self.repository.rollback_transaction(tx.asset_id, str(exc))
            tx.state = TransactionState.FAILED
            LOGGER.warning("business asset missing during commit: asset_id=%s reason=%s", tx.asset_id, exc)
            return self._build_result(
                success=False,
                code=ConfirmResult.ASSET_NOT_FOUND.value,
                message=str(exc),
                asset_id=tx.asset_id,
                action_type=tx.action_type,
                user_id=tx.user_id,
                user_name=tx.user_name,
                seq_id=tx.request_seq,
                request_seq=tx.request_seq,
                request_id=tx.request_id,
                hw_seq=tx.hw_seq,
                hw_result=hw_result,
                hw_sn=tx.hw_sn,
                state=tx.state,
            )
        except ValueError as exc:
            self.repository.rollback_transaction(tx.asset_id, str(exc))
            tx.state = TransactionState.FAILED
            LOGGER.warning("business state validation failed during commit: asset_id=%s reason=%s", tx.asset_id, exc)
            return self._build_result(
                success=False,
                code=ConfirmResult.STATE_INVALID.value,
                message=str(exc),
                asset_id=tx.asset_id,
                action_type=tx.action_type,
                user_id=tx.user_id,
                user_name=tx.user_name,
                seq_id=tx.request_seq,
                request_seq=tx.request_seq,
                request_id=tx.request_id,
                hw_seq=tx.hw_seq,
                hw_result=hw_result,
                hw_sn=tx.hw_sn,
                state=tx.state,
                extra={"asset_status": self.get_asset_status(tx.asset_id).value},
            )
        except Exception as exc:
            LOGGER.exception("business atomic commit failed: asset_id=%s request_seq=%s", tx.asset_id, tx.request_seq)
            self.repository.rollback_transaction(tx.asset_id, str(exc))
            tx.state = TransactionState.FAILED
            return self._build_result(
                success=False,
                code=ConfirmResult.INTERNAL_ERROR.value,
                message=f"业务更新失败: {exc}",
                asset_id=tx.asset_id,
                action_type=tx.action_type,
                user_id=tx.user_id,
                user_name=tx.user_name,
                seq_id=tx.request_seq,
                request_seq=tx.request_seq,
                request_id=tx.request_id,
                hw_seq=tx.hw_seq,
                hw_result=hw_result,
                hw_sn=tx.hw_sn,
                state=tx.state,
                extra={"asset_status": self.get_asset_status(tx.asset_id).value},
            )

        tx.state = TransactionState.COMPLETED
        LOGGER.info(
            "business success: asset_id=%s action=%s request_seq=%s request_id=%s hw_seq=%s",
            tx.asset_id,
            tx.action_type.value,
            tx.request_seq,
            tx.request_id,
            tx.hw_seq,
        )
        message = "用户已在硬件端确认借出" if tx.action_type == ActionType.BORROW else "用户已在硬件端确认归还"
        return self._build_result(
            success=True,
            code=ConfirmResult.CONFIRMED.value,
            message=message,
            asset_id=tx.asset_id,
            action_type=tx.action_type,
            user_id=tx.user_id,
            user_name=tx.user_name,
            seq_id=tx.request_seq,
            request_seq=tx.request_seq,
            request_id=tx.request_id,
            hw_seq=tx.hw_seq,
            hw_result=hw_result,
            hw_sn=tx.hw_sn,
            state=tx.state,
            extra={"asset_status": new_status.value},
        )

    def _result_from_send_failure(
        self,
        tx: PendingTransaction,
        send_result: SendResult,
        current_status: AssetStatus,
    ) -> BusinessResult:
        code = ConfirmResult.INTERNAL_ERROR.value
        if send_result.ack_type == MsgType.ACK_BUSY:
            code = ConfirmResult.BUSY.value
        elif send_result.ack_type == MsgType.ACK_INVALID:
            code = ConfirmResult.ACK_INVALID.value
        elif send_result.ack_type == MsgType.ACK_ERROR:
            code = ConfirmResult.ACK_ERROR.value
        elif send_result.ack_type is None:
            code = ConfirmResult.ACK_TIMEOUT.value

        LOGGER.error(
            "business send failed: asset_id=%s action=%s request_seq=%s request_id=%s message=%s",
            tx.asset_id,
            tx.action_type.value,
            tx.request_seq,
            tx.request_id,
            send_result.message,
        )
        return self._build_result(
            success=False,
            code=code,
            message=send_result.message,
            asset_id=tx.asset_id,
            action_type=tx.action_type,
            user_id=tx.user_id,
            user_name=tx.user_name,
            seq_id=tx.request_seq,
            request_seq=tx.request_seq,
            request_id=tx.request_id,
            state=tx.state,
            extra={"asset_status": current_status.value},
        )

    def _build_result(
        self,
        success: bool,
        code: str,
        message: str,
        asset_id: str,
        action_type: ActionType,
        user_id: str,
        user_name: str,
        seq_id: int,
        request_seq: int | None,
        request_id: str | None,
        state: TransactionState,
        hw_seq: int | None = None,
        hw_result: str | None = None,
        hw_sn: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> BusinessResult:
        return BusinessResult(
            success=success,
            code=code,
            message=message,
            asset_id=asset_id,
            action_type=action_type.value,
            user_id=user_id,
            user_name=user_name,
            seq_id=seq_id,
            request_seq=request_seq,
            request_id=request_id,
            hw_seq=hw_seq,
            hw_result=hw_result,
            hw_sn=hw_sn,
            device_status=self.device_status,
            transaction_state=state,
            extra=extra or {},
        )
