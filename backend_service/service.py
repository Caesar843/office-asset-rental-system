from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime
from typing import Any, Callable

import runtime_paths  # noqa: F401
from models import (
    ActionType,
    AssetStatus,
    BorrowApprovalCommand,
    BorrowCommand,
    BorrowRequestActionResult,
    BorrowRequestCreateCommand,
    BorrowRequestCreateInput,
    BorrowRequestReviewInput,
    BorrowRequestStatus,
    BusinessResult,
    ConfirmResult,
    DeviceStatus,
    InboundCommand,
    InboundCommitInput,
    InboundRuleCheckRequest,
    OperationRecordInput,
    PendingTransaction,
    ReturnCommand,
    RuleCheckRequest,
    TransactionState,
)
from protocol import Frame, MsgType
from repository import TransactionRepository
from rule_service import RuleService
from serial_manager import SendResult, SerialManager
from transaction_manager import BusyTransactionError, TransactionManager

LOGGER = logging.getLogger(__name__)
DEFAULT_INBOUND_ADMIN_USER_IDS = ("ADMIN", "U-ADMIN")


def _load_inbound_admin_user_ids() -> set[str]:
    raw = os.getenv("BACKEND_INBOUND_ADMIN_USER_IDS") or os.getenv("BACKEND_ADMIN_USER_IDS") or ""
    configured = {item.strip() for item in raw.split(",") if item.strip()}
    if configured:
        return configured
    return set(DEFAULT_INBOUND_ADMIN_USER_IDS)


class AssetConfirmService:
    """
    Business service layer suitable for CLI, FastAPI or Flask integration.

    Serial IO stays in SerialManager. This layer owns orchestration only:
    rule check, transaction lifecycle coordination, repository commit and
    final BusinessResult assembly.
    """

    def __init__(
        self,
        serial_manager: SerialManager,
        repository: TransactionRepository,
        rule_service: RuleService | None = None,
        transaction_manager: TransactionManager | None = None,
        status_callback: Callable[[dict[str, Any]], None] | None = None,
        inbound_admin_user_ids: set[str] | None = None,
    ) -> None:
        self.serial_manager = serial_manager
        self.repository = repository
        self.rule_service = rule_service or RuleService()
        self.transaction_manager = transaction_manager or TransactionManager()
        self._status_callback = status_callback
        self._device_status = DeviceStatus.UNKNOWN
        self._inbound_admin_user_ids = {
            user_id.strip() for user_id in (inbound_admin_user_ids or _load_inbound_admin_user_ids()) if user_id.strip()
        }

        self.serial_manager.set_frame_handler(self._on_frame)
        self.serial_manager.set_status_handler(self._on_status_changed)

    @property
    def device_status(self) -> DeviceStatus:
        return self._device_status

    def open(self) -> None:
        self.serial_manager.open()

    def close(self) -> None:
        self.serial_manager.close()

    def update_device_status(self, status: DeviceStatus) -> None:
        self._on_status_changed(status)

    def request_borrow(self, command: BorrowCommand) -> BusinessResult:
        return self._request_action(
            asset_id=command.asset_id,
            user_id=command.user_id,
            user_name=command.user_name,
            action_type=ActionType.BORROW,
            timeout_ms=command.timeout_ms,
        )

    def request_return(self, command: ReturnCommand) -> BusinessResult:
        return self._request_action(
            asset_id=command.asset_id,
            user_id=command.user_id,
            user_name=command.user_name,
            action_type=ActionType.RETURN,
            timeout_ms=command.timeout_ms,
        )

    def request_inbound(self, command: InboundCommand) -> BusinessResult:
        return self._request_inbound(command)

    def create_borrow_request(self, command: BorrowRequestCreateCommand) -> BorrowRequestActionResult:
        current_status = self.get_asset_status(command.asset_id)
        if current_status is None:
            return BorrowRequestActionResult(
                success=False,
                code=ConfirmResult.ASSET_NOT_FOUND.value,
                message="资产不存在，无法提交借用申请",
            )

        if current_status != AssetStatus.IN_STOCK:
            return BorrowRequestActionResult(
                success=False,
                code=ConfirmResult.STATE_INVALID.value,
                message="资产当前不可借用，无法提交借用申请",
            )

        if self.transaction_manager.has_pending_transaction(command.asset_id):
            return BorrowRequestActionResult(
                success=False,
                code=ConfirmResult.BUSY.value,
                message="该资产已有待确认事务，请勿重复提交",
            )

        pending_items = self.repository.list_borrow_requests(
            status=BorrowRequestStatus.PENDING,
            asset_id=command.asset_id,
        )
        approved_items = self.repository.list_borrow_requests(
            status=BorrowRequestStatus.APPROVED,
            asset_id=command.asset_id,
        )
        if pending_items or approved_items:
            return BorrowRequestActionResult(
                success=False,
                code=ConfirmResult.BUSY.value,
                message="该资产已有待处理借用申请，请勿重复提交",
            )

        requested_at = command.requested_at or datetime.now().isoformat(sep=" ", timespec="seconds")
        record = self.repository.create_borrow_request(
            BorrowRequestCreateInput(
                request_id=uuid.uuid4().hex,
                asset_id=command.asset_id,
                applicant_user_id=command.user_id,
                applicant_user_name=command.user_name,
                reason=command.reason,
                status=BorrowRequestStatus.PENDING,
                requested_at=requested_at,
            )
        )
        return BorrowRequestActionResult(
            success=True,
            code="REQUEST_CREATED",
            message="借用申请已提交，等待管理员审批",
            item=record,
        )

    def review_borrow_request(self, command: BorrowApprovalCommand) -> BorrowRequestActionResult:
        if not self._is_admin(command.reviewer_user_id):
            return BorrowRequestActionResult(
                success=False,
                code=ConfirmResult.PERMISSION_DENIED.value,
                message="当前用户无管理员审批权限",
            )

        current = self.repository.get_borrow_request(command.request_id)
        if current is None:
            return BorrowRequestActionResult(
                success=False,
                code=ConfirmResult.ASSET_NOT_FOUND.value,
                message="借用申请不存在",
            )

        if current.status != BorrowRequestStatus.PENDING:
            return BorrowRequestActionResult(
                success=False,
                code=ConfirmResult.STATE_INVALID.value,
                message="借用申请当前不是待审批状态，不能重复审批",
                item=current,
            )

        reviewed_at = command.reviewed_at or datetime.now().isoformat(sep=" ", timespec="seconds")
        next_status = BorrowRequestStatus.APPROVED if command.approved else BorrowRequestStatus.REJECTED
        record = self.repository.review_borrow_request(
            BorrowRequestReviewInput(
                request_id=command.request_id,
                status=next_status,
                reviewer_user_id=command.reviewer_user_id,
                reviewer_user_name=command.reviewer_user_name,
                review_comment=command.review_comment,
                reviewed_at=reviewed_at,
            )
        )
        return BorrowRequestActionResult(
            success=True,
            code="REQUEST_APPROVED" if command.approved else "REQUEST_REJECTED",
            message="借用申请已审批通过" if command.approved else "借用申请已审批拒绝",
            item=record,
        )

    def start_borrow_from_request(self, request_id: str, timeout_ms: int = 30000) -> BusinessResult:
        record = self.repository.get_borrow_request(request_id)
        if record is None:
            return self._return_with_status(
                self._build_result(
                    success=False,
                    code=ConfirmResult.ASSET_NOT_FOUND.value,
                    message="借用申请不存在",
                    asset_id="",
                    action_type=ActionType.BORROW,
                    user_id="",
                    user_name="",
                    seq_id=-1,
                    request_seq=None,
                    request_id=None,
                    state=TransactionState.FAILED,
                    extra={"borrow_request_id": request_id},
                )
            )

        if record.status != BorrowRequestStatus.APPROVED:
            message = "借用申请尚未审批通过，不能发起借出确认"
            if record.status == BorrowRequestStatus.REJECTED:
                message = "借用申请已被拒绝，不能发起借出确认"
            elif record.status == BorrowRequestStatus.CONSUMED:
                message = "借用申请已被使用，不能重复借出"
            return self._return_with_status(
                self._build_result(
                    success=False,
                    code=ConfirmResult.STATE_INVALID.value,
                    message=message,
                    asset_id=record.asset_id,
                    action_type=ActionType.BORROW,
                    user_id=record.applicant_user_id,
                    user_name=record.applicant_user_name,
                    seq_id=-1,
                    request_seq=None,
                    request_id=None,
                    state=TransactionState.FAILED,
                    extra={"borrow_request_id": record.request_id, "borrow_request_status": record.status.value},
                )
            )

        return self._request_action(
            asset_id=record.asset_id,
            user_id=record.applicant_user_id,
            user_name=record.applicant_user_name,
            action_type=ActionType.BORROW,
            timeout_ms=timeout_ms,
            extra={"borrow_request_id": record.request_id},
        )

    def request_asset_borrow_confirm(
        self,
        asset_id: str,
        user_id: str,
        user_name: str,
        timeout_ms: int = 30000,
    ) -> BusinessResult:
        return self.request_borrow(
            BorrowCommand(asset_id=asset_id, user_id=user_id, user_name=user_name, timeout_ms=timeout_ms)
        )

    def request_asset_return_confirm(
        self,
        asset_id: str,
        user_id: str,
        user_name: str,
        timeout_ms: int = 30000,
    ) -> BusinessResult:
        return self.request_return(
            ReturnCommand(asset_id=asset_id, user_id=user_id, user_name=user_name, timeout_ms=timeout_ms)
        )

    def request_asset_inbound_confirm(
        self,
        asset_id: str,
        user_id: str,
        user_name: str,
        asset_name: str,
        location: str,
        category_id: int | None,
        timeout_ms: int = 30000,
        request_source: str | None = None,
        raw_text: str | None = None,
        symbology: str | None = None,
    ) -> BusinessResult:
        return self.request_inbound(
            InboundCommand(
                asset_id=asset_id,
                user_id=user_id,
                user_name=user_name,
                asset_name=asset_name,
                category_id=category_id,
                location=location,
                timeout_ms=timeout_ms,
                request_source=request_source,
                raw_text=raw_text,
                symbology=symbology,
            )
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
        extra: dict[str, Any] | None = None,
    ) -> BusinessResult:
        current_status = self.get_asset_status(asset_id)
        rule_request = RuleCheckRequest(
            asset_id=asset_id,
            user_id=user_id,
            user_name=user_name,
            action_type=action_type,
            device_status=self.device_status,
            asset_status=current_status,
            has_pending_transaction=self.transaction_manager.has_pending_transaction(asset_id),
        )
        rule_result = self.rule_service.check_request(rule_request)
        if not rule_result.passed:
            return self._return_with_status(self._rule_result_to_business_result(rule_result, user_name))

        if current_status is None:
            raise AssertionError("rule check passed but asset status is missing")

        request_id = uuid.uuid4().hex
        request_seq = self.serial_manager.reserve_seq_id()

        try:
            tx = self.transaction_manager.create_transaction(
                asset_id=asset_id,
                user_id=user_id,
                user_name=user_name,
                action_type=action_type,
                request_id=request_id,
                request_seq=request_seq,
            )
        except BusyTransactionError:
            return self._return_with_status(
                self._build_result(
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
            )

        tx.extra = dict(extra or {})

        try:
            self._publish_pending_status(tx, code="WAITING_ACK", message="事务已创建，等待 ACK")
            payload = {
                "asset_id": asset_id,
                "action_type": action_type.value,
                "user_id": user_id,
                "user_name": user_name,
                "request_id": request_id,
                "request_seq": request_seq,
                "wait_timeout": timeout_ms,
            }
            send_result: SendResult = self.serial_manager.send_request(
                MsgType.CMD_REQ_CONFIRM,
                payload,
                seq_id=request_seq,
            )
            if not send_result.success:
                tx = self.transaction_manager.mark_ack_failure(asset_id, send_result.message)
                return self._return_with_status(self._result_from_send_failure(tx, send_result, current_status))

            tx = self.transaction_manager.mark_ack_success(asset_id)
            self._publish_pending_status(tx, code="WAITING_HW", message="已收到 ACK，等待硬件确认")
            LOGGER.info(
                "business waiting hw result: asset_id=%s action=%s request_seq=%s request_id=%s timeout_ms=%s",
                asset_id,
                action_type.value,
                tx.request_seq,
                tx.request_id,
                timeout_ms,
            )

            wait_result = self.transaction_manager.wait_for_hw_result(asset_id, timeout_ms)
            if wait_result.timed_out:
                return self._return_with_status(
                    self._build_result(
                        success=False,
                        code=ConfirmResult.HW_RESULT_TIMEOUT.value,
                        message="已收到 ACK，但等待硬件确认结果超时",
                        asset_id=asset_id,
                        action_type=action_type,
                        user_id=user_id,
                        user_name=user_name,
                        seq_id=wait_result.pending.request_seq,
                        request_seq=wait_result.pending.request_seq,
                        request_id=wait_result.pending.request_id,
                        state=wait_result.pending.state,
                        extra={"asset_status": current_status.value},
                    )
                )

            return self._return_with_status(self._finalize_transaction(wait_result.pending))
        finally:
            self.transaction_manager.remove_transaction(asset_id)

    def _request_inbound(self, command: InboundCommand) -> BusinessResult:
        rule_request = InboundRuleCheckRequest(
            asset_id=command.asset_id,
            user_id=command.user_id,
            user_name=command.user_name,
            action_type=ActionType.INBOUND,
            device_status=self.device_status,
            asset_status=self.get_asset_status(command.asset_id),
            has_pending_transaction=self.transaction_manager.has_pending_transaction(command.asset_id),
            asset_name=command.asset_name,
            category_id=command.category_id,
            location=command.location,
            has_inbound_permission=self._is_inbound_admin(command.user_id),
            category_exists=True if command.category_id is None else self.repository.category_exists(command.category_id),
        )
        rule_result = self.rule_service.check_request(rule_request)
        if not rule_result.passed:
            return self._return_with_status(self._rule_result_to_business_result(rule_result, command.user_name))

        request_id = uuid.uuid4().hex
        request_seq = self.serial_manager.reserve_seq_id()

        try:
            tx = self.transaction_manager.create_transaction(
                asset_id=command.asset_id,
                user_id=command.user_id,
                user_name=command.user_name,
                action_type=ActionType.INBOUND,
                request_id=request_id,
                request_seq=request_seq,
            )
        except BusyTransactionError:
            return self._return_with_status(
                self._build_result(
                    success=False,
                    code=ConfirmResult.BUSY.value,
                    message="该资产已有待确认事务，请勿重复提交",
                    asset_id=command.asset_id,
                    action_type=ActionType.INBOUND,
                    user_id=command.user_id,
                    user_name=command.user_name,
                    seq_id=-1,
                    request_seq=None,
                    request_id=None,
                    state=TransactionState.FAILED,
                    extra={},
                )
            )

        tx.extra = {
            "asset_name": command.asset_name,
            "category_id": command.category_id,
            "location": command.location,
            "request_source": command.request_source,
            "raw_text": command.raw_text,
            "symbology": command.symbology,
        }

        try:
            self._publish_pending_status(tx, code="WAITING_ACK", message="入库事务已创建，等待 ACK")
            payload = {
                "asset_id": command.asset_id,
                "action_type": ActionType.INBOUND.value,
                "user_id": command.user_id,
                "user_name": command.user_name,
                "request_id": request_id,
                "request_seq": request_seq,
                "wait_timeout": command.timeout_ms,
                "asset_name": command.asset_name,
                "category_id": command.category_id,
                "location": command.location,
            }
            send_result: SendResult = self.serial_manager.send_request(
                MsgType.CMD_REQ_CONFIRM,
                payload,
                seq_id=request_seq,
            )
            if not send_result.success:
                tx = self.transaction_manager.mark_ack_failure(command.asset_id, send_result.message)
                return self._return_with_status(self._result_from_send_failure(tx, send_result, current_status=None))

            tx = self.transaction_manager.mark_ack_success(command.asset_id)
            tx.extra.update(
                {
                    "asset_name": command.asset_name,
                    "category_id": command.category_id,
                    "location": command.location,
                    "request_source": command.request_source,
                    "raw_text": command.raw_text,
                    "symbology": command.symbology,
                }
            )
            self._publish_pending_status(tx, code="WAITING_HW", message="已收到 ACK，等待硬件确认入库")

            wait_result = self.transaction_manager.wait_for_hw_result(command.asset_id, command.timeout_ms)
            if wait_result.timed_out:
                return self._return_with_status(
                    self._build_result(
                        success=False,
                        code=ConfirmResult.HW_RESULT_TIMEOUT.value,
                        message="已收到 ACK，但等待硬件确认结果超时",
                        asset_id=command.asset_id,
                        action_type=ActionType.INBOUND,
                        user_id=command.user_id,
                        user_name=command.user_name,
                        seq_id=wait_result.pending.request_seq,
                        request_seq=wait_result.pending.request_seq,
                        request_id=wait_result.pending.request_id,
                        state=wait_result.pending.state,
                        extra={},
                    )
                )

            return self._return_with_status(self._finalize_inbound_transaction(wait_result.pending))
        finally:
            self.transaction_manager.remove_transaction(command.asset_id)

    def _on_frame(self, frame: Frame) -> None:
        self.transaction_manager.handle_frame(frame)

    def _on_status_changed(self, status: DeviceStatus) -> None:
        self._device_status = status
        if status == DeviceStatus.OFFLINE:
            LOGGER.warning("service observed device offline")
            self._publish_device_status(status)

    def _finalize_inbound_transaction(self, tx: PendingTransaction) -> BusinessResult:
        if tx.state != TransactionState.UPDATING:
            return self._result_from_runtime_failure(tx)

        hw_result = tx.hw_result or ConfirmResult.INTERNAL_ERROR.value
        if tx.hw_seq is None:
            tx = self.transaction_manager.mark_commit_failed(tx.asset_id, "缺少硬件序列号，拒绝提交入库结果")
            return self._build_result(
                success=False,
                code=ConfirmResult.INTERNAL_ERROR.value,
                message="缺少硬件序列号，拒绝提交入库结果",
                asset_id=tx.asset_id,
                action_type=tx.action_type,
                user_id=tx.user_id,
                user_name=tx.user_name,
                seq_id=tx.request_seq,
                request_seq=tx.request_seq,
                request_id=tx.request_id,
                hw_result=hw_result,
                hw_sn=tx.hw_sn,
                state=tx.state,
                extra={},
            )

        asset_name = str(tx.extra.get("asset_name") or "").strip()
        location = str(tx.extra.get("location") or "").strip()
        category_id = tx.extra.get("category_id")
        if not asset_name or not location:
            tx = self.transaction_manager.mark_commit_failed(tx.asset_id, "入库事务缺少资产基础信息")
            return self._build_result(
                success=False,
                code=ConfirmResult.INTERNAL_ERROR.value,
                message="入库事务缺少资产基础信息",
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
                extra={},
            )

        commit = InboundCommitInput(
            asset_id=tx.asset_id,
            asset_name=asset_name,
            category_id=category_id,
            location=location,
            user_id=tx.user_id,
            user_name=tx.user_name,
            request_seq=tx.request_seq,
            request_id=tx.request_id,
            hw_seq=tx.hw_seq,
            hw_result=hw_result,
            hw_sn=tx.hw_sn,
            op_time=datetime.now().isoformat(sep=" ", timespec="seconds"),
        )

        try:
            new_status = self.repository.apply_inbound_atomically(commit)
        except ValueError as exc:
            self.repository.rollback_transaction(tx.asset_id, str(exc))
            tx = self.transaction_manager.mark_commit_failed(tx.asset_id, str(exc))
            message = str(exc)
            code = (
                ConfirmResult.PARAM_INVALID.value
                if message.startswith("分类不存在")
                else ConfirmResult.STATE_INVALID.value
            )
            return self._build_result(
                success=False,
                code=code,
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
                extra={},
            )
        except Exception as exc:
            LOGGER.exception("business inbound commit failed: asset_id=%s request_seq=%s", tx.asset_id, tx.request_seq)
            self.repository.rollback_transaction(tx.asset_id, str(exc))
            tx = self.transaction_manager.mark_commit_failed(tx.asset_id, str(exc))
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
                extra={},
            )

        tx = self.transaction_manager.mark_commit_success(tx.asset_id)
        return self._build_result(
            success=True,
            code=ConfirmResult.CONFIRMED.value,
            message="用户已在硬件端确认入库",
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
            extra={
                "asset_status": new_status.value,
                "asset_name": asset_name,
                "category_id": category_id,
                "location": location,
            },
        )

    def _finalize_transaction(self, tx: PendingTransaction) -> BusinessResult:
        if tx.state != TransactionState.UPDATING:
            return self._result_from_runtime_failure(tx)

        current_status = self.get_asset_status(tx.asset_id)
        if current_status is None:
            tx = self.transaction_manager.mark_commit_failed(tx.asset_id, "资产不存在，无法提交业务结果")
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
        if tx.hw_seq is None:
            tx = self.transaction_manager.mark_commit_failed(tx.asset_id, "缺少硬件序列号，拒绝提交业务结果")
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
                hw_result=hw_result,
                hw_sn=tx.hw_sn,
                state=tx.state,
                extra=self._merge_result_extra(tx.extra, {"asset_status": current_status.value}),
            )

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
            borrow_request_id=str(tx.extra.get("borrow_request_id")) if tx.extra.get("borrow_request_id") else None,
        )

        try:
            new_status = self.repository.apply_operation_atomically(record)
        except LookupError as exc:
            self.repository.rollback_transaction(tx.asset_id, str(exc))
            tx = self.transaction_manager.mark_commit_failed(tx.asset_id, str(exc))
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
                extra=dict(tx.extra),
            )
        except ValueError as exc:
            self.repository.rollback_transaction(tx.asset_id, str(exc))
            tx = self.transaction_manager.mark_commit_failed(tx.asset_id, str(exc))
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
                extra=self._merge_result_extra(tx.extra, self._current_asset_status_extra(tx.asset_id)),
            )
        except Exception as exc:
            LOGGER.exception("business atomic commit failed: asset_id=%s request_seq=%s", tx.asset_id, tx.request_seq)
            self.repository.rollback_transaction(tx.asset_id, str(exc))
            tx = self.transaction_manager.mark_commit_failed(tx.asset_id, str(exc))
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
                extra=self._merge_result_extra(tx.extra, self._current_asset_status_extra(tx.asset_id)),
            )

        tx = self.transaction_manager.mark_commit_success(tx.asset_id)
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
            extra=self._merge_result_extra(tx.extra, {"asset_status": new_status.value}),
        )

    def _result_from_runtime_failure(self, tx: PendingTransaction) -> BusinessResult:
        if tx.response_received:
            code = ConfirmResult.INTERNAL_ERROR.value
            message = "硬件返回未知结果"
            if tx.hw_result == ConfirmResult.CANCELLED.value:
                code = ConfirmResult.CANCELLED.value
                message = "用户已在硬件端取消操作"
            elif tx.hw_result == ConfirmResult.TIMEOUT.value:
                code = ConfirmResult.TIMEOUT.value
                message = "用户在硬件端确认超时"
            elif tx.hw_result == ConfirmResult.BUSY.value:
                code = ConfirmResult.BUSY.value
                message = "设备忙，请稍后重试"
            return self._build_result(
                success=False,
                code=code,
                message=message,
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
                extra=self._merge_result_extra(tx.extra, self._current_asset_status_extra(tx.asset_id)),
            )

        message = tx.error_message or "事务失败"
        code = ConfirmResult.INTERNAL_ERROR.value
        if message == "等待 EVT_USER_ACTION 超时":
            code = ConfirmResult.HW_RESULT_TIMEOUT.value
            message = "已收到 ACK，但等待硬件确认结果超时"
        return self._build_result(
            success=False,
            code=code,
            message=message,
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
            extra=self._merge_result_extra(tx.extra, self._current_asset_status_extra(tx.asset_id)),
        )

    def _rule_result_to_business_result(self, rule_result: Any, user_name: str) -> BusinessResult:
        return self._build_result(
            success=False,
            code=rule_result.code,
            message=rule_result.message,
            asset_id=rule_result.asset_id,
            action_type=rule_result.action_type,
            user_id=rule_result.user_id,
            user_name=user_name,
            seq_id=-1,
            request_seq=None,
            request_id=None,
            state=TransactionState.FAILED,
            extra=dict(rule_result.extra),
        )

    def _result_from_send_failure(
        self,
        tx: PendingTransaction,
        send_result: SendResult,
        current_status: AssetStatus | None,
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
            extra=self._merge_result_extra(
                tx.extra,
                {} if current_status is None else {"asset_status": current_status.value},
            ),
        )

    def _current_asset_status_extra(self, asset_id: str) -> dict[str, str]:
        asset_status = self.get_asset_status(asset_id)
        if asset_status is None:
            return {}
        return {"asset_status": asset_status.value}

    @staticmethod
    def _merge_result_extra(
        tx_extra: dict[str, Any] | None,
        runtime_extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        if tx_extra:
            merged.update(tx_extra)
        if runtime_extra:
            merged.update(runtime_extra)
        return merged

    def _is_admin(self, user_id: str) -> bool:
        return user_id.strip() in self._inbound_admin_user_ids

    def _is_inbound_admin(self, user_id: str) -> bool:
        return self._is_admin(user_id)

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

    def _return_with_status(self, result: BusinessResult) -> BusinessResult:
        self._publish_business_result(result)
        return result

    def _publish_pending_status(self, tx: PendingTransaction, code: str, message: str) -> None:
        self._emit_status_payload(
            {
                "asset_id": tx.asset_id,
                "action_type": tx.action_type.value,
                "user_id": tx.user_id,
                "user_name": tx.user_name,
                "seq_id": tx.request_seq,
                "request_seq": tx.request_seq,
                "request_id": tx.request_id,
                "hw_seq": tx.hw_seq,
                "hw_result": tx.hw_result,
                "hw_sn": tx.hw_sn,
                "device_status": self.device_status.value,
                "transaction_state": tx.state.value,
                "code": code,
                "message": message,
                "success": None,
                "extra": {},
            }
        )

    def _publish_business_result(self, result: BusinessResult) -> None:
        self._emit_status_payload(result.to_dict())

    def _publish_device_status(self, status: DeviceStatus) -> None:
        self._emit_status_payload(
            {
                "asset_id": None,
                "action_type": None,
                "user_id": None,
                "user_name": None,
                "seq_id": None,
                "request_seq": None,
                "request_id": None,
                "hw_seq": None,
                "hw_result": None,
                "hw_sn": None,
                "device_status": status.value,
                "transaction_state": TransactionState.IDLE.value,
                "code": ConfirmResult.DEVICE_OFFLINE.value,
                "message": "设备离线",
                "success": None,
                "extra": {},
            }
        )

    def _emit_status_payload(self, payload: dict[str, Any]) -> None:
        callback = self._status_callback
        if callback is None:
            return
        try:
            callback(payload)
        except Exception:
            LOGGER.warning(
                "status callback failed: asset_id=%s action=%s request_seq=%s code=%s",
                payload.get("asset_id"),
                payload.get("action_type"),
                payload.get("request_seq"),
                payload.get("code"),
                exc_info=True,
            )
