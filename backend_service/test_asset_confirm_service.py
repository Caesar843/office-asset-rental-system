from __future__ import annotations

import threading
import time
import unittest
from typing import Callable
from unittest.mock import patch

import runtime_paths  # noqa: F401
from models import (
    AcceptanceResult,
    ActionType,
    AssetStatus,
    BorrowApprovalCommand,
    BorrowRequestCreateCommand,
    BorrowRequestStatus,
    ConfirmResult,
    DEFAULT_MAX_BORROW_DAYS,
    InboundCommand,
    OperationRecordInput,
    RoleType,
    ReturnAcceptanceCreateCommand,
    TransactionState,
)
from protocol import Frame, MsgType
from repository import InMemoryTransactionRepository
from serial_manager import SendResult
from service import AssetConfirmService
from transaction_manager import TransactionManager


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


class ProtocolOnlyRepository:
    def __init__(
        self,
        *,
        asset_id: str,
        initial_status: AssetStatus,
        fail_on_commit: Exception | None = None,
    ) -> None:
        self._asset_id = asset_id
        self._asset_status: AssetStatus | None = initial_status
        self._fail_on_commit = fail_on_commit
        self.last_record = None
        self.rollback_calls: list[tuple[str, str]] = []

    def get_asset_status(self, asset_id: str) -> AssetStatus | None:
        if asset_id != self._asset_id:
            return None
        return self._asset_status

    def apply_operation_atomically(self, record):
        self.last_record = record
        if self._fail_on_commit is not None:
            raise self._fail_on_commit

        if record.action_type == ActionType.BORROW:
            self._asset_status = AssetStatus.BORROWED
        else:
            self._asset_status = AssetStatus.IN_STOCK
        return self._asset_status

    def category_exists(self, category_id: int) -> bool:
        return category_id == 1

    def apply_inbound_atomically(self, commit):
        self.last_record = commit
        if self._fail_on_commit is not None:
            raise self._fail_on_commit

        self._asset_id = commit.asset_id
        self._asset_status = AssetStatus.IN_STOCK
        return self._asset_status

    def rollback_transaction(self, asset_id: str, reason: str) -> None:
        self.rollback_calls.append((asset_id, reason))


class AssetConfirmServiceTests(unittest.TestCase):
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

    def test_successful_borrow_updates_asset_and_records_hw_fields(self) -> None:
        serial_manager = FakeSerialManager(
            response_factory=lambda payload, seq_id: self.build_event_frame(
                payload,
                confirm_result=ConfirmResult.CONFIRMED.value,
            )
        )
        repository = InMemoryTransactionRepository(initial_assets={"AS-1001": AssetStatus.IN_STOCK})
        service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

        result = service.request_asset_borrow_confirm(
            asset_id="AS-1001",
            user_id="U-1001",
            user_name="赵子墨",
            timeout_ms=100,
        )

        self.assertTrue(result.success)
        self.assertEqual(result.code, ConfirmResult.CONFIRMED.value)
        self.assertEqual(result.transaction_state, TransactionState.COMPLETED)
        self.assertEqual(repository.assets["AS-1001"], AssetStatus.BORROWED)
        self.assertEqual(len(repository.records), 1)
        self.assertEqual(repository.records[0].hw_result, ConfirmResult.CONFIRMED.value)
        self.assertEqual(repository.records[0].hw_seq, result.hw_seq)
        self.assertEqual(repository.records[0].request_seq, result.request_seq)

    def test_ack_failure_returns_busy_without_mutating_repository(self) -> None:
        serial_manager = FakeSerialManager(
            send_result=SendResult(
                success=False,
                seq_id=100,
                ack_type=MsgType.ACK_BUSY,
                message="DEVICE_BUSY",
                ack_payload={"detail": "DEVICE_BUSY"},
            )
        )
        repository = InMemoryTransactionRepository(initial_assets={"AS-1001": AssetStatus.IN_STOCK})
        service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

        result = service.request_asset_borrow_confirm(
            asset_id="AS-1001",
            user_id="U-1001",
            user_name="赵子墨",
            timeout_ms=100,
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.BUSY.value)
        self.assertEqual(result.transaction_state, TransactionState.FAILED)
        self.assertEqual(repository.assets["AS-1001"], AssetStatus.IN_STOCK)
        self.assertEqual(repository.records, [])

    def test_ack_invalid_returns_failed_without_mutating_repository(self) -> None:
        serial_manager = FakeSerialManager(
            send_result=SendResult(
                success=False,
                seq_id=100,
                ack_type=MsgType.ACK_INVALID,
                message="INVALID_REQUEST",
                ack_payload={"detail": "INVALID_REQUEST"},
            )
        )
        repository = InMemoryTransactionRepository(initial_assets={"AS-1001": AssetStatus.IN_STOCK})
        service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

        result = service.request_asset_borrow_confirm(
            asset_id="AS-1001",
            user_id="U-1001",
            user_name="赵子墨",
            timeout_ms=100,
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.ACK_INVALID.value)
        self.assertEqual(result.transaction_state, TransactionState.FAILED)
        self.assertEqual(repository.assets["AS-1001"], AssetStatus.IN_STOCK)
        self.assertEqual(repository.records, [])

    def test_ack_error_returns_failed_without_mutating_repository(self) -> None:
        serial_manager = FakeSerialManager(
            send_result=SendResult(
                success=False,
                seq_id=100,
                ack_type=MsgType.ACK_ERROR,
                message="CRC_CHECK_FAIL",
                ack_payload={"detail": "CRC_CHECK_FAIL"},
            )
        )
        repository = InMemoryTransactionRepository(initial_assets={"AS-1001": AssetStatus.IN_STOCK})
        service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

        result = service.request_asset_borrow_confirm(
            asset_id="AS-1001",
            user_id="U-1001",
            user_name="赵子墨",
            timeout_ms=100,
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.ACK_ERROR.value)
        self.assertEqual(result.transaction_state, TransactionState.FAILED)
        self.assertEqual(repository.assets["AS-1001"], AssetStatus.IN_STOCK)
        self.assertEqual(repository.records, [])

    def test_ack_timeout_returns_failed_without_mutating_repository(self) -> None:
        serial_manager = FakeSerialManager(
            send_result=SendResult(
                success=False,
                seq_id=100,
                ack_type=None,
                message="ACK 超时，已重传 3 次",
                ack_payload=None,
            )
        )
        repository = InMemoryTransactionRepository(initial_assets={"AS-1001": AssetStatus.IN_STOCK})
        service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

        result = service.request_asset_borrow_confirm(
            asset_id="AS-1001",
            user_id="U-1001",
            user_name="赵子墨",
            timeout_ms=100,
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.ACK_TIMEOUT.value)
        self.assertEqual(result.transaction_state, TransactionState.FAILED)
        self.assertEqual(repository.assets["AS-1001"], AssetStatus.IN_STOCK)
        self.assertEqual(repository.records, [])

    def test_hardware_cancelled_returns_failed_without_commit(self) -> None:
        serial_manager = FakeSerialManager(
            response_factory=lambda payload, seq_id: self.build_event_frame(
                payload,
                confirm_result=ConfirmResult.CANCELLED.value,
            )
        )
        repository = InMemoryTransactionRepository(initial_assets={"AS-1001": AssetStatus.IN_STOCK})
        service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

        result = service.request_asset_borrow_confirm(
            asset_id="AS-1001",
            user_id="U-1001",
            user_name="赵子墨",
            timeout_ms=100,
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.CANCELLED.value)
        self.assertEqual(result.transaction_state, TransactionState.FAILED)
        self.assertEqual(repository.assets["AS-1001"], AssetStatus.IN_STOCK)
        self.assertEqual(repository.records, [])

    def test_hardware_wait_timeout_returns_failed_without_commit(self) -> None:
        serial_manager = FakeSerialManager()
        repository = InMemoryTransactionRepository(initial_assets={"AS-1001": AssetStatus.IN_STOCK})
        transaction_manager = TransactionManager(hw_wait_grace_seconds=0.0)
        service = AssetConfirmService(
            serial_manager=serial_manager,
            repository=repository,
            transaction_manager=transaction_manager,
        )

        result = service.request_asset_borrow_confirm(
            asset_id="AS-1001",
            user_id="U-1001",
            user_name="赵子墨",
            timeout_ms=10,
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.HW_RESULT_TIMEOUT.value)
        self.assertEqual(result.transaction_state, TransactionState.FAILED)
        self.assertEqual(repository.assets["AS-1001"], AssetStatus.IN_STOCK)
        self.assertEqual(repository.records, [])

    def test_hardware_failure_result_is_preserved_even_if_asset_disappears_before_finalize(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-1001": AssetStatus.IN_STOCK})

        def build_frame(payload: dict[str, object], seq_id: int) -> Frame:
            repository.assets.pop("AS-1001", None)
            return self.build_event_frame(payload, confirm_result=ConfirmResult.CANCELLED.value)

        serial_manager = FakeSerialManager(response_factory=build_frame)
        service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

        result = service.request_asset_borrow_confirm(
            asset_id="AS-1001",
            user_id="U-1001",
            user_name="赵子墨",
            timeout_ms=100,
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.CANCELLED.value)
        self.assertEqual(result.transaction_state, TransactionState.FAILED)

    def test_service_depends_only_on_repository_protocol_for_successful_commit(self) -> None:
        serial_manager = FakeSerialManager(
            response_factory=lambda payload, seq_id: self.build_event_frame(
                payload,
                confirm_result=ConfirmResult.CONFIRMED.value,
            )
        )
        repository = ProtocolOnlyRepository(asset_id="AS-2001", initial_status=AssetStatus.IN_STOCK)
        service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

        result = service.request_asset_borrow_confirm(
            asset_id="AS-2001",
            user_id="U-2001",
            user_name="李青云",
            timeout_ms=100,
        )

        self.assertTrue(result.success)
        self.assertEqual(result.code, ConfirmResult.CONFIRMED.value)
        self.assertEqual(result.transaction_state, TransactionState.COMPLETED)
        self.assertIsNotNone(repository.last_record)
        self.assertEqual(repository.last_record.request_seq, result.request_seq)
        self.assertEqual(repository.last_record.hw_seq, result.hw_seq)
        self.assertEqual(repository.last_record.hw_result, result.hw_result)

    def test_commit_failure_calls_repository_rollback_and_closes_transaction(self) -> None:
        serial_manager = FakeSerialManager(
            response_factory=lambda payload, seq_id: self.build_event_frame(
                payload,
                confirm_result=ConfirmResult.CONFIRMED.value,
            )
        )
        repository = ProtocolOnlyRepository(
            asset_id="AS-3001",
            initial_status=AssetStatus.IN_STOCK,
            fail_on_commit=RuntimeError("db down"),
        )
        service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

        result = service.request_asset_borrow_confirm(
            asset_id="AS-3001",
            user_id="U-3001",
            user_name="陈知远",
            timeout_ms=100,
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.INTERNAL_ERROR.value)
        self.assertEqual(result.transaction_state, TransactionState.FAILED)
        self.assertEqual(repository.rollback_calls, [("AS-3001", "db down")])

    def test_create_borrow_request_rejects_when_pending_transaction_exists(self) -> None:
        serial_manager = FakeSerialManager()
        repository = InMemoryTransactionRepository(initial_assets={"AS-6101": AssetStatus.IN_STOCK})
        transaction_manager = TransactionManager()
        service = AssetConfirmService(
            serial_manager=serial_manager,
            repository=repository,
            transaction_manager=transaction_manager,
        )
        transaction_manager.create_transaction(
            asset_id="AS-6101",
            user_id="U-OTHER",
            user_name="Other User",
            action_type=ActionType.BORROW,
            request_id="req-pending-6101",
            request_seq=6101,
        )

        result = service.create_borrow_request(
            BorrowRequestCreateCommand(
                asset_id="AS-6101",
                user_id="U-6101",
                user_name="Requester",
                reason="Need this asset",
            )
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.BUSY.value)
        self.assertEqual(repository.list_borrow_requests(), [])

    def test_borrow_request_can_be_approved_and_started(self) -> None:
        with patch.dict("os.environ", {"BACKEND_MAX_BORROW_DAYS": str(DEFAULT_MAX_BORROW_DAYS)}, clear=False):
            serial_manager = FakeSerialManager(
                response_factory=lambda payload, seq_id: self.build_event_frame(
                    payload,
                    confirm_result=ConfirmResult.CONFIRMED.value,
                )
            )
            repository = InMemoryTransactionRepository(initial_assets={"AS-6201": AssetStatus.IN_STOCK})
            service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

            created = service.create_borrow_request(
                BorrowRequestCreateCommand(
                    asset_id="AS-6201",
                    user_id="U-6201",
                    user_name="Requester",
                    reason="Temporary use",
                    requested_days=7,
                )
            )
            self.assertTrue(created.success)
            self.assertIsNotNone(created.item)
            request_id = created.item.request_id
            self.assertEqual(created.item.status, BorrowRequestStatus.PENDING)
            self.assertEqual(created.item.requested_days, 7)

            reviewed = service.review_borrow_request(
                BorrowApprovalCommand(
                    request_id=request_id,
                    reviewer_user_id="U-ADMIN",
                    reviewer_user_name="Admin",
                    approved=True,
                    review_comment="approved",
                )
            )
            self.assertTrue(reviewed.success)
            self.assertIsNotNone(reviewed.item)
            self.assertEqual(reviewed.item.status, BorrowRequestStatus.APPROVED)

            result = service.start_borrow_from_request(request_id, timeout_ms=100)

            self.assertTrue(result.success)
            self.assertEqual(result.code, ConfirmResult.CONFIRMED.value)
            self.assertEqual(result.transaction_state, TransactionState.COMPLETED)
            self.assertEqual(repository.assets["AS-6201"], AssetStatus.BORROWED)
            self.assertEqual(len(repository.records), 1)
            self.assertEqual(repository.records[0].borrow_request_id, request_id)
            self.assertIsNotNone(repository.records[0].due_time)
            self.assertEqual(result.extra["requested_days"], 7)
            self.assertEqual(result.extra["due_time"], repository.records[0].due_time)
            stored = repository.get_borrow_request(request_id)
            self.assertIsNotNone(stored)
            self.assertEqual(stored.status, BorrowRequestStatus.CONSUMED)
            self.assertIsNotNone(stored.consumed_at)

    def test_role_resolution_is_consistent_across_inbound_review_and_acceptance(self) -> None:
        repository = InMemoryTransactionRepository(
            initial_assets={
                "AS-6202": AssetStatus.IN_STOCK,
                "AS-6203": AssetStatus.IN_STOCK,
            }
        )
        repository.records.append(
            OperationRecordInput(
                asset_id="AS-6203",
                user_id="U-6203",
                user_name="Borrower",
                action_type=ActionType.RETURN,
                request_seq=6203,
                request_id="req-6203",
                hw_seq=0x80000023,
                hw_result=ConfirmResult.CONFIRMED.value,
                hw_sn="STM32F103-A23",
            )
        )
        service = AssetConfirmService(serial_manager=FakeSerialManager(), repository=repository)

        self.assertEqual(service.resolve_user_role("U-ADMIN"), RoleType.ADMIN)
        self.assertEqual(service.resolve_user_role("U-6202"), RoleType.BORROWER)

        inbound_result = service.request_asset_inbound_confirm(
            asset_id="AS-NEW-6202",
            user_id="U-NORMAL",
            user_name="Normal User",
            asset_name="New Device",
            location="Shelf A",
            category_id=1,
            timeout_ms=100,
        )
        review_result = service.review_borrow_request(
            BorrowApprovalCommand(
                request_id="missing-request",
                reviewer_user_id="U-NORMAL",
                reviewer_user_name="Normal User",
                approved=True,
            )
        )
        acceptance_result = service.create_return_acceptance(
            ReturnAcceptanceCreateCommand(
                asset_id="AS-6203",
                accepted_by_user_id="U-NORMAL",
                accepted_by_user_name="Normal User",
                acceptance_result=AcceptanceResult.NORMAL,
            )
        )

        self.assertEqual(inbound_result.code, ConfirmResult.PERMISSION_DENIED.value)
        self.assertEqual(review_result.code, ConfirmResult.PERMISSION_DENIED.value)
        self.assertEqual(acceptance_result.code, ConfirmResult.PERMISSION_DENIED.value)

    def test_borrower_can_still_create_borrow_request_with_default_requested_days(self) -> None:
        with patch.dict("os.environ", {"BACKEND_MAX_BORROW_DAYS": str(DEFAULT_MAX_BORROW_DAYS)}, clear=False):
            repository = InMemoryTransactionRepository(initial_assets={"AS-6204": AssetStatus.IN_STOCK})
            service = AssetConfirmService(serial_manager=FakeSerialManager(), repository=repository)

            result = service.create_borrow_request(
                BorrowRequestCreateCommand(
                    asset_id="AS-6204",
                    user_id="U-6204",
                    user_name="Requester",
                    reason="Default requested days",
                )
            )

            self.assertTrue(result.success)
            self.assertIsNotNone(result.item)
            self.assertEqual(result.item.requested_days, DEFAULT_MAX_BORROW_DAYS)

    def test_borrow_request_rejects_when_requested_days_exceeds_max(self) -> None:
        with patch.dict("os.environ", {"BACKEND_MAX_BORROW_DAYS": str(DEFAULT_MAX_BORROW_DAYS)}, clear=False):
            repository = InMemoryTransactionRepository(initial_assets={"AS-6205": AssetStatus.IN_STOCK})
            service = AssetConfirmService(serial_manager=FakeSerialManager(), repository=repository)

            result = service.create_borrow_request(
                BorrowRequestCreateCommand(
                    asset_id="AS-6205",
                    user_id="U-6205",
                    user_name="Requester",
                    reason="Too long",
                    requested_days=DEFAULT_MAX_BORROW_DAYS + 1,
                )
            )

            self.assertFalse(result.success)
            self.assertEqual(result.code, ConfirmResult.PARAM_INVALID.value)
            self.assertEqual(repository.list_borrow_requests(asset_id="AS-6205"), [])

    def test_admin_can_create_return_acceptance_after_successful_return(self) -> None:
        serial_manager = FakeSerialManager(
            response_factory=lambda payload, seq_id: self.build_event_frame(
                payload,
                confirm_result=ConfirmResult.CONFIRMED.value,
            )
        )
        repository = InMemoryTransactionRepository(initial_assets={"AS-6301": AssetStatus.BORROWED})
        service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

        return_result = service.request_asset_return_confirm(
            asset_id="AS-6301",
            user_id="U-6301",
            user_name="Borrower",
            timeout_ms=100,
        )
        acceptance_result = service.create_return_acceptance(
            ReturnAcceptanceCreateCommand(
                asset_id="AS-6301",
                accepted_by_user_id="U-ADMIN",
                accepted_by_user_name="Admin",
                acceptance_result=AcceptanceResult.NORMAL,
                note="all good",
            )
        )

        self.assertTrue(return_result.success)
        self.assertTrue(acceptance_result.success)
        self.assertEqual(acceptance_result.code, "ACCEPTANCE_CREATED")
        self.assertIsNotNone(acceptance_result.item)
        self.assertEqual(acceptance_result.item.acceptance_result, AcceptanceResult.NORMAL)
        self.assertEqual(acceptance_result.item.related_return_request_seq, return_result.request_seq)
        self.assertEqual(acceptance_result.item.related_return_hw_seq, return_result.hw_seq)
        self.assertEqual(len(repository.list_return_acceptances(asset_id="AS-6301")), 1)

    def test_return_acceptance_supports_damaged_and_missing_parts_results(self) -> None:
        repository = InMemoryTransactionRepository(
            initial_assets={
                "AS-6302": AssetStatus.IN_STOCK,
                "AS-6303": AssetStatus.IN_STOCK,
            }
        )
        repository.records.extend(
            [
                OperationRecordInput(
                    asset_id="AS-6302",
                    user_id="U-6302",
                    user_name="Borrower A",
                    action_type=ActionType.RETURN,
                    request_seq=6302,
                    request_id="req-6302",
                    hw_seq=0x80006302,
                    hw_result=ConfirmResult.CONFIRMED.value,
                    hw_sn="STM32F103-A23",
                    due_time=None,
                ),
                OperationRecordInput(
                    asset_id="AS-6303",
                    user_id="U-6303",
                    user_name="Borrower B",
                    action_type=ActionType.RETURN,
                    request_seq=6303,
                    request_id="req-6303",
                    hw_seq=0x80006303,
                    hw_result=ConfirmResult.CONFIRMED.value,
                    hw_sn="STM32F103-A23",
                    due_time=None,
                ),
            ]
        )
        service = AssetConfirmService(serial_manager=FakeSerialManager(), repository=repository)

        damaged = service.create_return_acceptance(
            ReturnAcceptanceCreateCommand(
                asset_id="AS-6302",
                accepted_by_user_id="U-ADMIN",
                accepted_by_user_name="Admin",
                acceptance_result=AcceptanceResult.DAMAGED,
            )
        )
        missing_parts = service.create_return_acceptance(
            ReturnAcceptanceCreateCommand(
                asset_id="AS-6303",
                accepted_by_user_id="U-ADMIN",
                accepted_by_user_name="Admin",
                acceptance_result=AcceptanceResult.MISSING_PARTS,
            )
        )

        self.assertTrue(damaged.success)
        self.assertEqual(damaged.item.acceptance_result, AcceptanceResult.DAMAGED)
        self.assertTrue(missing_parts.success)
        self.assertEqual(missing_parts.item.acceptance_result, AcceptanceResult.MISSING_PARTS)

    def test_return_acceptance_rejects_non_admin_without_writing_record(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-6304": AssetStatus.IN_STOCK})
        repository.records.append(
            OperationRecordInput(
                asset_id="AS-6304",
                user_id="U-6304",
                user_name="Borrower",
                action_type=ActionType.RETURN,
                request_seq=6304,
                request_id="req-6304",
                hw_seq=0x80006304,
                hw_result=ConfirmResult.CONFIRMED.value,
                hw_sn="STM32F103-A23",
                due_time=None,
            )
        )
        service = AssetConfirmService(serial_manager=FakeSerialManager(), repository=repository)

        result = service.create_return_acceptance(
            ReturnAcceptanceCreateCommand(
                asset_id="AS-6304",
                accepted_by_user_id="U-6304",
                accepted_by_user_name="Borrower",
                acceptance_result=AcceptanceResult.NORMAL,
            )
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.PERMISSION_DENIED.value)
        self.assertEqual(repository.list_return_acceptances(asset_id="AS-6304"), [])

    def test_return_acceptance_rejects_when_no_completed_return_trace_exists(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-6305": AssetStatus.IN_STOCK})
        service = AssetConfirmService(serial_manager=FakeSerialManager(), repository=repository)

        result = service.create_return_acceptance(
            ReturnAcceptanceCreateCommand(
                asset_id="AS-6305",
                accepted_by_user_id="U-ADMIN",
                accepted_by_user_name="Admin",
                acceptance_result=AcceptanceResult.NORMAL,
            )
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.STATE_INVALID.value)
        self.assertEqual(repository.list_return_acceptances(asset_id="AS-6305"), [])

    def test_return_acceptance_rejects_duplicate_for_latest_return(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-6306": AssetStatus.IN_STOCK})
        repository.records.append(
            OperationRecordInput(
                asset_id="AS-6306",
                user_id="U-6306",
                user_name="Borrower",
                action_type=ActionType.RETURN,
                request_seq=6306,
                request_id="req-6306",
                hw_seq=0x80006306,
                hw_result=ConfirmResult.CONFIRMED.value,
                hw_sn="STM32F103-A23",
                due_time=None,
            )
        )
        service = AssetConfirmService(serial_manager=FakeSerialManager(), repository=repository)
        first = service.create_return_acceptance(
            ReturnAcceptanceCreateCommand(
                asset_id="AS-6306",
                accepted_by_user_id="U-ADMIN",
                accepted_by_user_name="Admin",
                acceptance_result=AcceptanceResult.NORMAL,
            )
        )
        second = service.create_return_acceptance(
            ReturnAcceptanceCreateCommand(
                asset_id="AS-6306",
                accepted_by_user_id="U-ADMIN",
                accepted_by_user_name="Admin",
                acceptance_result=AcceptanceResult.NORMAL,
            )
        )

        self.assertTrue(first.success)
        self.assertFalse(second.success)
        self.assertEqual(second.code, ConfirmResult.STATE_INVALID.value)
        self.assertEqual(len(repository.list_return_acceptances(asset_id="AS-6306")), 1)

    def test_successful_inbound_creates_new_asset_and_records_hw_fields(self) -> None:
        serial_manager = FakeSerialManager(
            response_factory=lambda payload, seq_id: self.build_event_frame(
                payload,
                confirm_result=ConfirmResult.CONFIRMED.value,
            )
        )
        repository = InMemoryTransactionRepository(initial_assets={})
        service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

        result = service.request_inbound(
            InboundCommand(
                asset_id="AS-7001",
                user_id="U-ADMIN",
                user_name="管理员",
                asset_name="New Laptop",
                category_id=1,
                location="Inbound Shelf",
                timeout_ms=100,
                request_source="api",
            )
        )

        self.assertTrue(result.success)
        self.assertEqual(result.code, ConfirmResult.CONFIRMED.value)
        self.assertEqual(result.action_type, ActionType.INBOUND.value)
        self.assertEqual(result.transaction_state, TransactionState.COMPLETED)
        self.assertEqual(repository.assets["AS-7001"], AssetStatus.IN_STOCK)
        self.assertEqual(repository.asset_details["AS-7001"]["asset_name"], "New Laptop")
        self.assertEqual(len(repository.records), 1)
        self.assertEqual(repository.records[0].request_seq, result.request_seq)
        self.assertEqual(repository.records[0].hw_seq, result.hw_seq)
        self.assertEqual(serial_manager.sent_requests[0][1]["asset_name"], "New Laptop")
        self.assertEqual(serial_manager.sent_requests[0][1]["category_id"], 1)
        self.assertEqual(serial_manager.sent_requests[0][1]["location"], "Inbound Shelf")


if __name__ == "__main__":
    unittest.main()
