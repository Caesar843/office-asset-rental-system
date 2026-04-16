from __future__ import annotations

import threading
import time
import unittest
from typing import Callable

import runtime_paths  # noqa: F401
from models import (
    ActionType,
    AssetStatus,
    BorrowApprovalCommand,
    BorrowRequestCreateCommand,
    BorrowRequestStatus,
    ConfirmResult,
    InboundCommand,
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
            )
        )
        self.assertTrue(created.success)
        self.assertIsNotNone(created.item)
        request_id = created.item.request_id
        self.assertEqual(created.item.status, BorrowRequestStatus.PENDING)

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
        stored = repository.get_borrow_request(request_id)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.status, BorrowRequestStatus.CONSUMED)
        self.assertIsNotNone(stored.consumed_at)

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
