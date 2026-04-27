from __future__ import annotations

import unittest

from models import (
    AcceptanceResult,
    ActionType,
    AssetStatus,
    BorrowRequestCreateInput,
    BorrowRequestReviewInput,
    BorrowRequestStatus,
    ConfirmResult,
    InboundCommitInput,
    OperationRecordInput,
    ReturnAcceptanceCreateInput,
)
from repository import InMemoryTransactionRepository


class FailingRecordList(list):
    def append(self, item) -> None:
        raise RuntimeError("record append failed")


class InMemoryTransactionRepositoryTests(unittest.TestCase):
    def build_borrow_request(self, *, status: BorrowRequestStatus = BorrowRequestStatus.APPROVED) -> BorrowRequestCreateInput:
        return BorrowRequestCreateInput(
            request_id="br-4001",
            asset_id="AS-4001",
            applicant_user_id="U-4001",
            applicant_user_name="Tester",
            reason="Demo",
            requested_days=14,
            status=status,
            requested_at="2026-04-15 09:30:00",
        )

    def build_record(self) -> OperationRecordInput:
        return OperationRecordInput(
            asset_id="AS-4001",
            user_id="U-4001",
            user_name="林知夏",
            action_type=ActionType.BORROW,
            request_seq=401,
            request_id="req-4001",
            hw_seq=0x80000011,
            hw_result=ConfirmResult.CONFIRMED.value,
            hw_sn="STM32F103-A23",
            due_time=None,
        )

    def build_return_record(self) -> OperationRecordInput:
        return OperationRecordInput(
            asset_id="AS-4001",
            user_id="U-4001",
            user_name="Tester",
            action_type=ActionType.RETURN,
            request_seq=402,
            request_id="req-4002",
            hw_seq=0x80000012,
            hw_result=ConfirmResult.CONFIRMED.value,
            hw_sn="STM32F103-A23",
            due_time=None,
        )

    def build_return_acceptance(self) -> ReturnAcceptanceCreateInput:
        return ReturnAcceptanceCreateInput(
            asset_id="AS-4001",
            acceptance_result=AcceptanceResult.NORMAL,
            note="checked",
            accepted_by_user_id="U-ADMIN",
            accepted_by_user_name="Admin",
            accepted_at="2026-04-15 10:30:00",
            related_return_request_seq=402,
            related_return_request_id="req-4002",
            related_return_hw_seq=0x80000012,
        )

    def build_inbound_commit(self) -> InboundCommitInput:
        return InboundCommitInput(
            asset_id="AS-9001",
            asset_name="Dell Monitor",
            category_id=1,
            location="Rack A",
            user_id="U-ADMIN",
            user_name="管理员",
            request_seq=901,
            request_id="req-9001",
            hw_seq=0x80000031,
            hw_result=ConfirmResult.CONFIRMED.value,
            hw_sn="STM32F103-A23",
            op_time="2026-04-15 10:00:00",
        )

    def test_apply_operation_atomically_updates_asset_and_preserves_hw_trace(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-4001": AssetStatus.IN_STOCK})
        record = self.build_record()
        record.due_time = "2026-04-29 09:30:00"

        new_status = repository.apply_operation_atomically(record)

        self.assertEqual(new_status, AssetStatus.BORROWED)
        self.assertEqual(repository.assets["AS-4001"], AssetStatus.BORROWED)
        self.assertEqual(len(repository.records), 1)
        self.assertEqual(repository.records[0].request_seq, 401)
        self.assertEqual(repository.records[0].hw_seq, 0x80000011)
        self.assertEqual(repository.records[0].hw_result, ConfirmResult.CONFIRMED.value)
        self.assertEqual(repository.records[0].due_time, "2026-04-29 09:30:00")

    def test_apply_operation_atomically_restores_snapshot_when_record_write_fails(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-4001": AssetStatus.IN_STOCK})
        repository.records = FailingRecordList()

        with self.assertRaises(RuntimeError):
            repository.apply_operation_atomically(self.build_record())

        self.assertEqual(repository.assets["AS-4001"], AssetStatus.IN_STOCK)
        self.assertEqual(repository.records, [])

    def test_borrow_request_can_be_reviewed_and_consumed_by_borrow_commit(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-4001": AssetStatus.IN_STOCK})

        created = repository.create_borrow_request(self.build_borrow_request(status=BorrowRequestStatus.PENDING))
        reviewed = repository.review_borrow_request(
            BorrowRequestReviewInput(
                request_id=created.request_id,
                status=BorrowRequestStatus.APPROVED,
                reviewer_user_id="U-ADMIN",
                reviewer_user_name="Admin",
                review_comment="approved",
                reviewed_at="2026-04-15 09:31:00",
            )
        )
        record = self.build_record()
        record.borrow_request_id = reviewed.request_id

        repository.apply_operation_atomically(record)

        stored = repository.get_borrow_request(reviewed.request_id)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.status, BorrowRequestStatus.CONSUMED)
        self.assertEqual(stored.requested_days, 14)
        self.assertIsNotNone(stored.consumed_at)

    def test_borrow_commit_with_pending_request_is_rejected(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-4001": AssetStatus.IN_STOCK})
        created = repository.create_borrow_request(self.build_borrow_request(status=BorrowRequestStatus.PENDING))
        record = self.build_record()
        record.borrow_request_id = created.request_id

        with self.assertRaises(ValueError):
            repository.apply_operation_atomically(record)

        self.assertEqual(repository.assets["AS-4001"], AssetStatus.IN_STOCK)
        self.assertEqual(repository.get_borrow_request(created.request_id).status, BorrowRequestStatus.PENDING)

    def test_return_acceptance_can_be_created_listed_and_traced(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-4001": AssetStatus.BORROWED})
        repository.apply_operation_atomically(self.build_return_record())

        latest = repository.get_latest_operation_record("AS-4001")
        created = repository.create_return_acceptance(self.build_return_acceptance())
        listed = repository.list_return_acceptances(asset_id="AS-4001")
        found = repository.get_return_acceptance_by_related_return(
            asset_id="AS-4001",
            related_return_request_seq=latest.request_seq if latest is not None else None,
            related_return_hw_seq=latest.hw_seq if latest is not None else None,
        )

        self.assertIsNotNone(latest)
        self.assertEqual(latest.action_type, ActionType.RETURN)
        self.assertEqual(latest.request_seq, 402)
        self.assertEqual(created.acceptance_result, AcceptanceResult.NORMAL)
        self.assertEqual(created.related_return_hw_seq, 0x80000012)
        self.assertEqual(len(listed), 1)
        self.assertIsNotNone(found)
        self.assertEqual(found.id, created.id)

    def test_return_acceptance_duplicate_related_return_is_rejected(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-4001": AssetStatus.BORROWED})
        repository.apply_operation_atomically(self.build_return_record())
        repository.create_return_acceptance(self.build_return_acceptance())

        with self.assertRaises(ValueError):
            repository.create_return_acceptance(self.build_return_acceptance())

    def test_apply_inbound_atomically_creates_asset_and_record(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={})

        new_status = repository.apply_inbound_atomically(self.build_inbound_commit())

        self.assertEqual(new_status, AssetStatus.IN_STOCK)
        self.assertEqual(repository.assets["AS-9001"], AssetStatus.IN_STOCK)
        self.assertEqual(repository.asset_details["AS-9001"]["asset_name"], "Dell Monitor")
        self.assertEqual(repository.asset_details["AS-9001"]["category_id"], 1)
        self.assertEqual(repository.asset_details["AS-9001"]["location"], "Rack A")
        self.assertEqual(len(repository.records), 1)
        self.assertEqual(repository.records[0].action_type, ActionType.INBOUND)
        self.assertEqual(repository.records[0].hw_result, ConfirmResult.CONFIRMED.value)

    def test_apply_inbound_atomically_restores_snapshot_when_record_write_fails(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={})
        repository.records = FailingRecordList()

        with self.assertRaises(RuntimeError):
            repository.apply_inbound_atomically(self.build_inbound_commit())

        self.assertNotIn("AS-9001", repository.assets)
        self.assertNotIn("AS-9001", repository.asset_details)
        self.assertEqual(repository.records, [])


if __name__ == "__main__":
    unittest.main()
