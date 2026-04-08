from __future__ import annotations

import unittest

from models import ActionType, AssetStatus, ConfirmResult, OperationRecordInput
from repository import InMemoryTransactionRepository


class FailingRecordList(list):
    def append(self, item) -> None:
        raise RuntimeError("record append failed")


class InMemoryTransactionRepositoryTests(unittest.TestCase):
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

    def test_apply_operation_atomically_updates_asset_and_preserves_hw_trace(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-4001": AssetStatus.IN_STOCK})

        new_status = repository.apply_operation_atomically(self.build_record())

        self.assertEqual(new_status, AssetStatus.BORROWED)
        self.assertEqual(repository.assets["AS-4001"], AssetStatus.BORROWED)
        self.assertEqual(len(repository.records), 1)
        self.assertEqual(repository.records[0].request_seq, 401)
        self.assertEqual(repository.records[0].hw_seq, 0x80000011)
        self.assertEqual(repository.records[0].hw_result, ConfirmResult.CONFIRMED.value)

    def test_apply_operation_atomically_restores_snapshot_when_record_write_fails(self) -> None:
        repository = InMemoryTransactionRepository(initial_assets={"AS-4001": AssetStatus.IN_STOCK})
        repository.records = FailingRecordList()

        with self.assertRaises(RuntimeError):
            repository.apply_operation_atomically(self.build_record())

        self.assertEqual(repository.assets["AS-4001"], AssetStatus.IN_STOCK)
        self.assertEqual(repository.records, [])


if __name__ == "__main__":
    unittest.main()
