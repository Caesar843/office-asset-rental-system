from __future__ import annotations

import unittest

import runtime_paths  # noqa: F401
from models import ActionType, ConfirmResult, TransactionState
from protocol import Frame, MsgType
from transaction_manager import TransactionManager


class TransactionManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manager = TransactionManager(hw_wait_grace_seconds=0.0)

    def create_waiting_transaction(self, *, asset_id: str = "AS-1001", request_id: str = "req-1001") -> None:
        self.manager.create_transaction(
            asset_id=asset_id,
            user_id="U-1001",
            user_name="赵子墨",
            action_type=ActionType.BORROW,
            request_id=request_id,
            request_seq=101,
        )
        self.manager.mark_ack_success(asset_id)

    def build_event_frame(
        self,
        *,
        asset_id: str = "AS-1001",
        request_seq: int = 101,
        request_id: str | None = "req-1001",
        action_type: str = "BORROW",
        confirm_result: str = "CONFIRMED",
        hw_seq: int = 0x80000001,
    ) -> Frame:
        payload = {
            "asset_id": asset_id,
            "request_seq": request_seq,
            "request_id": request_id,
            "action_type": action_type,
            "confirm_result": confirm_result,
            "hw_sn": "STM32F103-A23",
        }
        return Frame.build(MsgType.EVT_USER_ACTION, seq_id=hw_seq, payload=payload)

    def test_mark_ack_success_transitions_to_wait_hw(self) -> None:
        pending = self.manager.create_transaction(
            asset_id="AS-1001",
            user_id="U-1001",
            user_name="赵子墨",
            action_type=ActionType.BORROW,
            request_id="req-1001",
            request_seq=101,
        )

        self.assertEqual(pending.state, TransactionState.WAIT_ACK)
        pending = self.manager.mark_ack_success("AS-1001")
        self.assertEqual(pending.state, TransactionState.WAIT_HW)

    def test_confirmed_hw_event_transitions_to_updating(self) -> None:
        self.create_waiting_transaction()

        self.manager.handle_frame(self.build_event_frame(confirm_result=ConfirmResult.CONFIRMED.value))
        pending = self.manager.get_transaction("AS-1001")

        self.assertIsNotNone(pending)
        self.assertEqual(pending.state, TransactionState.UPDATING)
        self.assertTrue(pending.response_received)
        self.assertEqual(pending.hw_result, ConfirmResult.CONFIRMED.value)
        self.assertEqual(pending.hw_seq, 0x80000001)

    def test_commit_success_transitions_to_completed(self) -> None:
        self.create_waiting_transaction()
        self.manager.handle_frame(self.build_event_frame(confirm_result=ConfirmResult.CONFIRMED.value))

        pending = self.manager.mark_commit_success("AS-1001")
        self.assertEqual(pending.state, TransactionState.COMPLETED)

    def test_mark_ack_failure_requires_wait_ack(self) -> None:
        self.create_waiting_transaction()

        with self.assertRaises(ValueError):
            self.manager.mark_ack_failure("AS-1001", "ACK_ERROR")

    def test_mark_commit_success_requires_updating(self) -> None:
        self.create_waiting_transaction()

        with self.assertRaises(ValueError):
            self.manager.mark_commit_success("AS-1001")

    def test_mark_commit_failed_requires_updating(self) -> None:
        self.create_waiting_transaction()

        with self.assertRaises(ValueError):
            self.manager.mark_commit_failed("AS-1001", "commit failed")

    def test_cancelled_hw_event_transitions_to_failed(self) -> None:
        self.create_waiting_transaction()

        self.manager.handle_frame(self.build_event_frame(confirm_result=ConfirmResult.CANCELLED.value))
        pending = self.manager.get_transaction("AS-1001")

        self.assertIsNotNone(pending)
        self.assertEqual(pending.state, TransactionState.FAILED)
        self.assertEqual(pending.hw_result, ConfirmResult.CANCELLED.value)

    def test_wait_for_hw_result_timeout_marks_failed(self) -> None:
        self.create_waiting_transaction()

        wait_result = self.manager.wait_for_hw_result("AS-1001", timeout_ms=10)

        self.assertTrue(wait_result.timed_out)
        self.assertEqual(wait_result.pending.state, TransactionState.FAILED)
        self.assertEqual(wait_result.pending.error_message, "等待 EVT_USER_ACTION 超时")

    def test_matching_event_before_ack_success_is_ignored(self) -> None:
        self.manager.create_transaction(
            asset_id="AS-1001",
            user_id="U-1001",
            user_name="赵子墨",
            action_type=ActionType.BORROW,
            request_id="req-1001",
            request_seq=101,
        )

        self.manager.handle_frame(self.build_event_frame(confirm_result=ConfirmResult.CONFIRMED.value))
        pending = self.manager.get_transaction("AS-1001")

        self.assertIsNotNone(pending)
        self.assertEqual(pending.state, TransactionState.WAIT_ACK)
        self.assertFalse(pending.response_received)

    def test_duplicate_event_does_not_override_first_result(self) -> None:
        self.create_waiting_transaction()

        self.manager.handle_frame(
            self.build_event_frame(confirm_result=ConfirmResult.CONFIRMED.value, hw_seq=0x80000001)
        )
        self.manager.handle_frame(
            self.build_event_frame(confirm_result=ConfirmResult.CANCELLED.value, hw_seq=0x80000002)
        )
        pending = self.manager.get_transaction("AS-1001")

        self.assertIsNotNone(pending)
        self.assertEqual(pending.state, TransactionState.UPDATING)
        self.assertEqual(pending.hw_result, ConfirmResult.CONFIRMED.value)
        self.assertEqual(pending.hw_seq, 0x80000001)

    def test_orphan_event_is_ignored(self) -> None:
        self.manager.handle_frame(self.build_event_frame(asset_id="AS-404"))

        self.assertFalse(self.manager.has_pending_transaction("AS-404"))

    def test_late_event_after_removal_is_ignored(self) -> None:
        self.create_waiting_transaction()
        self.manager.remove_transaction("AS-1001")

        self.manager.handle_frame(self.build_event_frame())

        self.assertFalse(self.manager.has_pending_transaction("AS-1001"))

    def test_failed_transaction_ignores_late_event(self) -> None:
        self.manager.create_transaction(
            asset_id="AS-1001",
            user_id="U-1001",
            user_name="赵子墨",
            action_type=ActionType.BORROW,
            request_id="req-1001",
            request_seq=101,
        )
        self.manager.mark_ack_failure("AS-1001", "ACK_ERROR")

        self.manager.handle_frame(self.build_event_frame())
        pending = self.manager.get_transaction("AS-1001")

        self.assertIsNotNone(pending)
        self.assertEqual(pending.state, TransactionState.FAILED)
        self.assertFalse(pending.response_received)

    def test_mismatched_events_do_not_advance_state(self) -> None:
        self.create_waiting_transaction()

        mismatch_frames = (
            self.build_event_frame(request_seq=999),
            self.build_event_frame(action_type="RETURN"),
            self.build_event_frame(request_id="bad-request-id"),
        )

        for frame in mismatch_frames:
            with self.subTest(frame=frame.payload):
                self.manager.handle_frame(frame)
                pending = self.manager.get_transaction("AS-1001")
                self.assertIsNotNone(pending)
                self.assertEqual(pending.state, TransactionState.WAIT_HW)
                self.assertFalse(pending.response_received)
                self.assertIsNone(pending.hw_result)
                self.assertIsNone(pending.hw_seq)

    def test_missing_fields_are_ignored(self) -> None:
        self.create_waiting_transaction()
        frame = Frame.build(
            MsgType.EVT_USER_ACTION,
            seq_id=0x80000001,
            payload={"asset_id": "AS-1001", "request_seq": 101},
        )

        self.manager.handle_frame(frame)
        pending = self.manager.get_transaction("AS-1001")

        self.assertIsNotNone(pending)
        self.assertEqual(pending.state, TransactionState.WAIT_HW)
        self.assertFalse(pending.response_received)

    def test_invalid_request_seq_and_action_type_are_ignored(self) -> None:
        self.create_waiting_transaction()
        invalid_frames = (
            Frame.build(
                MsgType.EVT_USER_ACTION,
                seq_id=0x80000001,
                payload={
                    "asset_id": "AS-1001",
                    "request_seq": "bad-seq",
                    "request_id": "req-1001",
                    "action_type": "BORROW",
                    "confirm_result": "CONFIRMED",
                },
            ),
            Frame.build(
                MsgType.EVT_USER_ACTION,
                seq_id=0x80000002,
                payload={
                    "asset_id": "AS-1001",
                    "request_seq": 101,
                    "request_id": "req-1001",
                    "action_type": "BAD_ACTION",
                    "confirm_result": "CONFIRMED",
                },
            ),
        )

        for frame in invalid_frames:
            with self.subTest(frame=frame.payload):
                self.manager.handle_frame(frame)
                pending = self.manager.get_transaction("AS-1001")
                self.assertIsNotNone(pending)
                self.assertEqual(pending.state, TransactionState.WAIT_HW)
                self.assertFalse(pending.response_received)

    def test_completed_transaction_cannot_fail_again(self) -> None:
        self.create_waiting_transaction()
        self.manager.handle_frame(self.build_event_frame(confirm_result=ConfirmResult.CONFIRMED.value))
        self.manager.mark_commit_success("AS-1001")

        with self.assertRaises(ValueError):
            self.manager.mark_failed("AS-1001", "should stay completed")


if __name__ == "__main__":
    unittest.main()
