from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from typing import Callable

import runtime_paths  # noqa: F401
from db_repository import MySQLTransactionRepository
from models import ConfirmResult, DeviceStatus
from protocol import Frame, MsgType
from serial_manager import SendResult
from service import AssetConfirmService


class AutoConfirmSerialManager:
    def __init__(
        self,
        *,
        confirm_result: str,
        response_delay: float = 0.01,
    ) -> None:
        self._confirm_result = confirm_result
        self._response_delay = response_delay
        self._next_seq = 100
        self._frame_handler: Callable[[Frame], None] | None = None
        self._status_handler = None

    def set_frame_handler(self, handler: Callable[[Frame], None]) -> None:
        self._frame_handler = handler

    def set_status_handler(self, handler) -> None:
        self._status_handler = handler

    def open(self) -> None:
        if self._status_handler is not None:
            self._status_handler(DeviceStatus.ONLINE)

    def close(self) -> None:
        if self._status_handler is not None:
            self._status_handler(DeviceStatus.OFFLINE)

    def reserve_seq_id(self) -> int:
        seq_id = self._next_seq
        self._next_seq += 1
        return seq_id

    def send_request(self, msg_type: MsgType, payload: dict[str, object], seq_id: int | None = None) -> SendResult:
        actual_seq_id = self.reserve_seq_id() if seq_id is None else seq_id
        result = SendResult(
            success=True,
            seq_id=actual_seq_id,
            ack_type=MsgType.ACK_OK,
            message="FRAME_RECEIVED",
            ack_payload={"detail": "FRAME_RECEIVED"},
        )

        if self._frame_handler is not None:
            frame = Frame.build(
                MsgType.EVT_USER_ACTION,
                seq_id=0x80000041,
                payload={
                    "asset_id": payload["asset_id"],
                    "request_seq": payload["request_seq"],
                    "request_id": payload["request_id"],
                    "action_type": payload["action_type"],
                    "confirm_result": self._confirm_result,
                    "hw_sn": "MYSQL-SMOKE",
                },
            )
            threading.Thread(target=self._emit_frame, args=(frame,), daemon=True).start()

        return result

    def _emit_frame(self, frame: Frame) -> None:
        time.sleep(self._response_delay)
        if self._frame_handler is not None:
            self._frame_handler(frame)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a borrow/return smoke flow against MySQL repository.")
    parser.add_argument("--action", choices=("borrow", "return"), required=True)
    parser.add_argument("--asset-id", required=True)
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--user-name", required=True)
    parser.add_argument("--timeout-ms", type=int, default=300)
    parser.add_argument(
        "--confirm-result",
        default=ConfirmResult.CONFIRMED.value,
        choices=(
            ConfirmResult.CONFIRMED.value,
            ConfirmResult.CANCELLED.value,
            ConfirmResult.TIMEOUT.value,
            ConfirmResult.BUSY.value,
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repository = MySQLTransactionRepository.from_env()
    probe = repository.probe()
    if not probe.ready:
        print(json.dumps({"repository_probe": probe.to_dict()}, ensure_ascii=False, indent=2))
        return 2

    serial_manager = AutoConfirmSerialManager(confirm_result=args.confirm_result)
    service = AssetConfirmService(serial_manager=serial_manager, repository=repository)
    service.open()
    try:
        if args.action == "borrow":
            result = service.request_asset_borrow_confirm(
                asset_id=args.asset_id,
                user_id=args.user_id,
                user_name=args.user_name,
                timeout_ms=args.timeout_ms,
            )
        else:
            result = service.request_asset_return_confirm(
                asset_id=args.asset_id,
                user_id=args.user_id,
                user_name=args.user_name,
                timeout_ms=args.timeout_ms,
            )
    finally:
        service.close()

    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
