from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class DeviceStatus(str, Enum):
    UNKNOWN = "UNKNOWN"
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"


class TransactionState(str, Enum):
    IDLE = "IDLE"
    WAIT_ACK = "WAIT_ACK"
    WAIT_HW = "WAIT_HW"
    UPDATING = "UPDATING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ActionType(str, Enum):
    BORROW = "BORROW"
    RETURN = "RETURN"


class ConfirmResult(str, Enum):
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    BUSY = "BUSY"
    ACK_ERROR = "ACK_ERROR"
    ACK_INVALID = "ACK_INVALID"
    ACK_TIMEOUT = "ACK_TIMEOUT"
    HW_RESULT_TIMEOUT = "HW_RESULT_TIMEOUT"
    DEVICE_OFFLINE = "DEVICE_OFFLINE"
    ASSET_NOT_FOUND = "ASSET_NOT_FOUND"
    STATE_INVALID = "STATE_INVALID"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class AssetStatus(str, Enum):
    IN_STOCK = "在库"
    BORROWED = "借出"
    MAINTENANCE = "维修"
    SCRAPPED = "报废"


@dataclass(slots=True)
class BusinessResult:
    success: bool
    code: str
    message: str
    asset_id: str
    action_type: str
    user_id: str
    user_name: str
    seq_id: int
    request_seq: int | None = None
    request_id: str | None = None
    hw_seq: int | None = None
    hw_result: str | None = None
    hw_sn: str | None = None
    device_status: DeviceStatus = DeviceStatus.UNKNOWN
    transaction_state: TransactionState = TransactionState.IDLE
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["device_status"] = self.device_status.value
        payload["transaction_state"] = self.transaction_state.value
        return payload


@dataclass(slots=True)
class OperationRecordInput:
    asset_id: str
    user_id: str
    user_name: str
    action_type: ActionType
    request_seq: int
    request_id: str | None
    hw_seq: int
    hw_result: str
    hw_sn: str | None = None
    due_time: str | None = None


@dataclass(slots=True)
class PendingTransaction:
    asset_id: str
    user_id: str
    user_name: str
    action_type: ActionType
    request_id: str | None = None
    request_seq: int = 0
    state: TransactionState = TransactionState.IDLE
    hw_seq: int | None = None
    hw_result: str | None = None
    hw_sn: str | None = None
    response_received: bool = False
    error_message: str | None = None
