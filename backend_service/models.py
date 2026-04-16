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
    INBOUND = "INBOUND"


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
    PERMISSION_DENIED = "PERMISSION_DENIED"
    PARAM_INVALID = "PARAM_INVALID"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class AssetStatus(str, Enum):
    IN_STOCK = "在库"
    BORROWED = "借出"
    MAINTENANCE = "维修"
    SCRAPPED = "报废"


class BorrowRequestStatus(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    CONSUMED = "CONSUMED"


def _validate_command_fields(asset_id: str, user_id: str, user_name: str, timeout_ms: int) -> None:
    if not asset_id.strip():
        raise ValueError("asset_id 不能为空")
    if not user_id.strip():
        raise ValueError("user_id 不能为空")
    if not user_name.strip():
        raise ValueError("user_name 不能为空")
    if timeout_ms <= 0:
        raise ValueError("timeout_ms 必须为正整数")


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


@dataclass(slots=True)
class BorrowCommand:
    asset_id: str
    user_id: str
    user_name: str
    timeout_ms: int = 30000

    def __post_init__(self) -> None:
        _validate_command_fields(self.asset_id, self.user_id, self.user_name, self.timeout_ms)


@dataclass(slots=True)
class ReturnCommand:
    asset_id: str
    user_id: str
    user_name: str
    timeout_ms: int = 30000

    def __post_init__(self) -> None:
        _validate_command_fields(self.asset_id, self.user_id, self.user_name, self.timeout_ms)


@dataclass(slots=True)
class InboundCommand:
    asset_id: str
    user_id: str
    user_name: str
    asset_name: str
    category_id: int | None
    location: str
    timeout_ms: int = 30000
    request_source: str | None = None
    raw_text: str | None = None
    symbology: str | None = None

    def __post_init__(self) -> None:
        self.asset_id = self.asset_id.strip()
        self.user_id = self.user_id.strip()
        self.user_name = self.user_name.strip()
        self.asset_name = self.asset_name.strip()
        self.location = self.location.strip()
        self.request_source = _normalize_optional_text(self.request_source)
        self.raw_text = _normalize_optional_text(self.raw_text)
        self.symbology = _normalize_optional_text(self.symbology)
        _validate_command_fields(self.asset_id, self.user_id, self.user_name, self.timeout_ms)
        if not self.asset_name:
            raise ValueError("asset_name 不能为空")
        if not self.location:
            raise ValueError("location 不能为空")
        if self.category_id is not None and self.category_id <= 0:
            raise ValueError("category_id 必须为正整数")


@dataclass(slots=True)
class BorrowRequestCreateCommand:
    asset_id: str
    user_id: str
    user_name: str
    reason: str | None = None
    requested_at: str | None = None

    def __post_init__(self) -> None:
        self.asset_id = self.asset_id.strip()
        self.user_id = self.user_id.strip()
        self.user_name = self.user_name.strip()
        self.reason = _normalize_optional_text(self.reason)
        self.requested_at = _normalize_optional_text(self.requested_at)
        if not self.asset_id:
            raise ValueError("asset_id 涓嶈兘涓虹┖")
        if not self.user_id:
            raise ValueError("user_id 涓嶈兘涓虹┖")
        if not self.user_name:
            raise ValueError("user_name 涓嶈兘涓虹┖")


@dataclass(slots=True)
class BorrowApprovalCommand:
    request_id: str
    reviewer_user_id: str
    reviewer_user_name: str
    approved: bool
    review_comment: str | None = None
    reviewed_at: str | None = None

    def __post_init__(self) -> None:
        self.request_id = self.request_id.strip()
        self.reviewer_user_id = self.reviewer_user_id.strip()
        self.reviewer_user_name = self.reviewer_user_name.strip()
        self.review_comment = _normalize_optional_text(self.review_comment)
        self.reviewed_at = _normalize_optional_text(self.reviewed_at)
        if not self.request_id:
            raise ValueError("request_id 涓嶈兘涓虹┖")
        if not self.reviewer_user_id:
            raise ValueError("reviewer_user_id 涓嶈兘涓虹┖")
        if not self.reviewer_user_name:
            raise ValueError("reviewer_user_name 涓嶈兘涓虹┖")


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
class RuleCheckRequest:
    asset_id: str
    user_id: str
    user_name: str
    action_type: ActionType
    device_status: DeviceStatus
    asset_status: AssetStatus | None
    has_pending_transaction: bool


@dataclass(slots=True)
class InboundRuleCheckRequest:
    asset_id: str
    user_id: str
    user_name: str
    action_type: ActionType
    device_status: DeviceStatus
    asset_status: AssetStatus | None
    has_pending_transaction: bool
    asset_name: str
    category_id: int | None
    location: str
    has_inbound_permission: bool
    category_exists: bool = True


@dataclass(slots=True)
class RuleCheckResult:
    passed: bool
    code: str
    message: str
    action_type: ActionType
    asset_id: str
    user_id: str
    extra: dict[str, Any] = field(default_factory=dict)


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
    borrow_request_id: str | None = None


@dataclass(slots=True)
class InboundCommitInput:
    asset_id: str
    asset_name: str
    category_id: int | None
    location: str
    user_id: str
    user_name: str
    request_seq: int
    request_id: str | None
    hw_seq: int
    hw_result: str
    hw_sn: str | None
    op_time: str

    @property
    def action_type(self) -> ActionType:
        return ActionType.INBOUND


@dataclass(slots=True)
class BorrowRequestCreateInput:
    request_id: str
    asset_id: str
    applicant_user_id: str
    applicant_user_name: str
    reason: str | None
    status: BorrowRequestStatus
    requested_at: str


@dataclass(slots=True)
class BorrowRequestReviewInput:
    request_id: str
    status: BorrowRequestStatus
    reviewer_user_id: str
    reviewer_user_name: str
    review_comment: str | None
    reviewed_at: str


@dataclass(slots=True)
class BorrowRequestRecord:
    request_id: str
    asset_id: str
    applicant_user_id: str
    applicant_user_name: str
    reason: str | None
    status: BorrowRequestStatus
    reviewer_user_id: str | None = None
    reviewer_user_name: str | None = None
    review_comment: str | None = None
    requested_at: str | None = None
    reviewed_at: str | None = None
    consumed_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["status"] = self.status.value
        return payload


@dataclass(slots=True)
class BorrowRequestActionResult:
    success: bool
    code: str
    message: str
    item: BorrowRequestRecord | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "code": self.code,
            "message": self.message,
            "item": None if self.item is None else self.item.to_dict(),
        }


@dataclass(slots=True)
class HardwareUserActionEvent:
    hw_seq: int
    asset_id: str
    request_seq: int
    request_id: str | None
    action_type: ActionType
    confirm_result: str
    hw_sn: str | None = None


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
    extra: dict[str, Any] = field(default_factory=dict)
