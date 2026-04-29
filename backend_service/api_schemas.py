from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from models import AcceptanceResult


def _strip_required_text(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError("field cannot be blank")
    return stripped


def _strip_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


class BorrowRequestBody(BaseModel):
    asset_id: str = Field(..., description="Asset ID")
    user_id: str = Field(..., description="User ID")
    user_name: str = Field(..., description="User name")
    timeout_ms: int = Field(30000, gt=0, description="Hardware wait timeout in milliseconds")

    @field_validator("asset_id", "user_id", "user_name")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _strip_required_text(value)


class ReturnRequestBody(BaseModel):
    asset_id: str = Field(..., description="Asset ID")
    user_id: str = Field(..., description="User ID")
    user_name: str = Field(..., description="User name")
    timeout_ms: int = Field(30000, gt=0, description="Hardware wait timeout in milliseconds")

    @field_validator("asset_id", "user_id", "user_name")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _strip_required_text(value)


class InboundRequestBody(BaseModel):
    asset_id: str = Field(..., description="Asset ID")
    user_id: str = Field(..., description="User ID")
    user_name: str = Field(..., description="User name")
    asset_name: str = Field(..., description="Asset name")
    category_id: int | None = Field(None, description="Category ID")
    location: str = Field(..., description="Storage location")
    raw_text: str | None = Field(None, description="Original scan text")
    symbology: str | None = Field(None, description="Scan code format")
    timeout_ms: int = Field(30000, gt=0, description="Hardware wait timeout in milliseconds")


class BorrowRequestCreateBody(BaseModel):
    asset_id: str = Field(..., description="Asset ID")
    user_id: str = Field(..., description="Applicant user ID")
    user_name: str = Field(..., description="Applicant user name")
    reason: str | None = Field(None, description="Borrow request reason")
    requested_days: int | None = Field(None, gt=0, description="Requested borrow duration in days")

    @field_validator("asset_id", "user_id", "user_name")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _strip_required_text(value)


class BorrowRequestReviewBody(BaseModel):
    reviewer_user_id: str = Field(..., description="Reviewer user ID")
    reviewer_user_name: str = Field(..., description="Reviewer user name")
    review_comment: str | None = Field(None, description="Review comment")

    @field_validator("reviewer_user_id", "reviewer_user_name")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _strip_required_text(value)


class BorrowRequestStartBorrowBody(BaseModel):
    timeout_ms: int = Field(30000, gt=0, description="Hardware wait timeout in milliseconds")


class ReturnAcceptanceCreateBody(BaseModel):
    asset_id: str = Field(..., description="Asset ID")
    accepted_by_user_id: str = Field(..., description="Acceptance operator user ID")
    accepted_by_user_name: str = Field(..., description="Acceptance operator user name")
    acceptance_result: AcceptanceResult = Field(..., description="Return acceptance result")
    note: str | None = Field(None, description="Acceptance note")

    @field_validator("asset_id", "accepted_by_user_id", "accepted_by_user_name")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _strip_required_text(value)


class ScanResultRequestBody(BaseModel):
    asset_id: str = Field(..., description="Asset ID from the scan result")
    raw_text: str | None = Field(None, description="Original scan text")
    symbology: str | None = Field(None, description="Scan code format")
    source_id: str | None = Field(None, description="Vision source identifier")
    frame_time: int | None = Field(None, gt=0, description="Vision frame Unix timestamp in seconds")

    @field_validator("asset_id")
    @classmethod
    def _validate_asset_id(cls, value: str) -> str:
        return _strip_required_text(value)

    @field_validator("raw_text", "symbology", "source_id")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        return _strip_optional_text(value)


class BusinessResultResponse(BaseModel):
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
    device_status: str
    transaction_state: str
    extra: dict[str, Any] = Field(default_factory=dict)


class BorrowRequestRecordResponse(BaseModel):
    request_id: str
    asset_id: str
    applicant_user_id: str
    applicant_user_name: str
    reason: str | None = None
    requested_days: int
    status: str
    reviewer_user_id: str | None = None
    reviewer_user_name: str | None = None
    review_comment: str | None = None
    requested_at: str | None = None
    reviewed_at: str | None = None
    consumed_at: str | None = None


class BorrowRequestActionResponse(BaseModel):
    success: bool
    code: str
    message: str
    item: BorrowRequestRecordResponse | None = None


class ReturnAcceptanceRecordResponse(BaseModel):
    id: int
    asset_id: str
    acceptance_result: str
    note: str | None = None
    accepted_by_user_id: str
    accepted_by_user_name: str
    accepted_at: str
    related_return_request_seq: int | None = None
    related_return_request_id: str | None = None
    related_return_hw_seq: int | None = None


class ReturnAcceptanceActionResponse(BaseModel):
    success: bool
    code: str
    message: str
    item: ReturnAcceptanceRecordResponse | None = None


class AssetSnapshotResponse(BaseModel):
    asset_id: str
    exists: bool
    asset_status: str | None = None
    available_actions: list[str] = Field(default_factory=list)
    device_status: str


class ScanResultResponse(BaseModel):
    success: bool
    code: str
    message: str
    asset_id: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class ScanLatestResponse(BaseModel):
    success: bool
    code: str
    message: str
    asset_id: str | None = None
    raw_text: str | None = None
    symbology: str | None = None
    source_id: str | None = None
    frame_time: int | None = None
    received_at: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class HealthResponse(BaseModel):
    status: str
    device_status: str
    serial_open: bool
    serial_details: dict[str, Any] = Field(default_factory=dict)
    requested_repository_mode: str
    repository_mode: str
    repository_fallback: bool
    repository_ready: bool
    repository_status: str
    repository_details: dict[str, Any] = Field(default_factory=dict)
    startup_error: str | None = None


class StatusMessageResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    asset_id: str | None = None
    action_type: str | None = None
    user_id: str | None = None
    user_name: str | None = None
    seq_id: int | None = None
    request_seq: int | None = None
    request_id: str | None = None
    hw_seq: int | None = None
    hw_result: str | None = None
    hw_sn: str | None = None
    device_status: str
    transaction_state: str
    code: str
    message: str
    success: bool | None = None
    extra: dict[str, Any] = Field(default_factory=dict)
