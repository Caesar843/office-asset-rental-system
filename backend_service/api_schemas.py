from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _strip_required_text(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError("field cannot be blank")
    return stripped


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


class ScanResultRequestBody(BaseModel):
    asset_id: str = Field(..., description="Asset ID from the scan result")

    @field_validator("asset_id")
    @classmethod
    def _validate_asset_id(cls, value: str) -> str:
        return _strip_required_text(value)


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


class AssetSnapshotResponse(BaseModel):
    asset_id: str
    exists: bool
    asset_status: str | None = None
    available_actions: list[str] = Field(default_factory=list)
    device_status: str


class ScanResultResponse(BaseModel):
    asset_id: str
    exists: bool
    asset_status: str | None = None
    device_status: str


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
