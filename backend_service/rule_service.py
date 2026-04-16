from __future__ import annotations

from asset_lifecycle import validate_asset_transition
from models import (
    ActionType,
    ConfirmResult,
    DeviceStatus,
    InboundRuleCheckRequest,
    RuleCheckRequest,
    RuleCheckResult,
)


class RuleService:
    def check_request(self, request: RuleCheckRequest | InboundRuleCheckRequest) -> RuleCheckResult:
        action_check = self.check_action_type(request)
        if action_check is not None:
            return action_check

        if request.action_type == ActionType.BORROW:
            return self.check_borrow(request)
        if request.action_type == ActionType.RETURN:
            return self.check_return(request)
        if request.action_type == ActionType.INBOUND:
            if not isinstance(request, InboundRuleCheckRequest):
                return self._failed(
                    request=request,
                    code=ConfirmResult.INTERNAL_ERROR.value,
                    message="入库规则请求对象非法",
                )
            return self.check_inbound(request)

        return self._failed(
            request=request,
            code=ConfirmResult.INTERNAL_ERROR.value,
            message="不支持的动作类型",
        )

    def check_borrow(self, request: RuleCheckRequest) -> RuleCheckResult:
        return self._run_common_checks(request)

    def check_return(self, request: RuleCheckRequest) -> RuleCheckResult:
        return self._run_common_checks(request)

    def check_inbound(self, request: InboundRuleCheckRequest) -> RuleCheckResult:
        for checker in (
            self.check_device_status,
            self.check_inbound_required_fields,
            self.check_inbound_permission,
            self.check_inbound_category,
            self.check_pending_conflict,
            self.check_inbound_asset_absent,
        ):
            result = checker(request)
            if result is not None:
                return result
        return self._passed(request)

    def check_device_status(self, request: RuleCheckRequest) -> RuleCheckResult | None:
        if request.device_status != DeviceStatus.OFFLINE:
            return None
        return self._failed(
            request=request,
            code=ConfirmResult.DEVICE_OFFLINE.value,
            message="设备离线，无法发起确认请求",
        )

    def check_action_type(self, request: RuleCheckRequest) -> RuleCheckResult | None:
        if request.action_type in (ActionType.BORROW, ActionType.RETURN, ActionType.INBOUND):
            return None
        return self._failed(
            request=request,
            code=ConfirmResult.INTERNAL_ERROR.value,
            message="不支持的动作类型",
        )

    def check_asset_exists(self, request: RuleCheckRequest) -> RuleCheckResult | None:
        if request.asset_status is not None:
            return None
        return self._failed(
            request=request,
            code=ConfirmResult.ASSET_NOT_FOUND.value,
            message="资产不存在，无法发起确认请求",
        )

    def check_asset_transition(self, request: RuleCheckRequest) -> RuleCheckResult | None:
        if request.asset_status is None:
            return None

        invalid_reason = validate_asset_transition(request.asset_status, request.action_type)
        if invalid_reason is None:
            return None

        return self._failed(
            request=request,
            code=ConfirmResult.STATE_INVALID.value,
            message=invalid_reason,
            extra={"asset_status": request.asset_status.value},
        )

    def check_pending_conflict(self, request: RuleCheckRequest) -> RuleCheckResult | None:
        if not request.has_pending_transaction:
            return None
        return self._failed(
            request=request,
            code=ConfirmResult.BUSY.value,
            message="该资产已有待确认事务，请勿重复提交",
            extra=self._asset_status_extra(request),
        )

    def check_inbound_required_fields(self, request: InboundRuleCheckRequest) -> RuleCheckResult | None:
        if not request.asset_id.strip():
            return self._failed(request=request, code=ConfirmResult.PARAM_INVALID.value, message="asset_id 不能为空")
        if not request.user_id.strip():
            return self._failed(request=request, code=ConfirmResult.PARAM_INVALID.value, message="user_id 不能为空")
        if not request.user_name.strip():
            return self._failed(request=request, code=ConfirmResult.PARAM_INVALID.value, message="user_name 不能为空")
        if not request.asset_name.strip():
            return self._failed(request=request, code=ConfirmResult.PARAM_INVALID.value, message="asset_name 不能为空")
        if not request.location.strip():
            return self._failed(request=request, code=ConfirmResult.PARAM_INVALID.value, message="location 不能为空")
        if request.category_id is not None and request.category_id <= 0:
            return self._failed(
                request=request,
                code=ConfirmResult.PARAM_INVALID.value,
                message="category_id 必须为正整数",
            )
        return None

    def check_inbound_permission(self, request: InboundRuleCheckRequest) -> RuleCheckResult | None:
        if request.has_inbound_permission:
            return None
        return self._failed(
            request=request,
            code=ConfirmResult.PERMISSION_DENIED.value,
            message="只有管理员可发起入库",
        )

    def check_inbound_category(self, request: InboundRuleCheckRequest) -> RuleCheckResult | None:
        if request.category_id is None or request.category_exists:
            return None
        return self._failed(
            request=request,
            code=ConfirmResult.PARAM_INVALID.value,
            message=f"分类不存在: {request.category_id}",
        )

    def check_inbound_asset_absent(self, request: InboundRuleCheckRequest) -> RuleCheckResult | None:
        if request.asset_status is None:
            return None
        return self._failed(
            request=request,
            code=ConfirmResult.STATE_INVALID.value,
            message="资产已存在，不允许重复入库",
            extra={"asset_status": request.asset_status.value},
        )

    def _run_common_checks(self, request: RuleCheckRequest) -> RuleCheckResult:
        for checker in (
            self.check_device_status,
            self.check_asset_exists,
            self.check_asset_transition,
            self.check_pending_conflict,
        ):
            result = checker(request)
            if result is not None:
                return result
        return self._passed(request)

    @staticmethod
    def _passed(request: RuleCheckRequest) -> RuleCheckResult:
        return RuleCheckResult(
            passed=True,
            code=ConfirmResult.CONFIRMED.value,
            message="规则校验通过",
            action_type=request.action_type,
            asset_id=request.asset_id,
            user_id=request.user_id,
            extra=RuleService._asset_status_extra(request),
        )

    @staticmethod
    def _failed(
        request: RuleCheckRequest,
        code: str,
        message: str,
        extra: dict[str, str] | None = None,
    ) -> RuleCheckResult:
        return RuleCheckResult(
            passed=False,
            code=code,
            message=message,
            action_type=request.action_type,
            asset_id=request.asset_id,
            user_id=request.user_id,
            extra=extra or {},
        )

    @staticmethod
    def _asset_status_extra(request: RuleCheckRequest) -> dict[str, str]:
        if request.asset_status is None:
            return {}
        return {"asset_status": request.asset_status.value}
