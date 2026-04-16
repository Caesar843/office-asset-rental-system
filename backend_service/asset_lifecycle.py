from __future__ import annotations

from models import ActionType, AssetStatus


def validate_asset_transition(current_status: AssetStatus, action_type: ActionType) -> str | None:
    if action_type == ActionType.BORROW:
        if current_status == AssetStatus.IN_STOCK:
            return None
        if current_status == AssetStatus.BORROWED:
            return "资产当前处于借出状态，不允许再次发起借出"
        if current_status == AssetStatus.MAINTENANCE:
            return "资产当前处于维修状态，禁止发起借出"
        return "资产当前处于报废状态，禁止发起借出"

    if action_type == ActionType.INBOUND:
        return "资产入库必须通过独立入库提交流程处理"

    if current_status == AssetStatus.BORROWED:
        return None
    if current_status == AssetStatus.IN_STOCK:
        return "资产当前处于在库状态，不允许发起归还"
    if current_status == AssetStatus.MAINTENANCE:
        return "资产当前处于维修状态，禁止发起归还"
    return "资产当前处于报废状态，禁止发起归还"


def next_asset_status_for_action(action_type: ActionType) -> AssetStatus:
    if action_type == ActionType.BORROW:
        return AssetStatus.BORROWED
    if action_type == ActionType.RETURN:
        return AssetStatus.IN_STOCK
    raise ValueError(f"unsupported asset transition action: {action_type.value}")
