from __future__ import annotations

import unittest

from models import (
    ActionType,
    AssetStatus,
    ConfirmResult,
    DeviceStatus,
    InboundRuleCheckRequest,
    RuleCheckRequest,
)
from rule_service import RuleService


class RuleServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.rule_service = RuleService()

    def build_request(
        self,
        *,
        action_type: ActionType,
        asset_status: AssetStatus | None,
        device_status: DeviceStatus = DeviceStatus.ONLINE,
        has_pending_transaction: bool = False,
    ) -> RuleCheckRequest:
        return RuleCheckRequest(
            asset_id="AS-1001",
            user_id="U-1001",
            user_name="赵子墨",
            action_type=action_type,
            device_status=device_status,
            asset_status=asset_status,
            has_pending_transaction=has_pending_transaction,
        )

    def build_inbound_request(
        self,
        *,
        asset_status: AssetStatus | None,
        user_id: str = "U-ADMIN",
        device_status: DeviceStatus = DeviceStatus.ONLINE,
        has_pending_transaction: bool = False,
        asset_name: str = "ThinkPad X1",
        category_id: int | None = 1,
        location: str = "Rack A",
        has_inbound_permission: bool = True,
        category_exists: bool = True,
    ) -> InboundRuleCheckRequest:
        return InboundRuleCheckRequest(
            asset_id="AS-9001",
            user_id=user_id,
            user_name="管理员",
            action_type=ActionType.INBOUND,
            device_status=device_status,
            asset_status=asset_status,
            has_pending_transaction=has_pending_transaction,
            asset_name=asset_name,
            category_id=category_id,
            location=location,
            has_inbound_permission=has_inbound_permission,
            category_exists=category_exists,
        )

    def test_offline_device_blocks_borrow(self) -> None:
        result = self.rule_service.check_request(
            self.build_request(
                action_type=ActionType.BORROW,
                asset_status=AssetStatus.IN_STOCK,
                device_status=DeviceStatus.OFFLINE,
            )
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.code, ConfirmResult.DEVICE_OFFLINE.value)

    def test_missing_asset_blocks_request(self) -> None:
        result = self.rule_service.check_request(
            self.build_request(action_type=ActionType.BORROW, asset_status=None)
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.code, ConfirmResult.ASSET_NOT_FOUND.value)

    def test_borrow_invalid_states_are_rejected(self) -> None:
        for status in (AssetStatus.BORROWED, AssetStatus.MAINTENANCE, AssetStatus.SCRAPPED):
            with self.subTest(status=status):
                result = self.rule_service.check_request(
                    self.build_request(action_type=ActionType.BORROW, asset_status=status)
                )
                self.assertFalse(result.passed)
                self.assertEqual(result.code, ConfirmResult.STATE_INVALID.value)
                self.assertEqual(result.extra["asset_status"], status.value)

    def test_return_invalid_states_are_rejected(self) -> None:
        for status in (AssetStatus.IN_STOCK, AssetStatus.MAINTENANCE, AssetStatus.SCRAPPED):
            with self.subTest(status=status):
                result = self.rule_service.check_request(
                    self.build_request(action_type=ActionType.RETURN, asset_status=status)
                )
                self.assertFalse(result.passed)
                self.assertEqual(result.code, ConfirmResult.STATE_INVALID.value)
                self.assertEqual(result.extra["asset_status"], status.value)

    def test_pending_transaction_blocks_request(self) -> None:
        result = self.rule_service.check_request(
            self.build_request(
                action_type=ActionType.BORROW,
                asset_status=AssetStatus.IN_STOCK,
                has_pending_transaction=True,
            )
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.code, ConfirmResult.BUSY.value)

    def test_valid_borrow_passes(self) -> None:
        result = self.rule_service.check_request(
            self.build_request(action_type=ActionType.BORROW, asset_status=AssetStatus.IN_STOCK)
        )

        self.assertTrue(result.passed)
        self.assertEqual(result.code, ConfirmResult.CONFIRMED.value)

    def test_valid_return_passes(self) -> None:
        result = self.rule_service.check_request(
            self.build_request(action_type=ActionType.RETURN, asset_status=AssetStatus.BORROWED)
        )

        self.assertTrue(result.passed)
        self.assertEqual(result.code, ConfirmResult.CONFIRMED.value)

    def test_valid_inbound_passes(self) -> None:
        result = self.rule_service.check_request(self.build_inbound_request(asset_status=None))

        self.assertTrue(result.passed)
        self.assertEqual(result.code, ConfirmResult.CONFIRMED.value)

    def test_inbound_requires_admin_permission(self) -> None:
        result = self.rule_service.check_request(
            self.build_inbound_request(asset_status=None, has_inbound_permission=False, user_id="U-1001")
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.code, ConfirmResult.PERMISSION_DENIED.value)

    def test_inbound_requires_asset_name(self) -> None:
        result = self.rule_service.check_request(
            self.build_inbound_request(asset_status=None, asset_name="")
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.code, ConfirmResult.PARAM_INVALID.value)

    def test_inbound_rejects_existing_asset(self) -> None:
        result = self.rule_service.check_request(
            self.build_inbound_request(asset_status=AssetStatus.IN_STOCK)
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.code, ConfirmResult.STATE_INVALID.value)

    def test_inbound_rejects_invalid_category(self) -> None:
        result = self.rule_service.check_request(
            self.build_inbound_request(asset_status=None, category_exists=False, category_id=9)
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.code, ConfirmResult.PARAM_INVALID.value)


if __name__ == "__main__":
    unittest.main()
