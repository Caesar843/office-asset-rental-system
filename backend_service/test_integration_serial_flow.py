from __future__ import annotations

import time
import unittest

import runtime_paths  # noqa: F401
from mock_mcu import MockMCUServer
from models import AssetStatus, ConfirmResult, TransactionState
from repository import InMemoryTransactionRepository
from serial_manager import SerialManager
from service import AssetConfirmService
from transaction_manager import TransactionManager


class SerialIntegrationFlowTests(unittest.TestCase):
    def run_scenario(
        self,
        *,
        port: int,
        mode: str,
        initial_status: AssetStatus,
        action: str,
        timeout_ms: int = 200,
        confirm_delay: float = 0.05,
    ):
        mock_server = MockMCUServer(host="127.0.0.1", port=port, mode=mode, confirm_delay=confirm_delay)
        mock_server.start()
        time.sleep(0.2)

        serial_manager = SerialManager(
            port=f"socket://127.0.0.1:{port}",
            ack_timeout=0.1,
            max_retries=3,
        )
        repository = InMemoryTransactionRepository(initial_assets={"AS-2001": initial_status})
        service = AssetConfirmService(
            serial_manager=serial_manager,
            repository=repository,
            transaction_manager=TransactionManager(hw_wait_grace_seconds=0.0),
        )

        try:
            service.open()
            time.sleep(0.2)
            if action == "borrow":
                result = service.request_asset_borrow_confirm(
                    asset_id="AS-2001",
                    user_id="U-2001",
                    user_name="李青云",
                    timeout_ms=timeout_ms,
                )
            else:
                result = service.request_asset_return_confirm(
                    asset_id="AS-2001",
                    user_id="U-2001",
                    user_name="李青云",
                    timeout_ms=timeout_ms,
                )
            return result, repository
        finally:
            service.close()
            mock_server.stop()

    def test_borrow_success_via_real_serial_stack(self) -> None:
        result, repository = self.run_scenario(
            port=9201,
            mode="confirmed",
            initial_status=AssetStatus.IN_STOCK,
            action="borrow",
        )

        self.assertTrue(result.success)
        self.assertEqual(result.code, ConfirmResult.CONFIRMED.value)
        self.assertEqual(result.transaction_state, TransactionState.COMPLETED)
        self.assertEqual(repository.assets["AS-2001"], AssetStatus.BORROWED)

    def test_return_success_via_real_serial_stack(self) -> None:
        result, repository = self.run_scenario(
            port=9202,
            mode="confirmed",
            initial_status=AssetStatus.BORROWED,
            action="return",
        )

        self.assertTrue(result.success)
        self.assertEqual(result.code, ConfirmResult.CONFIRMED.value)
        self.assertEqual(result.transaction_state, TransactionState.COMPLETED)
        self.assertEqual(repository.assets["AS-2001"], AssetStatus.IN_STOCK)

    def test_ack_invalid_via_real_serial_stack(self) -> None:
        result, repository = self.run_scenario(
            port=9203,
            mode="invalid",
            initial_status=AssetStatus.IN_STOCK,
            action="borrow",
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.ACK_INVALID.value)
        self.assertEqual(result.transaction_state, TransactionState.FAILED)
        self.assertEqual(repository.assets["AS-2001"], AssetStatus.IN_STOCK)

    def test_ack_error_via_real_serial_stack(self) -> None:
        result, repository = self.run_scenario(
            port=9204,
            mode="ack_error",
            initial_status=AssetStatus.IN_STOCK,
            action="borrow",
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.ACK_ERROR.value)
        self.assertEqual(result.transaction_state, TransactionState.FAILED)
        self.assertEqual(repository.assets["AS-2001"], AssetStatus.IN_STOCK)

    def test_ack_timeout_via_real_serial_stack(self) -> None:
        result, repository = self.run_scenario(
            port=9205,
            mode="no_ack",
            initial_status=AssetStatus.IN_STOCK,
            action="borrow",
            timeout_ms=50,
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.ACK_TIMEOUT.value)
        self.assertEqual(result.transaction_state, TransactionState.FAILED)
        self.assertEqual(repository.assets["AS-2001"], AssetStatus.IN_STOCK)

    def test_mismatch_request_id_results_in_hw_timeout_via_real_serial_stack(self) -> None:
        result, repository = self.run_scenario(
            port=9206,
            mode="mismatch_request_id",
            initial_status=AssetStatus.IN_STOCK,
            action="borrow",
            timeout_ms=80,
            confirm_delay=0.01,
        )

        self.assertFalse(result.success)
        self.assertEqual(result.code, ConfirmResult.HW_RESULT_TIMEOUT.value)
        self.assertEqual(result.transaction_state, TransactionState.FAILED)
        self.assertEqual(repository.assets["AS-2001"], AssetStatus.IN_STOCK)


if __name__ == "__main__":
    unittest.main()
