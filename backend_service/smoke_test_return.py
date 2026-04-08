from __future__ import annotations

import json
import logging
import time

import runtime_paths  # noqa: F401
from mock_mcu import MockMCUServer
from models import AssetStatus
from repository import InMemoryTransactionRepository
from serial_manager import SerialManager
from service import AssetConfirmService


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")


def main() -> None:
    configure_logging()
    mock_server = MockMCUServer(host="127.0.0.1", port=9101, mode="confirmed", confirm_delay=1.0)
    mock_server.start()
    time.sleep(0.3)

    serial_manager = SerialManager(port="socket://127.0.0.1:9101")
    repository = InMemoryTransactionRepository(initial_assets={"AS-0925": AssetStatus.BORROWED})
    service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

    try:
        service.open()
        time.sleep(0.5)
        result = service.request_asset_return_confirm(
            asset_id="AS-0925",
            user_id="U-1002",
            user_name="苏明月",
            timeout_ms=5000,
        )
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
        assert result.success is True
        assert result.code == "CONFIRMED"
        assert result.hw_result == "CONFIRMED"
        assert result.user_id == "U-1002"
        assert result.request_seq == result.seq_id
        assert repository.assets["AS-0925"] == AssetStatus.IN_STOCK
    finally:
        service.close()
        mock_server.stop()


if __name__ == "__main__":
    main()
