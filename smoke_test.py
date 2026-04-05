from __future__ import annotations

import json
import logging
import time

from mock_mcu import MockMCUServer
from models import AssetStatus
from serial_manager import SerialManager
from service import AssetConfirmService, InMemoryTransactionRepository


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")


def main() -> None:
    configure_logging()
    mock_server = MockMCUServer(host="127.0.0.1", port=9100, mode="confirmed", confirm_delay=1.0)
    mock_server.start()
    time.sleep(0.3)

    serial_manager = SerialManager(port="socket://127.0.0.1:9100")
    repository = InMemoryTransactionRepository(initial_assets={"AS-0924": AssetStatus.IN_STOCK})
    service = AssetConfirmService(serial_manager=serial_manager, repository=repository)

    try:
        service.open()
        time.sleep(0.5)
        result = service.request_asset_borrow_confirm(
            asset_id="AS-0924",
            user_id="U-1001",
            user_name="赵子墨",
            timeout_ms=5000,
        )
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
        assert result.success is True
        assert result.code == "CONFIRMED"
        assert result.hw_result == "CONFIRMED"
        assert result.user_id == "U-1001"
        assert result.request_seq == result.seq_id
    finally:
        service.close()
        mock_server.stop()


if __name__ == "__main__":
    main()
