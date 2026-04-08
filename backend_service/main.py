from __future__ import annotations

import argparse
import json
import logging
import time

import runtime_paths  # noqa: F401
from models import AssetStatus
from repository import InMemoryTransactionRepository
from serial_manager import SerialManager
from service import AssetConfirmService

CLI_STATUS_MAP = {
    "in_stock": AssetStatus.IN_STOCK,
    "borrowed": AssetStatus.BORROWED,
    "maintenance": AssetStatus.MAINTENANCE,
    "scrapped": AssetStatus.SCRAPPED,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="办公室资产租借管理串口通信演示程序")
    parser.add_argument("--port", required=True, help="串口号，例如 COM3 / /dev/ttyUSB0 / socket://127.0.0.1:9000")
    parser.add_argument("--baudrate", type=int, default=115200, help="串口波特率，默认 115200")
    parser.add_argument("--timeout-ms", type=int, default=30000, help="硬件等待确认超时毫秒数")
    parser.add_argument(
        "--initial-status",
        choices=sorted(CLI_STATUS_MAP.keys()),
        help="仅用于单次 CLI 演示时预置资产状态；未提供时，内存仓储会将该资产视为不存在",
    )
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    subparsers = parser.add_subparsers(dest="command", required=True)

    borrow_parser = subparsers.add_parser("borrow", help="发起借用确认")
    borrow_parser.add_argument("--asset-id", required=True, help="资产 ID")
    borrow_parser.add_argument("--user-id", required=True, help="用户唯一标识")
    borrow_parser.add_argument("--user-name", required=True, help="用户姓名")

    return_parser = subparsers.add_parser("return", help="发起归还确认")
    return_parser.add_argument("--asset-id", required=True, help="资产 ID")
    return_parser.add_argument("--user-id", required=True, help="用户唯一标识")
    return_parser.add_argument("--user-name", required=True, help="用户姓名")
    return parser


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(args.log_level)

    serial_manager = SerialManager(port=args.port, baudrate=args.baudrate)
    initial_assets: dict[str, AssetStatus] = {}
    if args.initial_status is not None:
        initial_assets[args.asset_id] = CLI_STATUS_MAP[args.initial_status]
    service = AssetConfirmService(
        serial_manager=serial_manager,
        repository=InMemoryTransactionRepository(initial_assets=initial_assets),
    )

    try:
        service.open()
        time.sleep(0.5)
        if args.command == "borrow":
            result = service.request_asset_borrow_confirm(
                asset_id=args.asset_id,
                user_id=args.user_id,
                user_name=args.user_name,
                timeout_ms=args.timeout_ms,
            )
        else:
            result = service.request_asset_return_confirm(
                asset_id=args.asset_id,
                user_id=args.user_id,
                user_name=args.user_name,
                timeout_ms=args.timeout_ms,
            )
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    finally:
        service.close()


if __name__ == "__main__":
    main()
