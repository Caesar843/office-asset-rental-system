from __future__ import annotations

import io
import json
import os
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import run_mock_api_flow
import run_real_device_flow
from start_demo_stack import print_summary


def build_mock_args(**overrides) -> Namespace:
    defaults = {
        "action": "borrow",
        "asset_id": "AS-1001",
        "user_id": "U-1001",
        "user_name": "Demo User",
        "asset_name": run_mock_api_flow.DEFAULT_INBOUND_ASSET_NAME,
        "category_id": run_mock_api_flow.DEFAULT_INBOUND_CATEGORY_ID,
        "location": run_mock_api_flow.DEFAULT_INBOUND_LOCATION,
        "timeout_ms": 300,
        "repository_kind": "inmemory",
        "initial_status": "",
        "mock_host": "127.0.0.1",
        "mock_port": 9100,
        "mock_mode": "confirmed",
        "mock_delay": 0.05,
        "mock_heartbeat": 5.0,
        "serial_ack_timeout": 0.1,
        "serial_max_retries": 3,
        "serial_offline_timeout": 15.0,
        "health_wait": 0.2,
        "log_level": "INFO",
    }
    defaults.update(overrides)
    return Namespace(**defaults)


def build_real_args(**overrides) -> Namespace:
    defaults = {
        "action": "borrow",
        "asset_id": "AS-1001",
        "user_id": "U-1001",
        "user_name": "Demo User",
        "asset_name": run_real_device_flow.DEFAULT_INBOUND_ASSET_NAME,
        "category_id": run_real_device_flow.DEFAULT_INBOUND_CATEGORY_ID,
        "location": run_real_device_flow.DEFAULT_INBOUND_LOCATION,
        "timeout_ms": 30000,
        "repository_kind": "inmemory",
        "serial_port": "COM7",
        "baudrate": 115200,
        "serial_ack_timeout": 0.5,
        "serial_max_retries": 3,
        "serial_offline_timeout": 15.0,
        "initial_status": "",
        "health_wait": 0.5,
        "allow_socket": False,
        "log_level": "INFO",
    }
    defaults.update(overrides)
    return Namespace(**defaults)


def build_demo_args(**overrides) -> Namespace:
    defaults = {
        "api_host": "127.0.0.1",
        "api_port": 8000,
        "repository_kind": "mysql",
        "transport_mode": "mock",
        "serial_port": "socket://127.0.0.1:9100",
        "baudrate": 115200,
        "ack_timeout": 0.5,
        "max_retries": 3,
        "offline_timeout": 15.0,
        "mock_host": "127.0.0.1",
        "mock_port": 9100,
        "mock_mode": "confirmed",
        "mock_delay": 0.2,
        "mock_heartbeat": 5.0,
        "skip_mock": False,
        "initial_assets_json": "",
        "log_level": "INFO",
    }
    defaults.update(overrides)
    return Namespace(**defaults)


class RehearsalScriptTests(unittest.TestCase):
    def test_mock_flow_parser_and_payload_support_inbound(self) -> None:
        args = run_mock_api_flow.build_arg_parser().parse_args(
            [
                "--action",
                "inbound",
                "--asset-id",
                "AS-0926",
                "--user-id",
                "U-ADMIN",
                "--user-name",
                "Demo Admin",
                "--asset-name",
                "Demo Inbound Asset",
                "--category-id",
                "1",
                "--location",
                "Inbound Shelf",
            ]
        )

        payload = run_mock_api_flow.build_request_payload(args)

        self.assertEqual(run_mock_api_flow.ACTION_ENDPOINTS[args.action], "/transactions/inbound")
        self.assertEqual(
            payload,
            {
                "asset_id": "AS-0926",
                "user_id": "U-ADMIN",
                "user_name": "Demo Admin",
                "timeout_ms": 300,
                "asset_name": "Demo Inbound Asset",
                "category_id": 1,
                "location": "Inbound Shelf",
            },
        )

    def test_mock_flow_inmemory_inbound_does_not_seed_existing_asset(self) -> None:
        args = build_mock_args(action="inbound", asset_id="AS-0926", user_id="U-ADMIN", user_name="Demo Admin")

        with patch.dict(os.environ, {}, clear=False):
            run_mock_api_flow.configure_environment(args)

            self.assertNotIn("BACKEND_INITIAL_ASSETS_JSON", os.environ)

    def test_real_flow_parser_and_payload_support_inbound(self) -> None:
        args = run_real_device_flow.build_arg_parser().parse_args(
            [
                "--action",
                "inbound",
                "--asset-id",
                "AS-0926",
                "--user-id",
                "U-ADMIN",
                "--user-name",
                "Demo Admin",
                "--asset-name",
                "Demo Inbound Asset",
                "--category-id",
                "1",
                "--location",
                "Inbound Shelf",
            ]
        )

        payload = run_real_device_flow.build_request_payload(args)

        self.assertEqual(run_real_device_flow.ACTION_ENDPOINTS[args.action], "/transactions/inbound")
        self.assertEqual(
            payload,
            {
                "asset_id": "AS-0926",
                "user_id": "U-ADMIN",
                "user_name": "Demo Admin",
                "timeout_ms": 30000,
                "asset_name": "Demo Inbound Asset",
                "category_id": 1,
                "location": "Inbound Shelf",
            },
        )

    def test_real_flow_inmemory_inbound_does_not_seed_existing_asset(self) -> None:
        args = build_real_args(
            action="inbound",
            asset_id="AS-0926",
            user_id="U-ADMIN",
            user_name="Demo Admin",
            serial_port="socket://127.0.0.1:9100",
            allow_socket=True,
        )

        with patch.dict(os.environ, {}, clear=False):
            serial_port = run_real_device_flow.configure_environment(args)

            self.assertEqual(serial_port, "socket://127.0.0.1:9100")
            self.assertNotIn("BACKEND_INITIAL_ASSETS_JSON", os.environ)

    def test_start_demo_stack_summary_contains_inbound_example(self) -> None:
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            print_summary(build_demo_args(), serial_port="socket://127.0.0.1:9100", transport_mode="mock")

        payload = json.loads(buffer.getvalue())

        self.assertIn("inbound_example", payload)
        self.assertEqual(payload["inbound_example"]["url"], "http://127.0.0.1:8000/transactions/inbound")
        self.assertEqual(
            payload["inbound_example"]["json"],
            {
                "asset_id": "AS-0926",
                "user_id": "U-ADMIN",
                "user_name": "Demo Admin",
                "asset_name": "Demo Inbound Asset",
                "category_id": 1,
                "location": "Inbound Shelf",
                "timeout_ms": 3000,
            },
        )

    def test_competition_runbook_contains_inbound_rehearsal_paths(self) -> None:
        runbook_text = Path("competition_runbook.md").read_text(encoding="utf-8")

        self.assertIn("run_real_device_flow.py --action inbound", runbook_text)
        self.assertIn("run_mock_api_flow.py --action inbound", runbook_text)
        self.assertIn("/transactions/inbound", runbook_text)
        self.assertIn("asset_name", runbook_text)
        self.assertIn("category_id", runbook_text)
        self.assertIn("location", runbook_text)
        self.assertIn("WAITING_ACK", runbook_text)
        self.assertIn("WAITING_HW", runbook_text)


if __name__ == "__main__":
    unittest.main()
