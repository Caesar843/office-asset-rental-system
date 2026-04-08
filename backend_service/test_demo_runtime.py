from __future__ import annotations

import os
import unittest
from argparse import Namespace
from unittest.mock import patch

from check_real_serial_runtime import build_next_steps, classify_open_error
from start_demo_stack import configure_environment, resolve_transport_mode


def build_demo_args(**overrides) -> Namespace:
    defaults = {
        "api_host": "127.0.0.1",
        "api_port": 8000,
        "repository_kind": "mysql",
        "transport_mode": "",
        "serial_port": "",
        "baudrate": 115200,
        "ack_timeout": 0.5,
        "max_retries": 3,
        "offline_timeout": 15.0,
        "mock_host": "127.0.0.1",
        "mock_port": 9100,
        "initial_assets_json": "",
        "skip_mock": False,
    }
    defaults.update(overrides)
    return Namespace(**defaults)


class DemoRuntimeTests(unittest.TestCase):
    def test_resolve_transport_mode_defaults_to_mock(self) -> None:
        args = build_demo_args()
        self.assertEqual(resolve_transport_mode(args), "mock")

    def test_resolve_transport_mode_detects_real_serial_target(self) -> None:
        args = build_demo_args(serial_port="COM7")
        self.assertEqual(resolve_transport_mode(args), "real")
        forced_mock = build_demo_args(serial_port="COM7", transport_mode="mock")
        self.assertEqual(resolve_transport_mode(forced_mock), "mock")

    def test_configure_environment_sets_mock_socket_defaults(self) -> None:
        args = build_demo_args(transport_mode="mock")
        with patch.dict(os.environ, {}, clear=False):
            serial_port, transport_mode = configure_environment(args)
            self.assertEqual(os.environ["BACKEND_SERIAL_PORT"], "socket://127.0.0.1:9100")

        self.assertEqual(transport_mode, "mock")
        self.assertEqual(serial_port, "socket://127.0.0.1:9100")
        self.assertFalse(args.skip_mock)

    def test_configure_environment_real_requires_physical_port(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires --serial-port"):
            configure_environment(build_demo_args(transport_mode="real"))

        with self.assertRaisesRegex(ValueError, "physical serial port"):
            configure_environment(build_demo_args(transport_mode="real", serial_port="socket://127.0.0.1:9100"))

    def test_configure_environment_real_disables_mock_autostart(self) -> None:
        args = build_demo_args(transport_mode="real", serial_port="COM7")
        with patch.dict(os.environ, {}, clear=False):
            serial_port, transport_mode = configure_environment(args)
            self.assertEqual(os.environ["BACKEND_SERIAL_PORT"], "COM7")

        self.assertEqual(transport_mode, "real")
        self.assertEqual(serial_port, "COM7")
        self.assertTrue(args.skip_mock)

    def test_real_serial_failure_classification_and_next_steps(self) -> None:
        self.assertEqual(classify_open_error(pyserial_available=False, detail="RuntimeError"), "pyserial_missing")
        self.assertEqual(
            classify_open_error(pyserial_available=True, detail="could not open port COM7: FileNotFoundError"),
            "serial_port_not_found",
        )
        self.assertEqual(
            classify_open_error(pyserial_available=True, detail="Access is denied"),
            "serial_port_busy",
        )
        steps = build_next_steps(
            serial_port="COM7",
            error_kind="serial_port_busy",
            mode="real_serial",
            wait_seconds=3.0,
        )
        self.assertEqual(len(steps), 3)
        self.assertIn("COM7", steps[0])


if __name__ == "__main__":
    unittest.main()
