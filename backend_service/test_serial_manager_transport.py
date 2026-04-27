from __future__ import annotations

import threading
import time
import unittest
from collections import deque
from unittest.mock import patch

import runtime_paths  # noqa: F401
import serial_manager as serial_runtime
from models import DeviceStatus
from protocol import Frame, MsgType, encode_frame
from serial_manager import SerialManager


class FakeSerialTransport:
    def __init__(self) -> None:
        self.events: list[tuple[str, object, object | None]] = []
        self.is_open = False
        self._dtr = True
        self._rts = True

    @property
    def dtr(self) -> bool:
        return self._dtr

    @dtr.setter
    def dtr(self, value: bool) -> None:
        self._dtr = value
        self.events.append(("dtr", value, self.is_open))

    @property
    def rts(self) -> bool:
        return self._rts

    @rts.setter
    def rts(self, value: bool) -> None:
        self._rts = value
        self.events.append(("rts", value, self.is_open))

    def open(self) -> None:
        self.events.append(("open", self._dtr, self._rts))
        self.is_open = True

    def close(self) -> None:
        self.is_open = False

    def reset_input_buffer(self) -> None:
        self.events.append(("reset_input_buffer", self.is_open, None))

    def reset_output_buffer(self) -> None:
        self.events.append(("reset_output_buffer", self.is_open, None))


class FakeSerialModule:
    EIGHTBITS = 8
    PARITY_NONE = "N"
    STOPBITS_ONE = 1

    def __init__(self) -> None:
        self.created_transport = FakeSerialTransport()
        self.calls: list[tuple[str, dict[str, object]]] = []

    def serial_for_url(self, port: str, **kwargs):
        self.calls.append((port, kwargs))
        return self.created_transport


class AckFailingStreamingTransport:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = deque(chunks)
        self.is_open = True
        self.read_calls = 0
        self.write_calls = 0

    def read(self, size: int = 1) -> bytes:
        self.read_calls += 1
        if self._chunks:
            return self._chunks.popleft()
        time.sleep(0.01)
        return b""

    def write(self, data: bytes) -> int:
        self.write_calls += 1
        raise TimeoutError("simulated ACK write timeout")

    def close(self) -> None:
        self.is_open = False


class SerialManagerTransportTests(unittest.TestCase):
    def test_real_serial_open_keeps_control_lines_low_and_resets_buffers(self) -> None:
        fake_serial = FakeSerialModule()
        with patch.object(serial_runtime, "serial", fake_serial):
            transport = SerialManager(port="COM7", baudrate=115200)._open_transport()

        self.assertIs(transport, fake_serial.created_transport)
        self.assertEqual(len(fake_serial.calls), 1)
        port, kwargs = fake_serial.calls[0]
        self.assertEqual(port, "COM7")
        self.assertIs(kwargs["do_not_open"], True)
        self.assertIs(kwargs["xonxoff"], False)
        self.assertIs(kwargs["rtscts"], False)
        self.assertIs(kwargs["dsrdtr"], False)
        self.assertEqual(kwargs["write_timeout"], 0.2)
        self.assertTrue(transport.is_open)
        self.assertFalse(transport.dtr)
        self.assertFalse(transport.rts)
        self.assertEqual(
            transport.events,
            [
                ("dtr", False, False),
                ("rts", False, False),
                ("open", False, False),
                ("dtr", False, True),
                ("rts", False, True),
                ("reset_input_buffer", True, None),
                ("reset_output_buffer", True, None),
            ],
        )

    def test_reader_continues_after_ack_write_failure(self) -> None:
        first_heartbeat = encode_frame(Frame.build(MsgType.EVT_HEARTBEAT, 101, {"status": "alive"}))
        second_heartbeat = encode_frame(Frame.build(MsgType.EVT_HEARTBEAT, 102, {"status": "alive"}))
        transport = AckFailingStreamingTransport([first_heartbeat, second_heartbeat])
        manager = SerialManager(port="COM7", read_timeout=0.01)
        statuses: list[DeviceStatus] = []
        manager.set_status_handler(statuses.append)
        manager._transport = transport
        manager._stop_event.clear()

        reader = threading.Thread(target=manager._reader_loop, daemon=True)
        reader.start()
        deadline = time.monotonic() + 1.0
        while transport.write_calls < 2 and time.monotonic() < deadline:
            time.sleep(0.01)

        manager._stop_event.set()
        transport.close()
        reader.join(timeout=1.0)

        self.assertGreaterEqual(transport.write_calls, 2)
        self.assertGreaterEqual(transport.read_calls, 2)
        self.assertIn(DeviceStatus.ONLINE, statuses)
        self.assertFalse(reader.is_alive())


if __name__ == "__main__":
    unittest.main()
