from __future__ import annotations

import json
import logging
import socket
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol
from urllib.parse import urlparse

from models import DeviceStatus
from protocol import ACK_TYPES, Frame, FrameParser, MsgType, ParserEvent, build_ack_payload, encode_frame

LOGGER = logging.getLogger(__name__)

try:
    import serial  # type: ignore
except ImportError:
    serial = None


class Transport(Protocol):
    is_open: bool

    def read(self, size: int = 1) -> bytes: ...

    def write(self, data: bytes) -> int: ...

    def close(self) -> None: ...


class SocketTransport:
    """Fallback transport for socket://host:port integration tests."""

    def __init__(self, port_url: str, timeout: float = 0.2) -> None:
        parsed = urlparse(port_url)
        if parsed.scheme != "socket":
            raise ValueError(f"unsupported transport url: {port_url}")
        self._host = parsed.hostname or "127.0.0.1"
        self._port = parsed.port
        if self._port is None:
            raise ValueError(f"missing port in url: {port_url}")
        self._timeout = timeout
        self._socket = socket.create_connection((self._host, self._port), timeout=self._timeout)
        self._socket.settimeout(self._timeout)
        self.is_open = True

    def read(self, size: int = 1) -> bytes:
        if not self.is_open:
            return b""
        try:
            return self._socket.recv(size)
        except socket.timeout:
            return b""
        except OSError:
            self.is_open = False
            return b""

    def write(self, data: bytes) -> int:
        if not self.is_open:
            raise ConnectionError("socket transport is closed")
        self._socket.sendall(data)
        return len(data)

    def close(self) -> None:
        if not self.is_open:
            return
        self.is_open = False
        try:
            self._socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._socket.close()


@dataclass(slots=True)
class AckWaiter:
    seq_id: int
    event: threading.Event = field(default_factory=threading.Event)
    ack_type: MsgType | None = None
    ack_payload: dict[str, Any] | None = None


@dataclass(slots=True)
class SendResult:
    success: bool
    seq_id: int
    ack_type: MsgType | None
    message: str
    ack_payload: dict[str, Any] | None = None


class SerialManager:
    """
    Serial transport manager.

    The frame structure, ACK timeout, retry count, heartbeat monitoring and
    recent-seq de-duplication remain unchanged. Only payload details and error
    reporting are tightened for integration stability.
    """

    def __init__(
        self,
        port: str,
        baudrate: int = 115200,
        read_timeout: float = 0.2,
        ack_timeout: float = 0.5,
        max_retries: int = 3,
        offline_timeout: float = 15.0,
    ) -> None:
        self.port = port
        self.baudrate = baudrate
        self.read_timeout = read_timeout
        self.ack_timeout = ack_timeout
        self.max_retries = max_retries
        self.offline_timeout = offline_timeout

        self._transport: Transport | None = None
        self._parser = FrameParser()
        self._write_lock = threading.Lock()
        self._seq_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._stop_event = threading.Event()

        self._reader_thread: threading.Thread | None = None
        self._monitor_thread: threading.Thread | None = None

        self._next_pc_seq = 0
        self._pending_acks: dict[int, AckWaiter] = {}
        self._recent_incoming_seq: deque[int] = deque(maxlen=5)
        self._recent_incoming_seq_set: set[int] = set()
        self._last_received_at = 0.0
        self._device_status = DeviceStatus.UNKNOWN

        self._frame_handler: Callable[[Frame], None] | None = None
        self._status_handler: Callable[[DeviceStatus], None] | None = None

    @property
    def device_status(self) -> DeviceStatus:
        return self._device_status

    @property
    def is_open(self) -> bool:
        return self._transport is not None and self._transport.is_open

    def set_frame_handler(self, handler: Callable[[Frame], None]) -> None:
        self._frame_handler = handler

    def set_status_handler(self, handler: Callable[[DeviceStatus], None]) -> None:
        self._status_handler = handler

    def open(self) -> None:
        if self.is_open:
            return
        self._transport = self._open_transport()
        self._stop_event.clear()
        self._last_received_at = time.monotonic()
        self._set_device_status(DeviceStatus.UNKNOWN)
        self._reader_thread = threading.Thread(target=self._reader_loop, name="serial-reader", daemon=True)
        self._monitor_thread = threading.Thread(target=self._monitor_loop, name="serial-monitor", daemon=True)
        self._reader_thread.start()
        self._monitor_thread.start()
        LOGGER.info("serial opened: port=%s baudrate=%s", self.port, self.baudrate)

    def close(self) -> None:
        self._stop_event.set()
        transport = self._transport
        self._transport = None
        if transport is not None:
            transport.close()
            LOGGER.info("serial closed: port=%s", self.port)
        if self._reader_thread is not None:
            self._reader_thread.join(timeout=1.0)
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=1.0)
        self._set_device_status(DeviceStatus.OFFLINE)

    def reserve_seq_id(self) -> int:
        return self._next_seq_id()

    def send_request(self, msg_type: MsgType, payload: dict[str, Any], seq_id: int | None = None) -> SendResult:
        if not self.is_open:
            return SendResult(False, -1, None, "串口未打开")

        actual_seq_id = self._next_seq_id() if seq_id is None else seq_id
        wire_payload = dict(payload)
        # Keep request correlation aligned with the transport seq id.
        wire_payload.setdefault("request_seq", actual_seq_id)
        frame = Frame.build(msg_type=msg_type, seq_id=actual_seq_id, payload=wire_payload)
        waiter = AckWaiter(seq_id=actual_seq_id)
        with self._pending_lock:
            self._pending_acks[actual_seq_id] = waiter

        try:
            for attempt in range(1, self.max_retries + 1):
                waiter.ack_type = None
                waiter.ack_payload = None
                waiter.event.clear()
                self._send_frame(frame, retry=attempt)
                if waiter.event.wait(self.ack_timeout):
                    ack_type = waiter.ack_type
                    ack_payload = waiter.ack_payload or {}
                    detail = str(ack_payload.get("detail", "")).strip() or self._default_ack_message(ack_type)
                    LOGGER.info(
                        "ack matched: seq_id=%s ack=%s payload=%s",
                        actual_seq_id,
                        ack_type.name if ack_type is not None else None,
                        self._safe_payload_text(ack_payload),
                    )
                    if ack_type == MsgType.ACK_OK:
                        return SendResult(True, actual_seq_id, ack_type, detail, ack_payload)
                    return SendResult(False, actual_seq_id, ack_type, detail, ack_payload)

                LOGGER.warning("ack timeout: seq_id=%s retry=%s/%s", actual_seq_id, attempt, self.max_retries)

            self._set_device_status(DeviceStatus.OFFLINE)
            return SendResult(False, actual_seq_id, None, f"ACK 超时，已重传 {self.max_retries} 次", None)
        finally:
            with self._pending_lock:
                self._pending_acks.pop(actual_seq_id, None)

    def notify(self, message: str) -> SendResult:
        return self.send_request(MsgType.CMD_SYS_NOTIFY, {"message": message})

    def _open_transport(self) -> Transport:
        if serial is not None:
            return serial.serial_for_url(
                self.port,
                baudrate=self.baudrate,
                timeout=self.read_timeout,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
            )
        if self.port.startswith("socket://"):
            LOGGER.warning("pyserial not installed, fallback to socket transport for %s", self.port)
            return SocketTransport(self.port, timeout=self.read_timeout)
        raise RuntimeError("pyserial 未安装，真实串口模式无法使用。请先执行: pip install pyserial")

    def _reader_loop(self) -> None:
        while not self._stop_event.is_set():
            transport = self._transport
            if transport is None:
                return
            try:
                chunk = transport.read(1024)
            except Exception as exc:
                LOGGER.exception("serial read failed: %s", exc)
                self._set_device_status(DeviceStatus.OFFLINE)
                time.sleep(0.5)
                continue

            if not chunk:
                continue

            self._last_received_at = time.monotonic()
            self._set_device_status(DeviceStatus.ONLINE)
            for event in self._parser.feed(chunk):
                self._handle_parser_event(event)

    def _monitor_loop(self) -> None:
        while not self._stop_event.is_set():
            time.sleep(1.0)
            if not self.is_open:
                continue
            idle_seconds = time.monotonic() - self._last_received_at
            if idle_seconds >= self.offline_timeout:
                LOGGER.warning("device offline detected: no inbound frame for %.1fs", idle_seconds)
                self._set_device_status(DeviceStatus.OFFLINE)

    def _handle_parser_event(self, event: ParserEvent) -> None:
        if event.frame is not None:
            self._handle_frame(event.frame)
            return

        LOGGER.warning(
            "parser error: error=%s seq_id=%s msg_type=%s detail=%s",
            event.error,
            event.seq_id,
            event.msg_type,
            event.detail,
        )
        if event.seq_id is None:
            return
        if event.error == "crc_error":
            self._send_ack(event.seq_id, MsgType.ACK_ERROR, "CRC_ERROR")
        else:
            self._send_ack(event.seq_id, MsgType.ACK_INVALID, event.detail or "INVALID_FRAME")

    def _handle_frame(self, frame: Frame) -> None:
        LOGGER.info(
            "frame received: seq_id=%s msg_type=%s payload=%s",
            frame.seq_id,
            frame.msg_type.name,
            self._safe_payload_text(frame.payload),
        )

        if frame.msg_type in ACK_TYPES:
            self._handle_ack(frame)
            return

        duplicate = self._remember_incoming_seq(frame.seq_id)
        if duplicate:
            LOGGER.info("duplicate frame ignored: seq_id=%s msg_type=%s", frame.seq_id, frame.msg_type.name)
            self._send_ack(frame.seq_id, MsgType.ACK_OK, "DUPLICATE_FRAME_IGNORED", duplicate=True)
            return

        self._send_ack(frame.seq_id, MsgType.ACK_OK, "FRAME_RECEIVED")

        if frame.msg_type == MsgType.EVT_HEARTBEAT:
            LOGGER.info("heartbeat received: seq_id=%s payload=%s", frame.seq_id, self._safe_payload_text(frame.payload))
            return

        handler = self._frame_handler
        if handler is None:
            return
        try:
            handler(frame)
        except Exception as exc:
            LOGGER.exception("frame handler failed: %s", exc)

    def _handle_ack(self, frame: Frame) -> None:
        with self._pending_lock:
            waiter = self._pending_acks.get(frame.seq_id)
        if waiter is None:
            LOGGER.warning("orphan ack ignored: seq_id=%s ack=%s", frame.seq_id, frame.msg_type.name)
            return

        waiter.ack_type = frame.msg_type
        waiter.ack_payload = frame.payload if isinstance(frame.payload, dict) else {}
        waiter.event.set()

    def _send_ack(self, seq_id: int, ack_type: MsgType, detail: str, duplicate: bool = False) -> None:
        frame = Frame.build(msg_type=ack_type, seq_id=seq_id, payload=build_ack_payload(ack_type, detail, duplicate))
        self._send_frame(frame, retry=None)

    def _send_frame(self, frame: Frame, retry: int | None) -> None:
        encoded = encode_frame(frame)
        with self._write_lock:
            transport = self._transport
            if transport is None or not transport.is_open:
                raise ConnectionError("transport is not open")
            transport.write(encoded)
        if retry is None:
            LOGGER.info(
                "frame sent: seq_id=%s msg_type=%s payload=%s",
                frame.seq_id,
                frame.msg_type.name,
                self._safe_payload_text(frame.payload),
            )
            return
        LOGGER.info(
            "frame sent: seq_id=%s msg_type=%s retry=%s payload=%s",
            frame.seq_id,
            frame.msg_type.name,
            retry,
            self._safe_payload_text(frame.payload),
        )

    def _next_seq_id(self) -> int:
        with self._seq_lock:
            seq_id = self._next_pc_seq
            self._next_pc_seq = (self._next_pc_seq + 1) & 0x7FFFFFFF
            return seq_id

    def _remember_incoming_seq(self, seq_id: int) -> bool:
        with self._state_lock:
            if seq_id in self._recent_incoming_seq_set:
                return True
            if len(self._recent_incoming_seq) == self._recent_incoming_seq.maxlen:
                oldest = self._recent_incoming_seq.popleft()
                self._recent_incoming_seq_set.discard(oldest)
            self._recent_incoming_seq.append(seq_id)
            self._recent_incoming_seq_set.add(seq_id)
        return False

    def _set_device_status(self, status: DeviceStatus) -> None:
        with self._state_lock:
            if status == self._device_status:
                return
            old_status = self._device_status
            self._device_status = status
        LOGGER.info("device status changed: %s -> %s", old_status.value, status.value)
        callback = self._status_handler
        if callback is None:
            return
        try:
            callback(status)
        except Exception as exc:
            LOGGER.exception("status handler failed: %s", exc)

    @staticmethod
    def _default_ack_message(ack_type: MsgType | None) -> str:
        if ack_type == MsgType.ACK_OK:
            return "ACK 确认成功"
        if ack_type == MsgType.ACK_BUSY:
            return "设备忙，请稍后重试"
        if ack_type == MsgType.ACK_INVALID:
            return "设备返回 ACK_INVALID"
        if ack_type == MsgType.ACK_ERROR:
            return "设备返回 ACK_ERROR"
        return "未知 ACK"

    @staticmethod
    def _safe_payload_text(payload: Any) -> str:
        try:
            return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        except TypeError:
            return str(payload)
