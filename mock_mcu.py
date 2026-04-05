from __future__ import annotations

import argparse
import json
import logging
import socket
import threading
import time
from collections import deque
from typing import Any

from protocol import ACK_TYPES, Frame, FrameParser, MsgType, ParserEvent, build_ack_payload, encode_frame

LOGGER = logging.getLogger(__name__)


class MockMCUServer:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9000,
        mode: str = "confirmed",
        confirm_delay: float = 2.0,
        heartbeat_interval: float = 5.0,
    ) -> None:
        self.host = host
        self.port = port
        self.mode = mode.lower()
        self.confirm_delay = confirm_delay
        self.heartbeat_interval = heartbeat_interval

        self._server_socket: socket.socket | None = None
        self._client_socket: socket.socket | None = None
        self._client_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._stop_event = threading.Event()

        self._reader_thread: threading.Thread | None = None
        self._accept_thread: threading.Thread | None = None
        self._heartbeat_thread: threading.Thread | None = None

        self._seq = 0x80000000
        self._recent_pc_seq: deque[int] = deque(maxlen=5)
        self._recent_pc_seq_set: set[int] = set()

    def start(self) -> None:
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(1)
        self._server_socket.settimeout(0.5)
        self._stop_event.clear()

        self._accept_thread = threading.Thread(target=self._accept_loop, name="mock-mcu-accept", daemon=True)
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, name="mock-mcu-heartbeat", daemon=True)
        self._accept_thread.start()
        self._heartbeat_thread.start()
        LOGGER.info("mock mcu started: %s:%s mode=%s", self.host, self.port, self.mode)

    def stop(self) -> None:
        self._stop_event.set()
        with self._client_lock:
            client = self._client_socket
            self._client_socket = None
        if client is not None:
            try:
                client.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            client.close()

        server = self._server_socket
        self._server_socket = None
        if server is not None:
            server.close()

        for thread in (self._reader_thread, self._accept_thread, self._heartbeat_thread):
            if thread is not None:
                thread.join(timeout=1.0)
        LOGGER.info("mock mcu stopped")

    def _accept_loop(self) -> None:
        assert self._server_socket is not None
        while not self._stop_event.is_set():
            try:
                client, address = self._server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                return

            client.settimeout(0.2)
            with self._client_lock:
                if self._client_socket is not None:
                    try:
                        self._client_socket.close()
                    except OSError:
                        pass
                self._client_socket = client
            LOGGER.info("mock mcu client connected: %s", address)
            self._reader_thread = threading.Thread(target=self._reader_loop, args=(client,), name="mock-mcu-reader", daemon=True)
            self._reader_thread.start()

    def _reader_loop(self, client: socket.socket) -> None:
        parser = FrameParser()
        while not self._stop_event.is_set():
            try:
                chunk = client.recv(1024)
            except socket.timeout:
                continue
            except OSError:
                break
            if not chunk:
                break

            for event in parser.feed(chunk):
                self._handle_parser_event(event)

        with self._client_lock:
            if self._client_socket is client:
                self._client_socket = None
        LOGGER.info("mock mcu client disconnected")

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            time.sleep(self.heartbeat_interval)
            if self._stop_event.is_set():
                return
            with self._client_lock:
                if self._client_socket is None:
                    continue
            self._send_frame(
                Frame.build(
                    msg_type=MsgType.EVT_HEARTBEAT,
                    seq_id=self._next_seq(),
                    payload={"hw_sn": "STM32F103-A23", "status": "OK"},
                )
            )

    def _handle_parser_event(self, event: ParserEvent) -> None:
        if event.frame is not None:
            self._handle_frame(event.frame)
            return
        LOGGER.warning("mock parser error: %s seq_id=%s detail=%s", event.error, event.seq_id, event.detail)
        if event.seq_id is not None:
            self._send_frame(
                Frame.build(
                    MsgType.ACK_ERROR,
                    event.seq_id,
                    build_ack_payload(MsgType.ACK_ERROR, event.detail or "PARSER_ERROR"),
                )
            )

    def _handle_frame(self, frame: Frame) -> None:
        LOGGER.info(
            "mock received: seq_id=%s msg_type=%s payload=%s",
            frame.seq_id,
            frame.msg_type.name,
            json.dumps(frame.payload, ensure_ascii=False, separators=(",", ":")),
        )

        if frame.msg_type in ACK_TYPES:
            return

        if frame.msg_type == MsgType.CMD_REQ_CONFIRM and self.mode == "no_ack":
            LOGGER.info("mock mode no_ack: seq_id=%s intentionally not replying", frame.seq_id)
            return

        duplicate = self._remember_pc_seq(frame.seq_id)
        if duplicate:
            self._send_frame(
                Frame.build(
                    MsgType.ACK_OK,
                    frame.seq_id,
                    build_ack_payload(MsgType.ACK_OK, "DUPLICATE_FRAME_IGNORED", duplicate=True),
                )
            )
            return

        if frame.msg_type == MsgType.CMD_SYS_NOTIFY:
            self._send_frame(Frame.build(MsgType.ACK_OK, frame.seq_id, build_ack_payload(MsgType.ACK_OK, "FRAME_RECEIVED")))
            return

        if frame.msg_type != MsgType.CMD_REQ_CONFIRM:
            self._send_frame(
                Frame.build(MsgType.ACK_INVALID, frame.seq_id, build_ack_payload(MsgType.ACK_INVALID, "UNSUPPORTED_MSG"))
            )
            return

        if self.mode == "busy":
            self._send_frame(Frame.build(MsgType.ACK_BUSY, frame.seq_id, build_ack_payload(MsgType.ACK_BUSY, "DEVICE_BUSY")))
            return
        if self.mode == "ack_error":
            self._send_frame(Frame.build(MsgType.ACK_ERROR, frame.seq_id, build_ack_payload(MsgType.ACK_ERROR, "CRC_CHECK_FAIL")))
            return
        if self.mode == "invalid":
            self._send_frame(
                Frame.build(MsgType.ACK_INVALID, frame.seq_id, build_ack_payload(MsgType.ACK_INVALID, "INVALID_REQUEST"))
            )
            return

        self._send_frame(Frame.build(MsgType.ACK_OK, frame.seq_id, build_ack_payload(MsgType.ACK_OK, "FRAME_RECEIVED")))
        threading.Thread(target=self._emit_user_action, args=(frame,), name="mock-mcu-action", daemon=True).start()

    def _emit_user_action(self, request_frame: Frame) -> None:
        payload = request_frame.payload if isinstance(request_frame.payload, dict) else {}
        wait_timeout_ms = int(payload.get("wait_timeout", 30000))
        asset_id = str(payload.get("asset_id", "UNKNOWN-ASSET"))
        request_seq = int(payload.get("request_seq", request_frame.seq_id))
        request_id = str(payload.get("request_id", "")).strip() or None
        action_type = str(payload.get("action_type", "BORROW")).strip() or "BORROW"

        delay = self.confirm_delay
        confirm_result = "CONFIRMED"
        response_action = action_type
        response_request_seq = request_seq

        if self.mode == "timeout":
            confirm_result = "TIMEOUT"
        elif self.mode == "cancelled":
            confirm_result = "CANCELLED"
        elif self.mode == "late_confirm":
            delay = wait_timeout_ms / 1000.0 + 6.0
        elif self.mode == "mismatch_action":
            response_action = "RETURN" if action_type == "BORROW" else "BORROW"
        elif self.mode == "mismatch_request_seq":
            response_request_seq = request_seq + 1
        elif self.mode == "mismatch_request_id":
            # Used to verify service-side request_id mismatch filtering.
            request_id = "bad-request-id"

        time.sleep(delay)
        with self._client_lock:
            if self._stop_event.is_set() or self._client_socket is None:
                return

        response_payload: dict[str, Any] = {
            "asset_id": asset_id,
            "request_seq": response_request_seq,
            "request_id": request_id,
            "action_type": response_action,
            "confirm_result": confirm_result,
            "hw_sn": "STM32F103-A23",
        }
        self._send_frame(Frame.build(MsgType.EVT_USER_ACTION, self._next_seq(), response_payload))

        if self.mode == "duplicate_confirm":
            time.sleep(0.2)
            self._send_frame(Frame.build(MsgType.EVT_USER_ACTION, self._next_seq(), response_payload))

    def _send_frame(self, frame: Frame) -> None:
        packet = encode_frame(frame)
        with self._write_lock:
            with self._client_lock:
                client = self._client_socket
            if client is None:
                return
            try:
                client.sendall(packet)
            except OSError as exc:
                LOGGER.warning("mock send failed: %s", exc)
                return
        LOGGER.info(
            "mock sent: seq_id=%s msg_type=%s payload=%s",
            frame.seq_id,
            frame.msg_type.name,
            json.dumps(frame.payload, ensure_ascii=False, separators=(",", ":")),
        )

    def _next_seq(self) -> int:
        seq = self._seq
        self._seq = 0x80000000 if self._seq == 0xFFFFFFFF else self._seq + 1
        return seq

    def _remember_pc_seq(self, seq_id: int) -> bool:
        if seq_id in self._recent_pc_seq_set:
            return True
        if len(self._recent_pc_seq) == self._recent_pc_seq.maxlen:
            oldest = self._recent_pc_seq.popleft()
            self._recent_pc_seq_set.discard(oldest)
        self._recent_pc_seq.append(seq_id)
        self._recent_pc_seq_set.add(seq_id)
        return False


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mock MCU for serial protocol integration test.")
    parser.add_argument("--host", default="127.0.0.1", help="server bind host")
    parser.add_argument("--port", type=int, default=9000, help="server bind port")
    parser.add_argument(
        "--mode",
        choices=[
            "confirmed",
            "cancelled",
            "timeout",
            "late_confirm",
            "duplicate_confirm",
            "mismatch_action",
            "mismatch_request_seq",
            "mismatch_request_id",
            "busy",
            "ack_error",
            "invalid",
            "no_ack",
        ],
        default="confirmed",
        help="mock response mode",
    )
    parser.add_argument("--delay", type=float, default=2.0, help="delay seconds before EVT_USER_ACTION")
    return parser


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")


def main() -> None:
    configure_logging()
    args = build_arg_parser().parse_args()
    server = MockMCUServer(host=args.host, port=args.port, mode=args.mode, confirm_delay=args.delay)
    server.start()
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        LOGGER.info("mock mcu interrupted by user")
    finally:
        server.stop()


if __name__ == "__main__":
    main()
