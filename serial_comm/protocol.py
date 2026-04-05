from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

LOGGER = logging.getLogger(__name__)

HEADER = b"\x55\xAA"
EOF_MARK = b"\x0D\x0A"
VERSION = 0x01
SUPPORTED_VERSIONS = {VERSION}
# Frame layout is fixed as:
# Header(2) + Version(1) + MsgType(1) + SeqID(4) + Timestamp(4)
# + Length(2) + Payload(N) + CRC16(2) + EOF(2) = 18 + N
FRAME_OVERHEAD = 18
MAX_PAYLOAD_SIZE = 4096


class MsgType(IntEnum):
    ACK_OK = 0x00
    ACK_ERROR = 0x01
    ACK_BUSY = 0x02
    ACK_INVALID = 0x03
    CMD_REQ_CONFIRM = 0x10
    CMD_SYS_NOTIFY = 0x11
    EVT_USER_ACTION = 0x20
    EVT_HEARTBEAT = 0x21


ACK_TYPES = {
    MsgType.ACK_OK,
    MsgType.ACK_ERROR,
    MsgType.ACK_BUSY,
    MsgType.ACK_INVALID,
}

ACK_CODE_BY_TYPE: dict[MsgType, str] = {
    MsgType.ACK_OK: "OK",
    MsgType.ACK_ERROR: "ERROR",
    MsgType.ACK_BUSY: "BUSY",
    MsgType.ACK_INVALID: "INVALID",
}


class ProtocolError(Exception):
    def __init__(self, message: str, seq_id: int | None = None, msg_type: int | None = None) -> None:
        super().__init__(message)
        self.seq_id = seq_id
        self.msg_type = msg_type


class CRCError(ProtocolError):
    pass


class PayloadDecodeError(ProtocolError):
    pass


@dataclass(slots=True)
class Frame:
    msg_type: MsgType
    seq_id: int
    timestamp: int
    payload: Any = field(default_factory=dict)
    version: int = VERSION

    @classmethod
    def build(
        cls,
        msg_type: MsgType,
        seq_id: int,
        payload: Any = None,
        timestamp: int | None = None,
    ) -> "Frame":
        return cls(
            msg_type=msg_type,
            seq_id=seq_id,
            timestamp=int(time.time()) if timestamp is None else timestamp,
            payload={} if payload is None else payload,
        )


@dataclass(slots=True)
class ParserEvent:
    frame: Frame | None = None
    error: str | None = None
    seq_id: int | None = None
    msg_type: int | None = None
    detail: str | None = None


def crc16_ccitt(data: bytes, init: int = 0xFFFF) -> int:
    crc = init
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


def ack_code_for_msg_type(msg_type: MsgType) -> str:
    return ACK_CODE_BY_TYPE.get(msg_type, "ERROR")


def build_ack_payload(msg_type: MsgType, detail: str, duplicate: bool = False) -> dict[str, Any]:
    return {
        "ack_code": ack_code_for_msg_type(msg_type),
        "detail": detail,
        "duplicate": duplicate,
    }


def _payload_to_bytes(payload: Any) -> bytes:
    if payload is None:
        return b"{}"
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def encode_frame(frame: Frame) -> bytes:
    payload_bytes = _payload_to_bytes(frame.payload)
    payload_length = len(payload_bytes)
    if payload_length > MAX_PAYLOAD_SIZE:
        raise ValueError(f"payload too large: {payload_length}")

    body = bytearray()
    body.append(frame.version & 0xFF)
    body.append(int(frame.msg_type) & 0xFF)
    body.extend(int(frame.seq_id).to_bytes(4, "big", signed=False))
    body.extend(int(frame.timestamp).to_bytes(4, "big", signed=False))
    body.extend(payload_length.to_bytes(2, "big", signed=False))
    body.extend(payload_bytes)

    crc = crc16_ccitt(bytes(body))
    packet = bytearray()
    packet.extend(HEADER)
    packet.extend(body)
    packet.extend(crc.to_bytes(2, "big", signed=False))
    packet.extend(EOF_MARK)
    return bytes(packet)


def decode_frame(packet: bytes) -> Frame:
    if len(packet) < FRAME_OVERHEAD:
        raise ProtocolError("frame too short")
    if packet[:2] != HEADER:
        raise ProtocolError("invalid frame header")
    if packet[-2:] != EOF_MARK:
        raise ProtocolError("invalid frame eof")

    version = packet[2]
    msg_type_raw = packet[3]
    seq_id = int.from_bytes(packet[4:8], "big", signed=False)
    timestamp = int.from_bytes(packet[8:12], "big", signed=False)
    payload_length = int.from_bytes(packet[12:14], "big", signed=False)
    expected_length = FRAME_OVERHEAD + payload_length
    if len(packet) != expected_length:
        raise ProtocolError(
            f"frame length mismatch: expected={expected_length}, actual={len(packet)}",
            seq_id=seq_id,
            msg_type=msg_type_raw,
        )

    payload_bytes = packet[14 : 14 + payload_length]
    crc_received = int.from_bytes(packet[14 + payload_length : 16 + payload_length], "big", signed=False)
    crc_calculated = crc16_ccitt(packet[2 : 14 + payload_length])
    if crc_received != crc_calculated:
        raise CRCError(
            f"crc mismatch: expected=0x{crc_calculated:04X}, actual=0x{crc_received:04X}",
            seq_id=seq_id,
            msg_type=msg_type_raw,
        )

    if version not in SUPPORTED_VERSIONS:
        raise ProtocolError(
            f"unsupported version: 0x{version:02X}",
            seq_id=seq_id,
            msg_type=msg_type_raw,
        )

    try:
        msg_type = MsgType(msg_type_raw)
    except ValueError as exc:
        raise ProtocolError(f"unsupported msg type: 0x{msg_type_raw:02X}", seq_id=seq_id, msg_type=msg_type_raw) from exc

    if payload_length == 0:
        payload: Any = {}
    else:
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PayloadDecodeError("payload is not valid UTF-8 JSON", seq_id=seq_id, msg_type=msg_type_raw) from exc

    return Frame(
        msg_type=msg_type,
        seq_id=seq_id,
        timestamp=timestamp,
        payload=payload,
        version=version,
    )


class FrameParser:
    """Length-driven stream parser supporting half packets and sticky packets."""

    def __init__(self, max_payload_size: int = MAX_PAYLOAD_SIZE) -> None:
        self._buffer = bytearray()
        self._max_payload_size = max_payload_size

    def feed(self, data: bytes) -> list[ParserEvent]:
        events: list[ParserEvent] = []
        if not data:
            return events

        self._buffer.extend(data)
        while True:
            header_index = self._buffer.find(HEADER)
            if header_index < 0:
                self._trim_orphaned_bytes()
                break
            if header_index > 0:
                del self._buffer[:header_index]

            if len(self._buffer) < FRAME_OVERHEAD:
                break

            payload_length = int.from_bytes(self._buffer[12:14], "big", signed=False)
            if payload_length > self._max_payload_size:
                LOGGER.warning("drop over-sized frame candidate: payload_length=%s", payload_length)
                events.append(ParserEvent(error="frame_too_large", detail=f"payload_length={payload_length}"))
                del self._buffer[0]
                continue

            frame_length = FRAME_OVERHEAD + payload_length
            if len(self._buffer) < frame_length:
                break

            candidate = bytes(self._buffer[:frame_length])
            try:
                frame = decode_frame(candidate)
            except CRCError as exc:
                events.append(ParserEvent(error="crc_error", seq_id=exc.seq_id, msg_type=exc.msg_type, detail=str(exc)))
                del self._buffer[:frame_length]
                continue
            except PayloadDecodeError as exc:
                events.append(
                    ParserEvent(error="payload_decode_error", seq_id=exc.seq_id, msg_type=exc.msg_type, detail=str(exc))
                )
                del self._buffer[:frame_length]
                continue
            except ProtocolError as exc:
                events.append(ParserEvent(error="protocol_error", seq_id=exc.seq_id, msg_type=exc.msg_type, detail=str(exc)))
                del self._buffer[0]
                continue

            events.append(ParserEvent(frame=frame))
            del self._buffer[:frame_length]

        return events

    def _trim_orphaned_bytes(self) -> None:
        if not self._buffer:
            return
        if self._buffer[-1:] == HEADER[:1]:
            self._buffer[:] = HEADER[:1]
            return
        self._buffer.clear()
