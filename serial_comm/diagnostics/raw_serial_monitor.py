from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from typing import Any

try:
    import serial  # type: ignore
except ImportError:  # pragma: no cover - diagnostic script path
    serial = None


PARITY_CHOICES = ("N", "E", "O", "M", "S")
STOPBITS_BY_TEXT = {
    "1": 1,
    "1.5": 1.5,
    "2": 2,
}


def parse_bool_line(raw: str | None) -> bool | None:
    if raw is None:
        return None
    lowered = raw.strip().lower()
    if lowered in {"1", "true", "on", "high"}:
        return True
    if lowered in {"0", "false", "off", "low"}:
        return False
    raise argparse.ArgumentTypeError("expected one of: high/low/on/off/true/false/1/0")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Raw pyserial monitor. Prints RX bytes and one status line per second; no protocol parsing.",
    )
    parser.add_argument("--port", required=True, help="Serial port, for example COM7.")
    parser.add_argument("--baudrate", type=int, default=115200)
    parser.add_argument("--bytesize", type=int, choices=(5, 6, 7, 8), default=8)
    parser.add_argument("--parity", choices=PARITY_CHOICES, default="N")
    parser.add_argument("--stopbits", choices=tuple(STOPBITS_BY_TEXT), default="1")
    parser.add_argument("--timeout", type=float, default=0.2)
    parser.add_argument("--read-size", type=int, default=1024)
    parser.add_argument("--duration", type=float, default=0.0, help="Seconds to run. 0 means run until Ctrl+C.")
    parser.add_argument("--dtr", type=parse_bool_line, default=None, help="Optional DTR state: high or low.")
    parser.add_argument("--rts", type=parse_bool_line, default=None, help="Optional RTS state: high or low.")
    parser.add_argument("--reset-input", action="store_true", help="Clear input buffer after opening.")
    parser.add_argument("--reset-output", action="store_true", help="Clear output buffer after opening.")
    return parser.parse_args()


def serial_snapshot(port: Any) -> dict[str, Any]:
    names = (
        "is_open",
        "in_waiting",
        "out_waiting",
        "dtr",
        "rts",
        "cts",
        "dsr",
        "cd",
        "ri",
        "timeout",
        "write_timeout",
        "inter_byte_timeout",
        "xonxoff",
        "rtscts",
        "dsrdtr",
    )
    snapshot: dict[str, Any] = {}
    for name in names:
        try:
            snapshot[name] = getattr(port, name)
        except Exception as exc:
            snapshot[name] = f"{type(exc).__name__}: {exc}"
    return snapshot


def timestamp() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


def main() -> int:
    if serial is None:
        print("pyserial is required. Install it with: pip install pyserial", file=sys.stderr)
        return 2

    args = parse_args()
    stopbits = STOPBITS_BY_TEXT[args.stopbits]
    started_at = time.monotonic()
    last_rx_at: float | None = None
    total_bytes = 0
    current_second = int(started_at)
    second_read_calls = 0
    second_empty_reads = 0

    try:
        port = serial.serial_for_url(
            args.port,
            baudrate=args.baudrate,
            bytesize=args.bytesize,
            parity=args.parity,
            stopbits=stopbits,
            timeout=args.timeout,
            xonxoff=False,
            rtscts=False,
            dsrdtr=False,
            do_not_open=True,
        )
        if args.dtr is not None:
            port.dtr = args.dtr
        if args.rts is not None:
            port.rts = args.rts
        port.open()
        if args.dtr is not None:
            port.dtr = args.dtr
        if args.rts is not None:
            port.rts = args.rts
        if args.reset_input:
            port.reset_input_buffer()
        if args.reset_output:
            port.reset_output_buffer()
    except Exception as exc:
        print(f"{timestamp()} open_failed type={type(exc).__name__} detail={exc}", file=sys.stderr)
        return 1

    print(f"{timestamp()} opened port={args.port} snapshot={serial_snapshot(port)}", flush=True)
    try:
        while True:
            now = time.monotonic()
            if args.duration > 0 and now - started_at >= args.duration:
                break

            try:
                data = port.read(args.read_size)
            except Exception as exc:
                print(f"{timestamp()} read_failed type={type(exc).__name__} detail={exc}", flush=True)
                continue

            second_read_calls += 1
            if data:
                total_bytes += len(data)
                last_rx_at = time.monotonic()
                print(f"{timestamp()} RX bytes={len(data)} hex={data.hex(' ')}", flush=True)
            else:
                second_empty_reads += 1

            second = int(time.monotonic())
            if second != current_second:
                last_rx_text = "never" if last_rx_at is None else f"{time.monotonic() - last_rx_at:.3f}s_ago"
                print(
                    f"{timestamp()} status is_open={port.is_open} in_waiting={port.in_waiting} "
                    f"read_calls={second_read_calls} empty_reads={second_empty_reads} "
                    f"total_bytes={total_bytes} last_rx={last_rx_text}",
                    flush=True,
                )
                current_second = second
                second_read_calls = 0
                second_empty_reads = 0
    except KeyboardInterrupt:
        print(f"{timestamp()} interrupted", flush=True)
    finally:
        if port.is_open:
            port.close()
        print(f"{timestamp()} closed total_bytes={total_bytes}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
