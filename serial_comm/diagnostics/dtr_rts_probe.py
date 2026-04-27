from __future__ import annotations

import argparse
import itertools
import sys
import time
from datetime import datetime
from typing import Any

try:
    import serial  # type: ignore
except ImportError:  # pragma: no cover - diagnostic script path
    serial = None


STOPBITS_BY_TEXT = {
    "1": 1,
    "1.5": 1.5,
    "2": 2,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe DTR/RTS combinations and report whether any raw RX byte appears.",
    )
    parser.add_argument("--port", required=True, help="Serial port, for example COM7.")
    parser.add_argument("--baudrate", type=int, default=115200)
    parser.add_argument("--bytesize", type=int, choices=(5, 6, 7, 8), default=8)
    parser.add_argument("--parity", choices=("N", "E", "O", "M", "S"), default="N")
    parser.add_argument("--stopbits", choices=tuple(STOPBITS_BY_TEXT), default="1")
    parser.add_argument("--seconds", type=float, default=8.0)
    parser.add_argument("--timeout", type=float, default=0.2)
    parser.add_argument("--read-size", type=int, default=1024)
    parser.add_argument("--reset-input", action="store_true", help="Clear input buffer after opening each combination.")
    parser.add_argument("--reset-output", action="store_true", help="Clear output buffer after opening each combination.")
    return parser.parse_args()


def timestamp() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


def open_port(args: argparse.Namespace, dtr: bool, rts: bool) -> Any:
    port = serial.serial_for_url(
        args.port,
        baudrate=args.baudrate,
        bytesize=args.bytesize,
        parity=args.parity,
        stopbits=STOPBITS_BY_TEXT[args.stopbits],
        timeout=args.timeout,
        write_timeout=args.timeout,
        xonxoff=False,
        rtscts=False,
        dsrdtr=False,
        do_not_open=True,
    )
    port.dtr = dtr
    port.rts = rts
    port.open()
    port.dtr = dtr
    port.rts = rts
    if args.reset_input:
        port.reset_input_buffer()
    if args.reset_output:
        port.reset_output_buffer()
    return port


def probe_one(args: argparse.Namespace, dtr: bool, rts: bool) -> dict[str, Any]:
    result: dict[str, Any] = {
        "dtr": dtr,
        "rts": rts,
        "opened": False,
        "has_rx": False,
        "total_bytes": 0,
        "read_calls": 0,
        "empty_reads": 0,
        "first_rx_hex": None,
        "initial_in_waiting": None,
        "final_in_waiting": None,
        "error": None,
    }
    port = None
    try:
        port = open_port(args, dtr, rts)
        result["opened"] = True
        result["initial_in_waiting"] = port.in_waiting
        start = time.monotonic()
        while time.monotonic() - start < args.seconds:
            data = port.read(args.read_size)
            result["read_calls"] += 1
            if data:
                result["has_rx"] = True
                result["total_bytes"] += len(data)
                if result["first_rx_hex"] is None:
                    result["first_rx_hex"] = data[:64].hex(" ")
            else:
                result["empty_reads"] += 1
        result["final_in_waiting"] = port.in_waiting
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if port is not None and getattr(port, "is_open", False):
            port.close()
    return result


def format_result(result: dict[str, Any]) -> str:
    return (
        f"dtr={result['dtr']} rts={result['rts']} opened={result['opened']} "
        f"has_rx={result['has_rx']} total={result['total_bytes']} reads={result['read_calls']} "
        f"empty={result['empty_reads']} initial_in_waiting={result['initial_in_waiting']} "
        f"final_in_waiting={result['final_in_waiting']} first_rx={result['first_rx_hex']} "
        f"error={result['error']}"
    )


def main() -> int:
    if serial is None:
        print("pyserial is required. Install it with: pip install pyserial", file=sys.stderr)
        return 2

    args = parse_args()
    results: list[dict[str, Any]] = []
    print(
        f"{timestamp()} start port={args.port} baudrate={args.baudrate} seconds_per_combo={args.seconds}",
        flush=True,
    )
    for dtr, rts in itertools.product((False, True), (False, True)):
        result = probe_one(args, dtr, rts)
        results.append(result)
        print(f"{timestamp()} result {format_result(result)}", flush=True)

    rx_results = [result for result in results if result["has_rx"]]
    no_rx_results = [result for result in results if not result["has_rx"] and result["error"] is None]
    error_results = [result for result in results if result["error"] is not None]
    print(f"{timestamp()} summary rx={len(rx_results)} no_rx={len(no_rx_results)} errors={len(error_results)}", flush=True)
    if rx_results:
        print("RX line states:", flush=True)
        for result in rx_results:
            print(f"  {format_result(result)}", flush=True)
    if no_rx_results:
        print("No-RX line states:", flush=True)
        for result in no_rx_results:
            print(f"  {format_result(result)}", flush=True)
    if error_results:
        print("Error line states:", flush=True)
        for result in error_results:
            print(f"  {format_result(result)}", flush=True)
    return 0 if rx_results else 1


if __name__ == "__main__":
    raise SystemExit(main())
