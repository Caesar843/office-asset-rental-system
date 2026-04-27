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


def csv_ints(raw: str) -> list[int]:
    values: list[int] = []
    for part in raw.split(","):
        text = part.strip()
        if text:
            values.append(int(text))
    if not values:
        raise argparse.ArgumentTypeError("expected at least one integer")
    return values


def csv_text(raw: str, allowed: set[str]) -> list[str]:
    values: list[str] = []
    for part in raw.split(","):
        text = part.strip().upper()
        if not text:
            continue
        if text not in allowed:
            raise argparse.ArgumentTypeError(f"unsupported value: {text}")
        values.append(text)
    if not values:
        raise argparse.ArgumentTypeError("expected at least one value")
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe serial parameter combinations and report whether any raw RX byte appears.",
    )
    parser.add_argument("--port", required=True, help="Serial port, for example COM7.")
    parser.add_argument("--baudrates", type=csv_ints, default=csv_ints("9600,19200,38400,57600,115200"))
    parser.add_argument("--bytesizes", type=csv_ints, default=csv_ints("8"))
    parser.add_argument("--parities", type=lambda raw: csv_text(raw, {"N", "E", "O", "M", "S"}), default=csv_text("N,E,O", {"N", "E", "O", "M", "S"}))
    parser.add_argument("--stopbits", type=lambda raw: csv_text(raw, set(STOPBITS_BY_TEXT)), default=csv_text("1,2", set(STOPBITS_BY_TEXT)))
    parser.add_argument("--seconds", type=float, default=5.0)
    parser.add_argument("--timeout", type=float, default=0.2)
    parser.add_argument("--read-size", type=int, default=1024)
    return parser.parse_args()


def timestamp() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


def open_port(args: argparse.Namespace, baudrate: int, bytesize: int, parity: str, stopbits_text: str) -> Any:
    return serial.serial_for_url(
        args.port,
        baudrate=baudrate,
        bytesize=bytesize,
        parity=parity,
        stopbits=STOPBITS_BY_TEXT[stopbits_text],
        timeout=args.timeout,
        xonxoff=False,
        rtscts=False,
        dsrdtr=False,
    )


def probe_one(args: argparse.Namespace, baudrate: int, bytesize: int, parity: str, stopbits_text: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "baudrate": baudrate,
        "bytesize": bytesize,
        "parity": parity,
        "stopbits": stopbits_text,
        "opened": False,
        "has_rx": False,
        "total_bytes": 0,
        "read_calls": 0,
        "empty_reads": 0,
        "first_rx_hex": None,
        "error": None,
    }
    port = None
    try:
        port = open_port(args, baudrate, bytesize, parity, stopbits_text)
        result["opened"] = True
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
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if port is not None and getattr(port, "is_open", False):
            port.close()
    return result


def format_result(result: dict[str, Any]) -> str:
    return (
        f"baud={result['baudrate']} bytesize={result['bytesize']} parity={result['parity']} "
        f"stopbits={result['stopbits']} opened={result['opened']} has_rx={result['has_rx']} "
        f"total={result['total_bytes']} reads={result['read_calls']} empty={result['empty_reads']} "
        f"first_rx={result['first_rx_hex']} error={result['error']}"
    )


def main() -> int:
    if serial is None:
        print("pyserial is required. Install it with: pip install pyserial", file=sys.stderr)
        return 2

    args = parse_args()
    combinations = list(itertools.product(args.baudrates, args.bytesizes, args.parities, args.stopbits))
    results: list[dict[str, Any]] = []
    print(f"{timestamp()} start port={args.port} combinations={len(combinations)} seconds_per_combo={args.seconds}", flush=True)
    for baudrate, bytesize, parity, stopbits_text in combinations:
        result = probe_one(args, baudrate, bytesize, parity, stopbits_text)
        results.append(result)
        print(f"{timestamp()} result {format_result(result)}", flush=True)

    rx_results = [result for result in results if result["has_rx"]]
    no_rx_results = [result for result in results if not result["has_rx"] and result["error"] is None]
    error_results = [result for result in results if result["error"] is not None]
    print(f"{timestamp()} summary rx={len(rx_results)} no_rx={len(no_rx_results)} errors={len(error_results)}", flush=True)
    if rx_results:
        print("RX combinations:", flush=True)
        for result in rx_results:
            print(f"  {format_result(result)}", flush=True)
    if no_rx_results:
        print("No-RX combinations:", flush=True)
        for result in no_rx_results:
            print(f"  {format_result(result)}", flush=True)
    if error_results:
        print("Error combinations:", flush=True)
        for result in error_results:
            print(f"  {format_result(result)}", flush=True)
    return 0 if rx_results else 1


if __name__ == "__main__":
    raise SystemExit(main())
