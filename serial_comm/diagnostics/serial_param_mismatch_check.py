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


def timestamp() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


def csv_ints(raw: str) -> list[int]:
    values = [int(part.strip()) for part in raw.split(",") if part.strip()]
    if not values:
        raise argparse.ArgumentTypeError("expected at least one integer")
    return values


def csv_choices(raw: str, allowed: set[str]) -> list[str]:
    values = [part.strip().upper() for part in raw.split(",") if part.strip()]
    if not values:
        raise argparse.ArgumentTypeError("expected at least one value")
    unsupported = [value for value in values if value not in allowed]
    if unsupported:
        raise argparse.ArgumentTypeError(f"unsupported value(s): {','.join(unsupported)}")
    return values


def line_state(raw: str) -> bool | None:
    value = raw.strip().lower()
    if value in {"unchanged", "none"}:
        return None
    if value in {"high", "on", "true", "1"}:
        return True
    if value in {"low", "off", "false", "0"}:
        return False
    raise argparse.ArgumentTypeError("expected high, low, or unchanged")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Check whether COM RX=0 is explained by baudrate/parity/stopbits mismatch. "
            "This is a raw byte scanner and does not parse project protocol frames."
        ),
    )
    parser.add_argument("--port", required=True, help="Serial port, for example COM7.")
    parser.add_argument("--baudrates", type=csv_ints, default=csv_ints("9600,19200,38400,57600,115200,230400"))
    parser.add_argument(
        "--parities",
        type=lambda raw: csv_choices(raw, {"N", "E", "O", "M", "S"}),
        default=csv_choices("N,E,O", {"N", "E", "O", "M", "S"}),
    )
    parser.add_argument(
        "--stopbits",
        type=lambda raw: csv_choices(raw, set(STOPBITS_BY_TEXT)),
        default=csv_choices("1,2", set(STOPBITS_BY_TEXT)),
    )
    parser.add_argument("--bytesize", type=int, choices=(5, 6, 7, 8), default=8)
    parser.add_argument("--seconds", type=float, default=6.0, help="Seconds to listen for each parameter group.")
    parser.add_argument("--timeout", type=float, default=0.2)
    parser.add_argument("--read-size", type=int, default=1024)
    parser.add_argument("--expected-baudrate", type=int, default=115200)
    parser.add_argument("--expected-parity", choices=("N", "E", "O", "M", "S"), default="N")
    parser.add_argument("--expected-stopbits", choices=tuple(STOPBITS_BY_TEXT), default="1")
    parser.add_argument("--dtr", type=line_state, default=None, help="Optional fixed DTR state: high, low, unchanged.")
    parser.add_argument("--rts", type=line_state, default=None, help="Optional fixed RTS state: high, low, unchanged.")
    return parser.parse_args()


def open_port(args: argparse.Namespace, baudrate: int, parity: str, stopbits_text: str) -> Any:
    port = serial.serial_for_url(
        args.port,
        baudrate=baudrate,
        bytesize=args.bytesize,
        parity=parity,
        stopbits=STOPBITS_BY_TEXT[stopbits_text],
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
    return port


def probe_one(args: argparse.Namespace, baudrate: int, parity: str, stopbits_text: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "baudrate": baudrate,
        "bytesize": args.bytesize,
        "parity": parity,
        "stopbits": stopbits_text,
        "opened": False,
        "has_rx": False,
        "total_bytes": 0,
        "read_calls": 0,
        "empty_reads": 0,
        "first_rx_at_seconds": None,
        "first_rx_hex": None,
        "initial_in_waiting": None,
        "final_in_waiting": None,
        "error": None,
    }
    port = None
    try:
        port = open_port(args, baudrate, parity, stopbits_text)
        result["opened"] = True
        result["initial_in_waiting"] = port.in_waiting
        started = time.monotonic()
        while time.monotonic() - started < args.seconds:
            data = port.read(args.read_size)
            result["read_calls"] += 1
            if data:
                if not result["has_rx"]:
                    result["first_rx_at_seconds"] = round(time.monotonic() - started, 3)
                    result["first_rx_hex"] = data[:64].hex(" ")
                result["has_rx"] = True
                result["total_bytes"] += len(data)
            else:
                result["empty_reads"] += 1
        result["final_in_waiting"] = port.in_waiting
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if port is not None and getattr(port, "is_open", False):
            port.close()
        time.sleep(0.2)
    return result


def is_expected(args: argparse.Namespace, result: dict[str, Any]) -> bool:
    return (
        result["baudrate"] == args.expected_baudrate
        and result["parity"] == args.expected_parity
        and result["stopbits"] == args.expected_stopbits
    )


def result_line(result: dict[str, Any]) -> str:
    return (
        f"baud={result['baudrate']} bytesize={result['bytesize']} parity={result['parity']} "
        f"stopbits={result['stopbits']} opened={result['opened']} has_rx={result['has_rx']} "
        f"total={result['total_bytes']} reads={result['read_calls']} empty={result['empty_reads']} "
        f"initial_in_waiting={result['initial_in_waiting']} final_in_waiting={result['final_in_waiting']} "
        f"first_rx_at={result['first_rx_at_seconds']} first_rx={result['first_rx_hex']} error={result['error']}"
    )


def judge(args: argparse.Namespace, results: list[dict[str, Any]]) -> tuple[str, str]:
    opened = [result for result in results if result["opened"]]
    rx = [result for result in results if result["has_rx"]]
    expected = [result for result in results if is_expected(args, result)]
    expected_has_rx = any(result["has_rx"] for result in expected)
    non_expected_has_rx = any(result["has_rx"] for result in rx if not is_expected(args, result))

    if not opened:
        return "INCONCLUSIVE", "No parameter group could open the serial port."
    if expected_has_rx:
        return "NO", "Expected parameters already receive RX bytes."
    if non_expected_has_rx:
        return "YES", "At least one non-expected parameter group received RX bytes while expected parameters did not."
    return (
        "NO",
        "No scanned baudrate/parity/stopbits group received any RX byte; this does not support a parameter mismatch cause.",
    )


def main() -> int:
    if serial is None:
        print("pyserial is required. Install it with: pip install pyserial", file=sys.stderr)
        return 2

    args = parse_args()
    combinations = list(itertools.product(args.baudrates, args.parities, args.stopbits))
    results: list[dict[str, Any]] = []
    print(
        f"{timestamp()} START port={args.port} combinations={len(combinations)} "
        f"seconds_per_group={args.seconds} expected={args.expected_baudrate}/{args.bytesize}"
        f"{args.expected_parity}{args.expected_stopbits} dtr={args.dtr} rts={args.rts}",
        flush=True,
    )
    for baudrate, parity, stopbits_text in combinations:
        result = probe_one(args, baudrate, parity, stopbits_text)
        results.append(result)
        marker = "EXPECTED" if is_expected(args, result) else "CANDIDATE"
        print(f"{timestamp()} {marker} {result_line(result)}", flush=True)

    rx_results = [result for result in results if result["has_rx"]]
    no_rx_results = [result for result in results if result["opened"] and not result["has_rx"]]
    error_results = [result for result in results if result["error"] is not None]
    final_judgment, reason = judge(args, results)

    print(
        f"{timestamp()} SUMMARY rx_groups={len(rx_results)} no_rx_groups={len(no_rx_results)} "
        f"error_groups={len(error_results)}",
        flush=True,
    )
    if rx_results:
        print("RX_GROUPS:", flush=True)
        for result in rx_results:
            print(f"  {result_line(result)}", flush=True)
    if error_results:
        print("ERROR_GROUPS:", flush=True)
        for result in error_results:
            print(f"  {result_line(result)}", flush=True)
    print(f"{timestamp()} FINAL_JUDGMENT={final_judgment} reason={reason}", flush=True)
    return 0 if final_judgment in {"YES", "NO"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
