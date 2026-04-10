from __future__ import annotations

import argparse
import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOGGER = logging.getLogger(__name__)


class ScanResultMockHandler(BaseHTTPRequestHandler):
    server_version = "VisionMockScanServer/0.2"

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/health":
            self.send_error(404, "Not Found")
            return
        self._write_json(
            200,
            {
                "success": True,
                "code": "MOCK_SERVER_HEALTHY",
                "message": "vision mock scan server is healthy",
                "asset_id": "MOCK_SERVER",
                "extra": {"path": self.path},
            },
        )
        LOGGER.info("mock_server health_check ok path=%s", self.path)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/scan/result":
            self.send_error(404, "Not Found")
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length)
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write_json(
                400,
                {
                    "success": False,
                    "code": "INVALID_JSON",
                    "message": "request body is not valid JSON",
                    "asset_id": None,
                    "extra": {},
                },
            )
            LOGGER.warning("mock_server invalid_json remote=%s", self.client_address[0])
            return

        asset_id = str(payload.get("asset_id", "")).strip()
        if not asset_id:
            self._write_json(
                422,
                {
                    "success": False,
                    "code": "INVALID_REQUEST",
                    "message": "asset_id is required",
                    "asset_id": None,
                    "extra": {},
                },
            )
            LOGGER.warning("mock_server invalid_request source_id=%s", payload.get("source_id"))
            return

        LOGGER.info(
            "mock_server accept asset_id=%s source_id=%s symbology=%s frame_time=%s",
            asset_id,
            payload.get("source_id"),
            payload.get("symbology"),
            payload.get("frame_time"),
        )
        self._write_json(
            200,
            {
                "success": True,
                "code": "SCAN_ACCEPTED",
                "message": "scan result accepted",
                "asset_id": asset_id,
                "extra": {
                    "server": "vision-local-mock",
                    "source_id": payload.get("source_id"),
                    "symbology": payload.get("symbology"),
                },
            },
        )

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        LOGGER.debug("mock_server http_log " + format, *args)

    def _write_json(self, status_code: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def build_mock_server(host: str, port: int) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), ScanResultMockHandler)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Local formal-contract scan-result mock server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s %(name)s %(message)s",
    )
    server = build_mock_server(args.host, args.port)
    print(f"vision mock scan server listening on http://{args.host}:{args.port}")
    print(f"health check: http://{args.host}:{args.port}/health")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("mock_server interrupted")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
