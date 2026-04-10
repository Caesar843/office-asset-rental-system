from __future__ import annotations

import json
import socket
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from app.config import GatewayConfig
from models.error_result import VisionErrorResult
from models.scan_result import ScanResult, ScanSubmitRequest
from models._validation import ensure_non_empty_text


Transport = Callable[[str, Mapping[str, Any], float, Mapping[str, str]], "TransportResponse"]


@dataclass(frozen=True, slots=True)
class TransportResponse:
    status_code: int
    body: bytes
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SubmitResult:
    status: str
    http_status: int | None
    transport_success: bool
    http_ok: bool
    response_valid: bool
    business_success: bool
    code: str
    message: str
    request_payload: dict[str, Any]
    response_payload: dict[str, Any] = field(default_factory=dict)
    error: VisionErrorResult | None = None


def _default_transport(
    url: str,
    payload: Mapping[str, Any],
    timeout_sec: float,
    headers: Mapping[str, str],
) -> TransportResponse:
    data = json.dumps(payload).encode("utf-8")
    request = Request(url=url, data=data, method="POST")
    for key, value in headers.items():
        request.add_header(key, value)

    try:
        with urlopen(request, timeout=timeout_sec) as response:
            return TransportResponse(
                status_code=getattr(response, "status", response.getcode()),
                body=response.read(),
                headers=dict(response.headers.items()),
            )
    except HTTPError as exc:
        return TransportResponse(
            status_code=exc.code,
            body=exc.read(),
            headers=dict(exc.headers.items()) if exc.headers else {},
        )


class APIClient:
    def __init__(
        self,
        config: GatewayConfig,
        *,
        transport: Transport | None = None,
        strict_response_validation: bool = True,
    ) -> None:
        self._config = config
        self._transport = transport or _default_transport
        self._strict_response_validation = strict_response_validation

    @property
    def endpoint_url(self) -> str:
        return urljoin(f"{self._config.base_url}/", self._config.scan_result_path.lstrip("/"))

    def build_request_payload(self, request: ScanResult | ScanSubmitRequest) -> dict[str, Any]:
        submit_request = self._coerce_submit_request(request)
        return submit_request.to_payload()

    def submit(self, request: ScanResult | ScanSubmitRequest) -> SubmitResult:
        submit_request = self._coerce_submit_request(request)
        payload = submit_request.to_payload()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": self._config.user_agent,
        }

        try:
            response = self._transport(
                self.endpoint_url,
                payload,
                self._config.request_timeout_sec,
                headers,
            )
        except (TimeoutError, socket.timeout) as exc:
            return self._build_transport_error(payload, "NETWORK_TIMEOUT", f"request timed out: {exc}")
        except (URLError, OSError) as exc:
            return self._build_transport_error(payload, "NETWORK_ERROR", f"request failed: {exc}")

        http_ok = 200 <= response.status_code < 300
        try:
            body = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return self._build_invalid_response(
                submit_request,
                payload,
                response.status_code,
                True,
                http_ok,
                {},
                "response body is not valid JSON",
            )

        if not isinstance(body, dict):
            return self._build_invalid_response(
                submit_request,
                payload,
                response.status_code,
                True,
                http_ok,
                {},
                "response JSON must be an object",
            )
        try:
            parsed_body = self._validate_response_body(body, submit_request)
        except ValueError as exc:
            return self._build_invalid_response(
                submit_request,
                payload,
                response.status_code,
                True,
                http_ok,
                body,
                str(exc),
            )

        success = parsed_body["success"]
        code = parsed_body["code"]
        message = parsed_body["message"]

        status = "business_success" if http_ok and success else "business_error" if http_ok else "http_error"
        error = None
        if not (http_ok and success):
            error = VisionErrorResult(
                stage="gateway",
                error_code=code,
                message=message,
                source_id=submit_request.source_id,
                frame_id=submit_request.frame_id,
            )

        return SubmitResult(
            status=status,
            http_status=response.status_code,
            transport_success=True,
            http_ok=http_ok,
            response_valid=True,
            business_success=http_ok and success,
            code=code,
            message=message,
            request_payload=payload,
            response_payload=parsed_body,
            error=error,
        )

    def _coerce_submit_request(self, request: ScanResult | ScanSubmitRequest) -> ScanSubmitRequest:
        if isinstance(request, ScanSubmitRequest):
            return request
        if isinstance(request, ScanResult):
            return request.to_submit_request()
        raise TypeError("request must be a ScanResult or ScanSubmitRequest")

    def _build_invalid_response(
        self,
        submit_request: ScanSubmitRequest,
        payload: dict[str, Any],
        status_code: int,
        transport_success: bool,
        http_ok: bool,
        response_payload: dict[str, Any],
        message: str,
    ) -> SubmitResult:
        return SubmitResult(
            status="invalid_response",
            http_status=status_code,
            transport_success=transport_success,
            http_ok=http_ok,
            response_valid=False,
            business_success=False,
            code="INVALID_RESPONSE",
            message=message,
            request_payload=payload,
            response_payload=response_payload,
            error=VisionErrorResult(
                stage="gateway",
                error_code="INVALID_RESPONSE",
                message=message,
                source_id=submit_request.source_id,
                frame_id=submit_request.frame_id,
            ),
        )

    def _build_transport_error(self, payload: dict[str, Any], code: str, message: str) -> SubmitResult:
        return SubmitResult(
            status="network_error",
            http_status=None,
            transport_success=False,
            http_ok=False,
            response_valid=False,
            business_success=False,
            code=code,
            message=message,
            request_payload=payload,
            response_payload={},
            error=VisionErrorResult(stage="gateway", error_code=code, message=message),
        )

    def _validate_response_body(
        self,
        body: dict[str, Any],
        submit_request: ScanSubmitRequest,
    ) -> dict[str, Any]:
        success = body.get("success")
        code = body.get("code")
        message = body.get("message")
        if not isinstance(success, bool):
            raise ValueError("response JSON missing required boolean field: success")
        if not isinstance(code, str) or not code.strip():
            raise ValueError("response JSON missing required string field: code")
        if not isinstance(message, str) or not message.strip():
            raise ValueError("response JSON missing required string field: message")

        parsed = dict(body)
        parsed["code"] = ensure_non_empty_text(code, "response.code")
        parsed["message"] = ensure_non_empty_text(message, "response.message")

        if self._strict_response_validation:
            asset_id = body.get("asset_id")
            extra = body.get("extra")
            if not isinstance(asset_id, str) or not asset_id.strip():
                raise ValueError("response JSON missing required string field: asset_id")
            if asset_id.strip() != submit_request.asset_id:
                raise ValueError("response asset_id does not match submitted asset_id")
            if not isinstance(extra, Mapping):
                raise ValueError("response JSON missing required object field: extra")
            parsed["asset_id"] = ensure_non_empty_text(asset_id, "response.asset_id")
            parsed["extra"] = dict(extra)
        else:
            parsed.setdefault("asset_id", submit_request.asset_id)
            extra = parsed.get("extra", {})
            parsed["extra"] = dict(extra) if isinstance(extra, Mapping) else {}

        return parsed
