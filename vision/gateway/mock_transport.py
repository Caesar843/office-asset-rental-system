from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from gateway.api_client import Transport, TransportResponse


def build_contract_mock_transport(
    *,
    status_code: int = 200,
    success: bool = True,
    code: str = "SCAN_ACCEPTED",
    message: str = "scan result accepted",
    response_overrides: Mapping[str, Any] | None = None,
) -> Transport:
    """Build an in-process transport that speaks the frozen scan response contract."""

    def transport(
        url: str,
        payload: Mapping[str, Any],
        timeout_sec: float,
        headers: Mapping[str, str],
    ) -> TransportResponse:
        del url, timeout_sec, headers
        asset_id = str(payload.get("asset_id", "")).strip()
        body: dict[str, Any] = {
            "success": success,
            "code": code,
            "message": message,
            "asset_id": asset_id,
            "extra": {
                "transport": "inprocess_contract_mock",
                "source_id": payload.get("source_id"),
            },
        }
        if response_overrides:
            body.update(dict(response_overrides))
        return TransportResponse(
            status_code=status_code,
            body=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )

    return transport
