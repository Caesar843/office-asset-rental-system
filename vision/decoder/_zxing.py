from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from app.config import DecodeConfig
from decoder.base import DecoderDependencyError, DecoderError
from models.decode_result import DecodeResult
from models.frame import FrameData

try:
    import numpy as np
    import zxingcpp
except ImportError:  # pragma: no cover - dependency-failure path is unit tested
    np = None
    zxingcpp = None


def ensure_zxingcpp_available() -> None:
    if zxingcpp is None or np is None:
        raise DecoderDependencyError(
            "real decoding requires zxing-cpp and numpy; install them before using live mode"
        )


def read_with_zxing(
    frame: FrameData,
    *,
    formats: Any,
    config: DecodeConfig,
) -> list[DecodeResult]:
    ensure_zxingcpp_available()
    image = _coerce_image(frame.image)
    results = zxingcpp.read_barcodes(
        image,
        formats=formats,
        try_rotate=config.try_rotate,
        try_downscale=config.try_downscale,
        try_invert=config.try_invert,
    )
    return [_to_decode_result(item) for item in results if item.valid and str(item.text).strip()]


def qr_format() -> Any:
    ensure_zxingcpp_available()
    return zxingcpp.BarcodeFormat.QRCode


def linear_barcode_formats() -> Any:
    ensure_zxingcpp_available()
    return zxingcpp.BarcodeFormat.LinearCodes


def is_allowed_symbology(symbology: str, allowed_symbologies: Sequence[str]) -> bool:
    normalized = symbology.upper()
    allowed = {item.upper() for item in allowed_symbologies}
    if normalized == "QR":
        return "QR" in allowed or normalized in allowed
    return "BARCODE" in allowed or normalized in allowed


def _coerce_image(image: Any):
    if np is None:
        raise DecoderDependencyError("numpy is required for real decoding")
    if isinstance(image, np.ndarray):
        return image
    raise DecoderError("frame image must be a numpy array for real decoding")


def _to_decode_result(item: Any) -> DecodeResult:
    symbology = _normalize_symbology(item)
    bbox = _extract_bbox(item)
    extra = {
        "decoder_backend": "zxingcpp",
        "symbology_identifier": getattr(item, "symbology_identifier", None),
        "content_type": str(getattr(item, "content_type", "")) or None,
    }
    return DecodeResult(
        raw_text=str(item.text).strip(),
        symbology=symbology,
        bbox=bbox,
        confidence=None,
        decoder_name="zxingcpp",
        extra={key: value for key, value in extra.items() if value is not None},
    )


def _normalize_symbology(item: Any) -> str:
    raw_name = getattr(getattr(item, "format", None), "name", "")
    if raw_name.upper().startswith("QR"):
        return "QR"
    return raw_name.upper()


def _extract_bbox(item: Any) -> tuple[int, int, int, int] | None:
    position = getattr(item, "position", None)
    if position is None:
        return None
    points = [
        position.top_left,
        position.top_right,
        position.bottom_right,
        position.bottom_left,
    ]
    xs = [int(point.x) for point in points]
    ys = [int(point.y) for point in points]
    x0 = min(xs)
    y0 = min(ys)
    x1 = max(xs)
    y1 = max(ys)
    width = max(1, x1 - x0)
    height = max(1, y1 - y0)
    return (x0, y0, width, height)
