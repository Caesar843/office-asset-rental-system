from __future__ import annotations

from collections.abc import Sequence

from app.config import DecodeConfig
from decoder.barcode_decoder import BarcodeDecoder
from decoder.base import Decoder, DecoderConfigurationError
from decoder.qr_decoder import QRCodeDecoder
from models.decode_result import DecodeResult
from models.frame import FrameData


class HybridDecoder(Decoder):
    def __init__(
        self,
        config: DecodeConfig,
        *,
        qr_decoder: Decoder | None = None,
        barcode_decoder: Decoder | None = None,
    ) -> None:
        self._config = config
        self._qr_decoder = qr_decoder or QRCodeDecoder(config)
        self._barcode_decoder = barcode_decoder or BarcodeDecoder(config)

    def decode(self, frame: FrameData) -> list[DecodeResult]:
        if not self._config.enable_qr and not self._config.enable_barcode:
            raise DecoderConfigurationError("hybrid decoder requires at least one enabled decode path")

        ordered_decoders: list[Decoder] = []
        if self._config.prefer_qr_first:
            if self._config.enable_qr:
                ordered_decoders.append(self._qr_decoder)
            if self._config.enable_barcode:
                ordered_decoders.append(self._barcode_decoder)
        else:
            if self._config.enable_barcode:
                ordered_decoders.append(self._barcode_decoder)
            if self._config.enable_qr:
                ordered_decoders.append(self._qr_decoder)

        merged: list[DecodeResult] = []
        seen: set[tuple[str, str, tuple[int, int, int, int] | None]] = set()
        for decoder in ordered_decoders:
            for result in decoder.decode(frame):
                fingerprint = (result.raw_text, result.symbology, result.bbox)
                if fingerprint in seen:
                    continue
                merged.append(result)
                seen.add(fingerprint)
            if merged and not self._config.allow_multi_decode:
                break

        if self._config.allow_multi_decode:
            return merged
        return merged[:1]
