from __future__ import annotations

from app.config import DecodeConfig
from decoder._zxing import is_allowed_symbology, linear_barcode_formats, read_with_zxing
from decoder.base import Decoder
from models.decode_result import DecodeResult
from models.frame import FrameData


class BarcodeDecoder(Decoder):
    def __init__(self, config: DecodeConfig) -> None:
        self._config = config

    def decode(self, frame: FrameData) -> list[DecodeResult]:
        results = read_with_zxing(frame, formats=linear_barcode_formats(), config=self._config)
        return [result for result in results if is_allowed_symbology(result.symbology, self._config.allowed_symbologies)]
