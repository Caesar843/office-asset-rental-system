from __future__ import annotations

from collections.abc import Sequence

from decoder.base import Decoder
from models.decode_result import DecodeResult
from models.frame import FrameData


class StaticDecoder(Decoder):
    def __init__(
        self,
        *,
        results: Sequence[DecodeResult] | None = None,
        error: Exception | None = None,
    ) -> None:
        self._results = tuple(results or ())
        self._error = error

    def decode(self, frame: FrameData) -> Sequence[DecodeResult]:
        del frame
        if self._error is not None:
            raise self._error
        return self._results
