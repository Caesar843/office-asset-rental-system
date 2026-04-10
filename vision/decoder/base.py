from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from models.decode_result import DecodeResult
from models.frame import FrameData


class DecoderError(RuntimeError):
    """Raised when a decoder cannot produce valid decode results."""


class DecoderDependencyError(DecoderError):
    """Raised when the configured decoder backend is unavailable."""


class DecoderConfigurationError(DecoderError):
    """Raised when decoder configuration is invalid."""


class Decoder(Protocol):
    def decode(self, frame: FrameData) -> Sequence[DecodeResult]:
        """Decode barcode-like content from one frame."""
