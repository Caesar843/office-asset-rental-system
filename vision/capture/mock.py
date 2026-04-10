from __future__ import annotations

from capture.base import CaptureError, FrameSource
from models.frame import FrameData


class StaticFrameSource(FrameSource):
    def __init__(self, frame: FrameData) -> None:
        self._frame = frame
        self._is_open = False

    def open(self) -> None:
        self._is_open = True

    def read(self) -> FrameData:
        if not self._is_open:
            raise CaptureError("static frame source is not open")
        return self._frame

    def close(self) -> None:
        self._is_open = False
