from __future__ import annotations

import time
from typing import Any

from capture.base import (
    CaptureConnectionLostError,
    CaptureDependencyError,
    CaptureOpenError,
    CaptureReadError,
    CaptureTemporaryReadError,
    FrameSource,
    configure_opencv_capture_timeouts,
    extract_frame_dimensions,
)
from models.frame import FrameData

try:
    import cv2  # type: ignore
except ImportError:  # pragma: no cover - exercised via constructor/open checks
    cv2 = None


class IPCameraFrameSource(FrameSource):
    def __init__(self, *, stream_url: str, source_id: str, connect_timeout_sec: float = 3.0) -> None:
        self._stream_url = stream_url
        self._source_id = source_id
        self._connect_timeout_sec = float(connect_timeout_sec)
        self._capture: Any | None = None
        self._frame_index = 0

    def open(self) -> None:
        if cv2 is None:
            raise CaptureDependencyError(
                "live IP camera capture requires cv2; install opencv-python before using --run-mode live"
            )
        self._capture = cv2.VideoCapture(self._stream_url)
        configure_opencv_capture_timeouts(cv2, self._capture, self._connect_timeout_sec)
        if not self._capture or not self._capture.isOpened():
            raise CaptureOpenError(
                f"unable to open IP camera stream {self._stream_url!r}; "
                f"check stream URL, connectivity, and open timeout {self._connect_timeout_sec:.1f}s"
            )

    def read(self) -> FrameData:
        if self._capture is None:
            raise CaptureReadError("IP camera source is not open")
        ok, frame = self._capture.read()
        if not ok or frame is None:
            is_open = True
            try:
                is_open = bool(self._capture.isOpened())
            except Exception:
                is_open = False
            if not is_open:
                raise CaptureConnectionLostError("IP camera stream connection was lost during capture")
            raise CaptureTemporaryReadError("temporary IP camera frame read failure")

        self._frame_index += 1
        width, height, channel_count = extract_frame_dimensions(frame)
        return FrameData(
            frame_id=f"{self._source_id}-{self._frame_index}",
            image=frame,
            timestamp=time.time(),
            source_id=self._source_id,
            width=width,
            height=height,
            channel_count=channel_count,
        )

    def close(self) -> None:
        if self._capture is not None:
            self._capture.release()
            self._capture = None
        self._frame_index = 0

    def supports_reconnect(self) -> bool:
        return True
