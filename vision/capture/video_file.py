from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from capture.base import (
    CaptureDependencyError,
    CaptureOpenError,
    CaptureReadError,
    CaptureStreamEnded,
    FrameSource,
    configure_opencv_capture_timeouts,
    extract_frame_dimensions,
)
from models.frame import FrameData

try:
    import cv2  # type: ignore
except ImportError:  # pragma: no cover - exercised by dependency tests
    cv2 = None


class VideoFileFrameSource(FrameSource):
    def __init__(self, *, video_path: str, source_id: str, connect_timeout_sec: float = 3.0) -> None:
        self._video_path = Path(video_path)
        self._source_id = source_id
        self._connect_timeout_sec = float(connect_timeout_sec)
        self._capture: Any | None = None
        self._frame_index = 0

    def open(self) -> None:
        if cv2 is None:
            raise CaptureDependencyError(
                "video_file capture requires cv2; install opencv-python-headless before using live video sources"
            )
        if not self._video_path.exists():
            raise CaptureOpenError(f"video file not found: {self._video_path}")
        self._capture = cv2.VideoCapture(str(self._video_path))
        configure_opencv_capture_timeouts(cv2, self._capture, self._connect_timeout_sec)
        if not self._capture or not self._capture.isOpened():
            raise CaptureOpenError(
                f"unable to open video file {self._video_path}; check the path and codec support"
            )

    def read(self) -> FrameData:
        if self._capture is None:
            raise CaptureReadError("video_file source is not open")
        ok, frame = self._capture.read()
        if not ok or frame is None:
            if self._frame_index > 0:
                raise CaptureStreamEnded(f"video file reached end of stream: {self._video_path}")
            raise CaptureReadError(f"failed to read the first frame from video file {self._video_path}")

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
            extra={"source_path": str(self._video_path)},
        )

    def close(self) -> None:
        if self._capture is not None:
            self._capture.release()
            self._capture = None
        self._frame_index = 0

    def is_finite(self) -> bool:
        return True
