from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from models.frame import FrameData


class CaptureError(RuntimeError):
    """Raised when a frame source cannot produce a valid frame."""


class CaptureDependencyError(CaptureError):
    """Raised when an optional runtime dependency is unavailable."""


class CaptureOpenError(CaptureError):
    """Raised when a capture device or stream cannot be opened."""


class CaptureReconnectError(CaptureOpenError):
    """Raised when a reconnect-capable source cannot be recovered."""


class CaptureReadError(CaptureError):
    """Raised when a frame source fails during frame retrieval."""


class CaptureTemporaryReadError(CaptureReadError):
    """Raised when a frame source hits a recoverable read failure."""


class CaptureConnectionLostError(CaptureReadError):
    """Raised when a reconnect-capable source loses its underlying device or stream."""


class CaptureStreamEnded(CaptureError):
    """Raised when a finite source reaches its natural end of stream."""


def configure_opencv_capture_timeouts(cv2_module: Any, capture: Any, timeout_sec: float) -> None:
    if cv2_module is None or capture is None:
        return
    timeout_ms = int(float(timeout_sec) * 1000)
    for attr_name in ("CAP_PROP_OPEN_TIMEOUT_MSEC", "CAP_PROP_READ_TIMEOUT_MSEC"):
        prop = getattr(cv2_module, attr_name, None)
        if prop is not None:
            try:
                capture.set(prop, timeout_ms)
            except Exception:
                continue


def extract_frame_dimensions(frame: Any) -> tuple[int | None, int | None, int | None]:
    shape = getattr(frame, "shape", None)
    height = int(shape[0]) if shape and len(shape) >= 1 else None
    width = int(shape[1]) if shape and len(shape) >= 2 else None
    channel_count = int(shape[2]) if shape and len(shape) >= 3 else 1
    return width, height, channel_count


class FrameSource(ABC):
    @abstractmethod
    def open(self) -> None:
        """Allocate capture resources."""

    @abstractmethod
    def read(self) -> FrameData:
        """Read a single frame."""

    @abstractmethod
    def close(self) -> None:
        """Release capture resources."""

    def supports_reconnect(self) -> bool:
        return False

    def is_finite(self) -> bool:
        return False

    def reconnect(self) -> None:
        if not self.supports_reconnect():
            raise CaptureOpenError(f"{self.__class__.__name__} does not support reconnect")
        self.close()
        self.open()

    def __enter__(self) -> FrameSource:
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
