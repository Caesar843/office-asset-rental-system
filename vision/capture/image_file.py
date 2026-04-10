from __future__ import annotations

import time
from pathlib import Path

from capture.base import CaptureDependencyError, CaptureOpenError, CaptureStreamEnded, FrameSource, extract_frame_dimensions
from models.frame import FrameData

try:
    import numpy as np
    from PIL import Image
except ImportError:  # pragma: no cover - dependency failure is unit tested
    np = None
    Image = None


class ImageFileFrameSource(FrameSource):
    def __init__(self, *, image_path: str, source_id: str) -> None:
        self._image_path = Path(image_path)
        self._source_id = source_id
        self._frame: FrameData | None = None
        self._open_count = 0
        self._consumed = False

    def open(self) -> None:
        if np is None or Image is None:
            raise CaptureDependencyError("image_file capture requires pillow and numpy")
        if not self._image_path.exists():
            raise CaptureOpenError(f"image file not found: {self._image_path}")
        self._open_count += 1
        self._consumed = False
        image = Image.open(self._image_path).convert("RGB")
        rgb = np.array(image)
        bgr = rgb[:, :, ::-1].copy()
        width, height, channel_count = extract_frame_dimensions(bgr)
        self._frame = FrameData(
            frame_id=f"{self._image_path.stem}-{self._open_count}",
            image=bgr,
            timestamp=time.time(),
            source_id=self._source_id,
            width=width,
            height=height,
            channel_count=channel_count,
            extra={"source_path": str(self._image_path)},
        )

    def read(self) -> FrameData:
        if self._frame is None:
            raise CaptureOpenError("image_file source is not open")
        if self._consumed:
            raise CaptureStreamEnded(f"image file source reached end of input: {self._image_path}")
        self._consumed = True
        return self._frame

    def close(self) -> None:
        self._frame = None
        self._consumed = False

    def is_finite(self) -> bool:
        return True
