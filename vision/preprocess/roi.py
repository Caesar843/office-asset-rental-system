from __future__ import annotations

from dataclasses import replace
from typing import Any

from app.config import PreprocessConfig
from models.frame import FrameData
from preprocess.quality_check import PreprocessDependencyError, PreprocessError

try:
    import numpy as np
except ImportError:  # pragma: no cover - exercised in dependency-failure tests
    np = None


class ROIProcessor:
    def __init__(self, config: PreprocessConfig) -> None:
        self._config = config

    def apply(self, frame: FrameData) -> FrameData:
        if not self._config.enable_roi or self._config.roi is None:
            return frame
        if np is None:
            raise PreprocessDependencyError("numpy is required for ROI cropping")
        if not isinstance(frame.image, np.ndarray):
            raise PreprocessError("frame image must be a numpy array for ROI cropping")

        image = frame.image
        height, width = image.shape[:2]
        rx, ry, rw, rh = self._config.roi
        x0 = int(round(width * rx))
        y0 = int(round(height * ry))
        x1 = int(round(width * (rx + rw)))
        y1 = int(round(height * (ry + rh)))
        cropped = image[y0:y1, x0:x1]
        if cropped.size == 0:
            raise PreprocessError("configured ROI produced an empty crop")

        extra = dict(frame.extra)
        extra["roi"] = {"x": x0, "y": y0, "width": x1 - x0, "height": y1 - y0}
        channel_count = 1 if cropped.ndim == 2 else int(cropped.shape[2])
        return replace(
            frame,
            image=cropped,
            width=int(cropped.shape[1]),
            height=int(cropped.shape[0]),
            channel_count=channel_count,
            extra=extra,
        )
