from __future__ import annotations

from dataclasses import replace
from typing import Any

from app.config import PreprocessConfig
from models.frame import FrameData
from preprocess.quality_check import PreprocessDependencyError, PreprocessError

try:
    import cv2  # type: ignore
    import numpy as np
except ImportError:  # pragma: no cover - exercised in dependency-failure tests
    cv2 = None
    np = None


class ImageEnhancer:
    def __init__(self, config: PreprocessConfig) -> None:
        self._config = config

    def prepare(self, frame: FrameData) -> FrameData:
        return self._apply(frame, alpha=self._config.contrast_alpha, retry=False)

    def retry(self, frame: FrameData) -> FrameData:
        return self._apply(frame, alpha=self._config.retry_contrast_alpha, retry=True)

    def _apply(self, frame: FrameData, *, alpha: float, retry: bool) -> FrameData:
        self._ensure_dependencies()
        image = _as_numpy_image(frame.image).copy()
        steps: list[str] = []

        if self._config.enable_grayscale and image.ndim == 3:
            image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            steps.append("grayscale")

        if self._config.enable_contrast_enhance:
            image = cv2.convertScaleAbs(image, alpha=alpha, beta=self._config.contrast_beta)
            steps.append("contrast_enhance_retry" if retry else "contrast_enhance")

        height, width = image.shape[:2]
        channel_count = 1 if image.ndim == 2 else int(image.shape[2])
        extra = dict(frame.extra)
        history = list(extra.get("preprocess_steps", []))
        history.extend(steps)
        extra["preprocess_steps"] = history
        return replace(
            frame,
            image=image,
            width=width,
            height=height,
            channel_count=channel_count,
            extra=extra,
        )

    def _ensure_dependencies(self) -> None:
        if cv2 is None or np is None:
            raise PreprocessDependencyError(
                "image enhancement requires opencv-python-headless and numpy"
            )


def _as_numpy_image(image: Any):
    if np is None:
        raise PreprocessDependencyError("numpy is required for image enhancement")
    if isinstance(image, np.ndarray):
        return image
    raise PreprocessError("frame image must be a numpy array for image enhancement")
