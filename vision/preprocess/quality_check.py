from __future__ import annotations

from dataclasses import replace
from typing import Any

from app.config import PreprocessConfig
from models.frame import FrameData

try:
    import cv2  # type: ignore
    import numpy as np
except ImportError:  # pragma: no cover - exercised in dependency-failure tests
    cv2 = None
    np = None


class PreprocessError(RuntimeError):
    """Raised when preprocess cannot produce a valid decode-ready frame."""


class PreprocessDependencyError(PreprocessError):
    """Raised when preprocess dependencies are unavailable."""


class QualityGateError(PreprocessError):
    """Raised when a frame fails the quality gate."""


class QualityChecker:
    def __init__(self, config: PreprocessConfig) -> None:
        self._config = config

    def measure_sharpness(self, frame: FrameData) -> float:
        self._ensure_dependencies()
        image = _as_numpy_image(frame.image)
        gray = image if image.ndim == 2 else cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        score = float(cv2.Laplacian(gray, cv2.CV_64F).var())
        return score

    def validate(self, frame: FrameData) -> float:
        sharpness = self.measure_sharpness(frame)
        if self._config.enable_quality_gate and sharpness < self._config.laplacian_variance_threshold:
            raise QualityGateError(
                f"image quality below threshold: sharpness={sharpness:.2f} < {self._config.laplacian_variance_threshold:.2f}"
            )
        return sharpness

    def annotate(self, frame: FrameData, *, sharpness: float) -> FrameData:
        extra = dict(frame.extra)
        extra["quality"] = {
            "sharpness": sharpness,
            "threshold": self._config.laplacian_variance_threshold,
        }
        return replace(frame, extra=extra)

    def _ensure_dependencies(self) -> None:
        if cv2 is None or np is None:
            raise PreprocessDependencyError(
                "preprocess quality check requires opencv-python-headless and numpy"
            )


def _as_numpy_image(image: Any):
    if np is None:
        raise PreprocessDependencyError("numpy is required for preprocess quality check")
    if isinstance(image, np.ndarray):
        return image
    raise PreprocessError("frame image must be a numpy array for preprocess quality check")
