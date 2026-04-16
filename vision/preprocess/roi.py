from __future__ import annotations

from dataclasses import replace
from typing import Any

from app.config import PreprocessConfig
from models.frame import DECODE_CANDIDATES_EXTRA_KEY, PRIMARY_DECODE_CANDIDATE_EXTRA_KEY, FrameData
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
            return self._attach_full_frame_candidate(frame)
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
        preprocess_steps = list(extra.get("preprocess_steps", []))
        roi_steps = tuple(preprocess_steps + ["roi"])
        candidates = [
            self._build_candidate(
                name="roi_original",
                image=cropped,
                bbox_offset=(x0, y0),
                origin="roi",
                variant="original",
                preprocess_steps=roi_steps,
            )
        ]
        if self._config.roi_fallback_to_full_frame:
            candidates.append(
                self._build_candidate(
                    name="full_original",
                    image=image,
                    bbox_offset=(0, 0),
                    origin="full",
                    variant="original",
                    preprocess_steps=tuple(preprocess_steps),
                )
            )
        extra["roi"] = {"x": x0, "y": y0, "width": x1 - x0, "height": y1 - y0}
        extra["preprocess_steps"] = list(roi_steps)
        extra[DECODE_CANDIDATES_EXTRA_KEY] = candidates
        extra[PRIMARY_DECODE_CANDIDATE_EXTRA_KEY] = "roi_original"
        channel_count = 1 if cropped.ndim == 2 else int(cropped.shape[2])
        return replace(
            frame,
            image=cropped,
            width=int(cropped.shape[1]),
            height=int(cropped.shape[0]),
            channel_count=channel_count,
            extra=extra,
        )

    def _attach_full_frame_candidate(self, frame: FrameData) -> FrameData:
        extra = dict(frame.extra)
        preprocess_steps = tuple(extra.get("preprocess_steps", []))
        extra[DECODE_CANDIDATES_EXTRA_KEY] = [
            self._build_candidate(
                name="full_original",
                image=frame.image,
                bbox_offset=(0, 0),
                origin="full",
                variant="original",
                preprocess_steps=preprocess_steps,
            )
        ]
        extra[PRIMARY_DECODE_CANDIDATE_EXTRA_KEY] = "full_original"
        return replace(frame, extra=extra)

    def _build_candidate(
        self,
        *,
        name: str,
        image: Any,
        bbox_offset: tuple[int, int],
        origin: str,
        variant: str,
        preprocess_steps: tuple[str, ...],
    ) -> dict[str, Any]:
        return {
            "name": name,
            "image": image,
            "bbox_offset": bbox_offset,
            "origin": origin,
            "variant": variant,
            "preprocess_steps": preprocess_steps,
        }
