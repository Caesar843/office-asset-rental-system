from __future__ import annotations

from dataclasses import replace
from typing import Any

from app.config import PreprocessConfig
from models.frame import (
    DECODE_CANDIDATES_EXTRA_KEY,
    PRIMARY_DECODE_CANDIDATE_EXTRA_KEY,
    QUALITY_RETRY_EXTRA_KEY,
    FrameData,
)
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
        extra = dict(frame.extra)
        base_candidates = self._load_candidates(frame)
        enhancement_steps = ["contrast_enhance_retry"] if retry else self._prepare_steps()
        if not enhancement_steps:
            return replace(frame, extra=extra)
        transformed_candidates: list[dict[str, Any]] = []
        for candidate in base_candidates:
            if retry and str(candidate.get("variant", "")) != "original":
                continue
            transformed_image = self._enhance_image(candidate["image"], alpha=alpha)
            transformed_candidates.append(
                {
                    **candidate,
                    "name": f"{candidate['origin']}_{'retry' if retry else 'enhanced'}",
                    "image": transformed_image,
                    "variant": "retry" if retry else "enhanced",
                    "preprocess_steps": tuple(list(candidate.get("preprocess_steps", ())) + enhancement_steps),
                }
            )

        if retry:
            candidates = transformed_candidates + base_candidates
            primary_name = transformed_candidates[0]["name"] if transformed_candidates else extra.get(PRIMARY_DECODE_CANDIDATE_EXTRA_KEY)
        else:
            candidates = []
            for candidate in base_candidates:
                candidates.append(candidate)
                candidates.extend(
                    item
                    for item in transformed_candidates
                    if item["origin"] == candidate["origin"] and item["variant"] == "enhanced"
                )
            primary_name = extra.get(PRIMARY_DECODE_CANDIDATE_EXTRA_KEY, candidates[0]["name"] if candidates else None)

        primary_candidate = self._select_primary_candidate(candidates, primary_name)
        history = list(extra.get("preprocess_steps", []))
        history.extend(enhancement_steps)
        extra["preprocess_steps"] = list(dict.fromkeys(history))
        extra[DECODE_CANDIDATES_EXTRA_KEY] = candidates
        if primary_name is not None:
            extra[PRIMARY_DECODE_CANDIDATE_EXTRA_KEY] = primary_name
        if retry:
            extra[QUALITY_RETRY_EXTRA_KEY] = True
        height, width = primary_candidate["image"].shape[:2]
        channel_count = 1 if primary_candidate["image"].ndim == 2 else int(primary_candidate["image"].shape[2])
        return replace(
            frame,
            image=primary_candidate["image"],
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

    def _load_candidates(self, frame: FrameData) -> list[dict[str, Any]]:
        extra = dict(frame.extra)
        candidates = extra.get(DECODE_CANDIDATES_EXTRA_KEY)
        if isinstance(candidates, list) and candidates:
            return [dict(candidate) for candidate in candidates]
        return [
            {
                "name": "full_original",
                "image": frame.image,
                "bbox_offset": (0, 0),
                "origin": "full",
                "variant": "original",
                "preprocess_steps": tuple(extra.get("preprocess_steps", ())),
            }
        ]

    def _enhance_image(self, image: Any, *, alpha: float):
        enhanced = _as_numpy_image(image).copy()
        if self._config.enable_grayscale and enhanced.ndim == 3:
            enhanced = cv2.cvtColor(enhanced, cv2.COLOR_BGR2GRAY)
        if self._config.enable_contrast_enhance:
            enhanced = cv2.convertScaleAbs(enhanced, alpha=alpha, beta=self._config.contrast_beta)
        return enhanced

    def _prepare_steps(self) -> list[str]:
        steps: list[str] = []
        if self._config.enable_grayscale:
            steps.append("grayscale")
        if self._config.enable_contrast_enhance:
            steps.append("contrast_enhance")
        return steps

    def _select_primary_candidate(
        self,
        candidates: list[dict[str, Any]],
        primary_name: str | None,
    ) -> dict[str, Any]:
        if not candidates:
            raise PreprocessError("image enhancement produced no decode candidates")
        if primary_name is None:
            return candidates[0]
        for candidate in candidates:
            if candidate.get("name") == primary_name:
                return candidate
        return candidates[0]


def _as_numpy_image(image: Any):
    if np is None:
        raise PreprocessDependencyError("numpy is required for image enhancement")
    if isinstance(image, np.ndarray):
        return image
    raise PreprocessError("frame image must be a numpy array for image enhancement")
