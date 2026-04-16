from __future__ import annotations

from dataclasses import replace
from typing import Any

from app.config import PreprocessConfig
from models.frame import DECODE_CANDIDATES_EXTRA_KEY, QUALITY_REPORT_EXTRA_KEY, QUALITY_RETRY_EXTRA_KEY, FrameData

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

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details = details or {}


class QualityChecker:
    def __init__(self, config: PreprocessConfig) -> None:
        self._config = config
        self._last_report: dict[str, Any] | None = None

    def measure_sharpness(self, frame: FrameData) -> float:
        self._ensure_dependencies()
        image = _as_numpy_image(frame.image)
        gray = image if image.ndim == 2 else cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        score = float(cv2.Laplacian(gray, cv2.CV_64F).var())
        return score

    def validate(self, frame: FrameData) -> float:
        reports = self._build_candidate_reports(frame)
        best = max(
            reports,
            key=lambda item: (
                float(item["quality_score"]),
                float(item["sharpness"]),
                -int(item["order"]),
            ),
        )
        self._last_report = {
            "selected_candidate": best["name"],
            "candidate_count": len(reports),
            "retry_applied": bool(frame.extra.get(QUALITY_RETRY_EXTRA_KEY, False)),
            "candidates": tuple(
                {
                    "name": item["name"],
                    "origin": item["origin"],
                    "variant": item["variant"],
                    "sharpness": item["sharpness"],
                    "quality_score": item["quality_score"],
                    "mean_luma": item["mean_luma"],
                    "contrast_std": item["contrast_std"],
                }
                for item in reports
            ),
        }
        threshold = self._config.laplacian_variance_threshold
        if self._config.enable_quality_gate:
            reasons: list[str] = []
            if float(best["sharpness"]) < threshold:
                reasons.append("low_sharpness")
            if float(best["quality_score"]) < self._config.min_quality_score:
                reasons.append("low_quality_score")
            if reasons:
                details = dict(self._last_report)
                details.update(
                    {
                        "classifier": "low_quality",
                        "reasons": tuple(reasons),
                        "threshold": float(threshold),
                    }
                )
                raise QualityGateError(
                    "image quality below threshold: "
                    f"candidate={best['name']} sharpness={best['sharpness']:.2f} "
                    f"quality_score={best['quality_score']:.3f} reasons={','.join(reasons)}",
                    details=details,
                )
        return float(best["sharpness"])

    def annotate(self, frame: FrameData, *, sharpness: float) -> FrameData:
        extra = dict(frame.extra)
        report = dict(self._last_report or {})
        selected_name = str(report.get("selected_candidate", extra.get("quality", {}).get("selected_candidate", "current")))
        extra["quality"] = {
            "sharpness": sharpness,
            "threshold": self._config.laplacian_variance_threshold,
            "quality_score": self._quality_score(sharpness),
            "selected_candidate": selected_name,
            "candidate_count": int(report.get("candidate_count", 1)),
            "retry_applied": bool(report.get("retry_applied", False)),
        }
        if report:
            extra[QUALITY_REPORT_EXTRA_KEY] = report
        return replace(frame, extra=extra)

    def _ensure_dependencies(self) -> None:
        if cv2 is None or np is None:
            raise PreprocessDependencyError(
                "preprocess quality check requires opencv-python-headless and numpy"
            )

    def _build_candidate_reports(self, frame: FrameData) -> list[dict[str, Any]]:
        reports: list[dict[str, Any]] = []
        candidates = frame.extra.get(DECODE_CANDIDATES_EXTRA_KEY)
        iterable = candidates if isinstance(candidates, list) and candidates else [self._fallback_candidate(frame)]
        for order, candidate in enumerate(iterable):
            image = _as_numpy_image(candidate["image"])
            gray = image if image.ndim == 2 else cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            sharpness = float(cv2.Laplacian(gray, cv2.CV_64F).var())
            reports.append(
                {
                    "order": order,
                    "name": str(candidate.get("name", f"candidate-{order}")),
                    "origin": str(candidate.get("origin", "full")),
                    "variant": str(candidate.get("variant", "original")),
                    "sharpness": sharpness,
                    "quality_score": self._quality_score(sharpness),
                    "mean_luma": float(gray.mean()),
                    "contrast_std": float(gray.std()),
                }
            )
        return reports

    def _quality_score(self, sharpness: float) -> float:
        threshold = max(float(self._config.laplacian_variance_threshold), 1.0)
        return min(1.0, float(sharpness) / threshold)

    def _fallback_candidate(self, frame: FrameData) -> dict[str, Any]:
        return {
            "name": "full_original",
            "image": frame.image,
            "origin": "full",
            "variant": "original",
        }


def _as_numpy_image(image: Any):
    if np is None:
        raise PreprocessDependencyError("numpy is required for preprocess quality check")
    if isinstance(image, np.ndarray):
        return image
    raise PreprocessError("frame image must be a numpy array for preprocess quality check")
