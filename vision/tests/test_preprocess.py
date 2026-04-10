from __future__ import annotations

from unittest.mock import patch
import unittest

from app.config import VisionConfig
from app.runner import _build_preprocessor
from models.frame import FrameData
from preprocess.image_enhance import ImageEnhancer
from preprocess.quality_check import QualityChecker, QualityGateError
from preprocess.roi import ROIProcessor
from tests._fixtures import blur_image, combine_images_horizontally, make_code128_image, make_qr_image


class PreprocessTests(unittest.TestCase):
    def test_quality_gate_passes_for_sharp_qr(self) -> None:
        frame = FrameData(frame_id="sharp", image=make_qr_image("AS-4001"), timestamp=1700000000, source_id="cam-1")
        checker = QualityChecker(VisionConfig().preprocess)

        sharpness = checker.validate(frame)

        self.assertGreater(sharpness, 0.0)

    def test_low_quality_image_is_rejected(self) -> None:
        blurred = blur_image(make_qr_image("AS-4002"), kernel_size=31)
        config = VisionConfig.from_overrides(preprocess={"laplacian_variance_threshold": 150.0})
        checker = QualityChecker(config.preprocess)
        frame = FrameData(frame_id="blurred", image=blurred, timestamp=1700000000, source_id="cam-1")

        with self.assertRaises(QualityGateError):
            checker.validate(frame)

    def test_roi_processor_crops_configured_region(self) -> None:
        combo = combine_images_horizontally(make_qr_image("AS-4003"), make_code128_image("AS-4004"))
        config = VisionConfig.from_overrides(preprocess={"enable_roi": True, "roi": (0.0, 0.0, 0.45, 1.0)})
        processor = ROIProcessor(config.preprocess)
        frame = FrameData(frame_id="roi", image=combo, timestamp=1700000000, source_id="cam-1")

        cropped = processor.apply(frame)

        self.assertLess(cropped.width, frame.image.shape[1])
        self.assertIn("roi", cropped.extra)

    def test_retry_enhancement_path_is_triggered(self) -> None:
        frame = FrameData(frame_id="retry", image=make_qr_image("AS-4005"), timestamp=1700000000, source_id="cam-1")
        config = VisionConfig.from_overrides(
            runtime={"run_mode": "live"},
            preprocess={"retry_with_enhancement": True, "max_retry_count": 1},
        )
        preprocessor = _build_preprocessor(config)

        with patch.object(QualityChecker, "validate", side_effect=[QualityGateError("low quality"), 99.0]):
            processed = preprocessor(frame)

        self.assertIn("contrast_enhance_retry", processed.extra["preprocess_steps"])


if __name__ == "__main__":
    unittest.main()
