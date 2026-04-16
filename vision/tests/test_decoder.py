from __future__ import annotations

from unittest.mock import patch
import unittest

from app.config import DecodeConfig, VisionConfig
from decoder.barcode_decoder import BarcodeDecoder
from decoder.base import DecoderDependencyError, DecoderError
from decoder.hybrid_decoder import HybridDecoder
from decoder.qr_decoder import QRCodeDecoder
from decoder.stub import StaticDecoder
from models.decode_result import DecodeResult
from models.frame import DECODE_CANDIDATES_EXTRA_KEY, PRIMARY_DECODE_CANDIDATE_EXTRA_KEY, FrameData
from preprocess.image_enhance import ImageEnhancer
from preprocess.roi import ROIProcessor
from tests._fixtures import combine_images_horizontally, make_blank_image, make_code128_image, make_qr_image


class _StageAwareDecoder:
    def __init__(self, *, results_by_stage: dict[str, list[DecodeResult]]) -> None:
        self._results_by_stage = results_by_stage
        self.calls: list[str] = []

    def decode(self, frame: FrameData) -> list[DecodeResult]:
        stage = str(frame.extra.get("decode_stage", "full_original"))
        self.calls.append(stage)
        return list(self._results_by_stage.get(stage, []))


class DecoderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.frame = FrameData(frame_id="frame-1", image=make_qr_image("AS-2001"), timestamp=1700000000, source_id="webcam-0")

    def test_static_decoder_returns_configured_results(self) -> None:
        decoder = StaticDecoder(results=[DecodeResult(raw_text="AS-2001", symbology="QR")])

        results = decoder.decode(self.frame)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].raw_text, "AS-2001")

    def test_static_decoder_can_raise_stable_error(self) -> None:
        decoder = StaticDecoder(error=DecoderError("no code detected"))

        with self.assertRaises(DecoderError) as ctx:
            decoder.decode(self.frame)

        self.assertIn("no code detected", str(ctx.exception))

    def test_qr_decoder_decodes_real_qr_image(self) -> None:
        decoder = QRCodeDecoder(DecodeConfig())

        results = decoder.decode(self.frame)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].raw_text, "AS-2001")
        self.assertEqual(results[0].symbology, "QR")
        self.assertIsNotNone(results[0].bbox)

    def test_barcode_decoder_decodes_real_code128_image(self) -> None:
        frame = FrameData(
            frame_id="frame-bar-1",
            image=make_code128_image("AS-2002"),
            timestamp=1700000001,
            source_id="webcam-0",
        )
        decoder = BarcodeDecoder(DecodeConfig())

        results = decoder.decode(frame)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].raw_text, "AS-2002")
        self.assertEqual(results[0].symbology, "CODE128")

    def test_blank_image_returns_no_decode_results(self) -> None:
        frame = FrameData(
            frame_id="frame-empty-1",
            image=make_blank_image(),
            timestamp=1700000002,
            source_id="webcam-0",
        )
        decoder = HybridDecoder(DecodeConfig())

        results = decoder.decode(frame)

        self.assertEqual(results, [])

    def test_decoder_dependency_failure_is_stable(self) -> None:
        with patch("decoder._zxing.ensure_zxingcpp_available", side_effect=DecoderDependencyError("missing zxing-cpp")):
            decoder = QRCodeDecoder(DecodeConfig())

            with self.assertRaises(DecoderDependencyError) as ctx:
                decoder.decode(self.frame)

        self.assertIn("missing zxing-cpp", str(ctx.exception))

    def test_hybrid_decoder_honors_prefer_qr_first_when_multi_disabled(self) -> None:
        combo = combine_images_horizontally(make_qr_image("AS-2003"), make_code128_image("AS-2004"))
        frame = FrameData(frame_id="frame-combo-1", image=combo, timestamp=1700000003, source_id="webcam-0")
        decoder = HybridDecoder(DecodeConfig(allow_multi_decode=False, prefer_qr_first=True))

        results = decoder.decode(frame)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].raw_text, "AS-2003")
        self.assertEqual(results[0].symbology, "QR")

    def test_hybrid_decoder_honors_barcode_first_when_multi_disabled(self) -> None:
        combo = combine_images_horizontally(make_qr_image("AS-2005"), make_code128_image("AS-2006"))
        frame = FrameData(frame_id="frame-combo-2", image=combo, timestamp=1700000004, source_id="webcam-0")
        decoder = HybridDecoder(DecodeConfig(allow_multi_decode=False, prefer_qr_first=False))

        results = decoder.decode(frame)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].raw_text, "AS-2006")
        self.assertEqual(results[0].symbology, "CODE128")

    def test_hybrid_decoder_returns_multiple_results_when_enabled(self) -> None:
        combo = combine_images_horizontally(make_qr_image("AS-2007"), make_code128_image("AS-2008"))
        frame = FrameData(frame_id="frame-combo-3", image=combo, timestamp=1700000005, source_id="webcam-0")
        decoder = HybridDecoder(DecodeConfig(allow_multi_decode=True, prefer_qr_first=True))

        results = decoder.decode(frame)

        self.assertEqual(len(results), 2)
        self.assertEqual([item.raw_text for item in results], ["AS-2007", "AS-2008"])

    def test_hybrid_decoder_tries_roi_then_enhancement_then_full_frame_fallback(self) -> None:
        config = VisionConfig.from_overrides(
            preprocess={"enable_roi": True, "roi": (0.0, 0.0, 0.45, 1.0), "roi_fallback_to_full_frame": True},
        )
        frame = FrameData(
            frame_id="frame-stage-1",
            image=combine_images_horizontally(make_blank_image(width=220, height=220), make_qr_image("AS-2009")),
            timestamp=1700000006,
            source_id="webcam-0",
        )
        prepared = ImageEnhancer(config.preprocess).prepare(ROIProcessor(config.preprocess).apply(frame))
        qr_decoder = _StageAwareDecoder(
            results_by_stage={
                "full_original": [
                    DecodeResult(raw_text="AS-2009", symbology="QR", bbox=(12, 18, 40, 40), decoder_name="stage-qr")
                ]
            }
        )
        barcode_decoder = _StageAwareDecoder(results_by_stage={})
        decoder = HybridDecoder(config.decode, qr_decoder=qr_decoder, barcode_decoder=barcode_decoder)

        results = decoder.decode(prepared)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].raw_text, "AS-2009")
        self.assertEqual(qr_decoder.calls[:3], ["roi_original", "roi_enhanced", "full_original"])
        self.assertEqual(results[0].extra["decode_stage"], "full_original")
        self.assertTrue(results[0].extra["decode_used_fallback"])

    def test_hybrid_decoder_can_succeed_on_enhanced_stage_before_full_fallback(self) -> None:
        config = VisionConfig.from_overrides(
            preprocess={"enable_roi": True, "roi": (0.0, 0.0, 0.45, 1.0), "roi_fallback_to_full_frame": True},
        )
        frame = FrameData(
            frame_id="frame-stage-2",
            image=combine_images_horizontally(make_blank_image(width=220, height=220), make_qr_image("AS-2010")),
            timestamp=1700000007,
            source_id="webcam-0",
        )
        prepared = ImageEnhancer(config.preprocess).prepare(ROIProcessor(config.preprocess).apply(frame))
        qr_decoder = _StageAwareDecoder(
            results_by_stage={
                "roi_enhanced": [
                    DecodeResult(raw_text="AS-2010", symbology="QR", bbox=(8, 10, 24, 24), decoder_name="stage-qr")
                ]
            }
        )
        decoder = HybridDecoder(config.decode, qr_decoder=qr_decoder, barcode_decoder=_StageAwareDecoder(results_by_stage={}))

        results = decoder.decode(prepared)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].extra["decode_stage"], "roi_enhanced")
        self.assertEqual(qr_decoder.calls, ["roi_original", "roi_enhanced"])

    def test_hybrid_decoder_translates_roi_bbox_back_to_full_frame_coordinates(self) -> None:
        candidate_image = make_qr_image("AS-2011")
        frame = FrameData(
            frame_id="frame-stage-3",
            image=make_blank_image(width=320, height=240),
            timestamp=1700000008,
            source_id="webcam-0",
            extra={
                DECODE_CANDIDATES_EXTRA_KEY: [
                    {
                        "name": "roi_original",
                        "image": candidate_image,
                        "bbox_offset": (15, 20),
                        "origin": "roi",
                        "variant": "original",
                        "preprocess_steps": ("roi",),
                    }
                ],
                PRIMARY_DECODE_CANDIDATE_EXTRA_KEY: "roi_original",
            },
        )
        qr_decoder = _StageAwareDecoder(
            results_by_stage={
                "roi_original": [
                    DecodeResult(raw_text="AS-2011", symbology="QR", bbox=(1, 2, 30, 40), decoder_name="stage-qr")
                ]
            }
        )
        decoder = HybridDecoder(DecodeConfig(), qr_decoder=qr_decoder, barcode_decoder=_StageAwareDecoder(results_by_stage={}))

        results = decoder.decode(frame)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].bbox, (16, 22, 30, 40))


if __name__ == "__main__":
    unittest.main()
