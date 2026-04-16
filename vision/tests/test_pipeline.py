from __future__ import annotations

import json
import unittest

from app.config import VisionConfig
from app.runner import _build_preprocessor
from app.pipeline import VisionPipeline
from capture.base import CaptureConnectionLostError, CaptureStreamEnded, CaptureTemporaryReadError, FrameSource
from capture.mock import StaticFrameSource
from decoder.base import DecoderError
from decoder.hybrid_decoder import HybridDecoder
from decoder.stub import StaticDecoder
from gateway.api_client import APIClient, TransportResponse
from models.decode_result import DecodeResult
from models.frame import (
    DECODE_CANDIDATE_SUMMARY_EXTRA_KEY,
    DECODE_CANDIDATES_EXTRA_KEY,
    FrameData,
)
from models.scan_result import ScanResult
from parser.asset_id_parser import AssetIdParser
from parser.deduplicator import ScanResultDeduplicator
from parser.normalizer import FormalScanResultBuilder, ScanResultNormalizer
from parser.stub import MockScanResultBuilder
from tests._fixtures import blur_image, combine_images_horizontally, make_blank_image, make_qr_image


class _SequenceSource(FrameSource):
    def __init__(self, frames: list[FrameData]) -> None:
        self._frames = list(frames)
        self._index = 0
        self._open = False

    def open(self) -> None:
        self._open = True
        self._index = 0

    def read(self) -> FrameData:
        if not self._open:
            raise RuntimeError("sequence source is not open")
        if self._index >= len(self._frames):
            raise CaptureStreamEnded("sequence source reached end of stream")
        frame = self._frames[self._index]
        self._index += 1
        return frame

    def close(self) -> None:
        self._open = False


class _TemporaryReadFailSource(FrameSource):
    def open(self) -> None:
        return None

    def read(self) -> FrameData:
        raise CaptureTemporaryReadError("temporary read issue")

    def close(self) -> None:
        return None


class _ConnectionLostSource(FrameSource):
    def open(self) -> None:
        return None

    def read(self) -> FrameData:
        raise CaptureConnectionLostError("connection lost")

    def close(self) -> None:
        return None


class _ExplodingDecoder:
    def decode(self, frame: FrameData):
        del frame
        raise DecoderError("forced decode failure")


class PipelineTests(unittest.TestCase):
    def test_minimal_pipeline_can_submit_one_result(self) -> None:
        frame = FrameData(
            frame_id="frame-1",
            image=b"x",
            timestamp=1700000000,
            source_id="webcam-0",
            width=1,
            height=1,
            channel_count=1,
        )
        source = StaticFrameSource(frame)
        decoder = StaticDecoder(results=[DecodeResult(raw_text="AS-9001", symbology="QR", confidence=0.99)])
        builder = MockScanResultBuilder(asset_id="AS-9001")
        client = APIClient(
            VisionConfig().gateway,
            transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                status_code=200,
                body=json.dumps(
                    {
                        "success": True,
                        "code": "OK",
                        "message": "accepted",
                        "asset_id": "AS-9001",
                        "extra": {"server": "pipeline-test"},
                    }
                ).encode("utf-8"),
            ),
        )
        pipeline = VisionPipeline(source=source, decoder=decoder, scan_result_builder=builder, api_client=client)

        with source:
            output = pipeline.run_once()

        self.assertEqual(output.status, "submitted")
        self.assertIsNotNone(output.frame)
        self.assertIsNotNone(output.scan_result)
        self.assertIsNotNone(output.submit_request)
        self.assertIsNotNone(output.submit_result)
        self.assertEqual(output.submit_request.asset_id, "AS-9001")
        self.assertIsInstance(output.submit_request.frame_time, int)
        self.assertEqual(output.submit_request.frame_time, 1700000000)
        self.assertTrue(output.submit_result.business_success)

    def test_duplicate_result_is_not_submitted(self) -> None:
        transport_called = {"count": 0}

        def transport(url, payload, timeout_sec, headers):
            transport_called["count"] += 1
            return TransportResponse(
                status_code=200,
                body=json.dumps(
                    {
                        "success": True,
                        "code": "OK",
                        "message": "accepted",
                        "asset_id": "AS-9002",
                        "extra": {"server": "pipeline-test"},
                    }
                ).encode("utf-8"),
            )

        frame = FrameData(
            frame_id="frame-2",
            image=b"x",
            timestamp=1700000001,
            source_id="webcam-0",
        )
        source = StaticFrameSource(frame)
        decoder = StaticDecoder(results=[DecodeResult(raw_text="AS-9002", symbology="QR")])
        builder = MockScanResultBuilder(asset_id="AS-9002", is_duplicate=True)
        client = APIClient(VisionConfig().gateway, transport=transport)
        pipeline = VisionPipeline(source=source, decoder=decoder, scan_result_builder=builder, api_client=client)

        with source:
            output = pipeline.run_once()

        self.assertEqual(output.status, "skipped_duplicate")
        self.assertIsNotNone(output.scan_result)
        self.assertTrue(output.scan_result.is_duplicate)
        self.assertIsNone(output.submit_request)
        self.assertIsNone(output.submit_result)
        self.assertEqual(transport_called["count"], 0)

    def test_invalid_scan_result_object_does_not_enter_submit_chain(self) -> None:
        class InvalidBuilder:
            def build(self, frame, decode_results):
                del frame, decode_results
                return {"not": "a_scan_result"}

        frame = FrameData(
            frame_id="frame-3",
            image=b"x",
            timestamp=1700000002,
            source_id="webcam-0",
        )
        source = StaticFrameSource(frame)
        decoder = StaticDecoder(results=[DecodeResult(raw_text="AS-9003", symbology="QR")])
        client = APIClient(
            VisionConfig().gateway,
            transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                status_code=200,
                body=json.dumps(
                    {
                        "success": True,
                        "code": "OK",
                        "message": "accepted",
                        "asset_id": "AS-9003",
                        "extra": {},
                    }
                ).encode("utf-8"),
            ),
        )
        pipeline = VisionPipeline(source=source, decoder=decoder, scan_result_builder=InvalidBuilder(), api_client=client)

        with source:
            output = pipeline.run_once()

        self.assertEqual(output.status, "failed")
        self.assertEqual(output.error.error_code, "INVALID_SCAN_RESULT")
        self.assertIsNone(output.submit_request)
        self.assertIsNone(output.submit_result)

    def test_invalid_scan_result_instance_is_rejected_before_submit(self) -> None:
        class InvalidScanResultBuilder:
            def build(self, frame, decode_results):
                del decode_results
                result = object.__new__(ScanResult)
                object.__setattr__(result, "asset_id", "bad asset")
                object.__setattr__(result, "raw_text", "bad asset")
                object.__setattr__(result, "symbology", "QR")
                object.__setattr__(result, "source_id", frame.source_id)
                object.__setattr__(result, "frame_time", 1700000000123)
                object.__setattr__(result, "frame_id", frame.frame_id)
                object.__setattr__(result, "bbox", None)
                object.__setattr__(result, "confidence", 0.9)
                object.__setattr__(result, "is_duplicate", False)
                object.__setattr__(result, "duplicate_reason", None)
                object.__setattr__(result, "extra", {})
                return result

        transport_called = {"count": 0}

        def transport(url, payload, timeout_sec, headers):
            del url, payload, timeout_sec, headers
            transport_called["count"] += 1
            return TransportResponse(status_code=200, body=b"{}")

        frame = FrameData(frame_id="frame-invalid-1", image=b"x", timestamp=1700000002, source_id="webcam-0")
        pipeline = VisionPipeline(
            source=StaticFrameSource(frame),
            decoder=StaticDecoder(results=[DecodeResult(raw_text="AS-9003", symbology="QR")]),
            scan_result_builder=InvalidScanResultBuilder(),
            api_client=APIClient(VisionConfig().gateway, transport=transport),
        )

        with pipeline._source:
            output = pipeline.run_once()

        self.assertEqual(output.status, "failed")
        self.assertEqual(output.error.error_code, "INVALID_SUBMIT_REQUEST")
        self.assertEqual(output.error.extra["classifier"], "parse_fail")
        self.assertIn("invalid_asset_id", output.error.extra["validation_errors"])
        self.assertIn("invalid_value:frame_time", output.error.extra["validation_errors"])
        self.assertEqual(transport_called["count"], 0)

    def test_live_pipeline_real_decode_parse_and_submit_succeeds(self) -> None:
        frame = FrameData(
            frame_id="live-frame-1",
            image=make_qr_image("AS-9101"),
            timestamp=1700000002,
            source_id="live-cam-1",
        )
        config = VisionConfig.from_overrides(
            runtime={"run_mode": "live"},
            preprocess={"enable_quality_gate": False},
        )
        source = StaticFrameSource(frame)
        decoder = HybridDecoder(config.decode)
        builder = FormalScanResultBuilder(
            asset_id_parser=AssetIdParser(),
            normalizer=ScanResultNormalizer(),
            deduplicator=ScanResultDeduplicator(config.dedup),
        )
        client = APIClient(
            config.gateway,
            transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                status_code=200,
                body=json.dumps(
                    {
                        "success": True,
                        "code": "SCAN_ACCEPTED",
                        "message": "accepted",
                        "asset_id": "AS-9101",
                        "extra": {"server": "pipeline-live"},
                    }
                ).encode("utf-8"),
            ),
        )
        pipeline = VisionPipeline(
            source=source,
            decoder=decoder,
            scan_result_builder=builder,
            api_client=client,
            preprocessor=_build_preprocessor(config),
        )

        with source:
            output = pipeline.run_once()

        self.assertEqual(output.status, "submitted")
        self.assertEqual(output.submit_request.asset_id, "AS-9101")
        self.assertEqual(output.submit_result.code, "SCAN_ACCEPTED")

    def test_live_pipeline_quality_failure_is_collected(self) -> None:
        blurred = blur_image(make_qr_image("AS-9102"), kernel_size=31)
        frame = FrameData(frame_id="live-frame-2", image=blurred, timestamp=1700000003, source_id="live-cam-1")
        config = VisionConfig.from_overrides(
            runtime={"run_mode": "live"},
            preprocess={"laplacian_variance_threshold": 150.0, "retry_with_enhancement": False},
        )
        pipeline = VisionPipeline(
            source=StaticFrameSource(frame),
            decoder=HybridDecoder(config.decode),
            scan_result_builder=FormalScanResultBuilder(
                asset_id_parser=AssetIdParser(),
                normalizer=ScanResultNormalizer(),
                deduplicator=ScanResultDeduplicator(config.dedup),
            ),
            api_client=APIClient(
                config.gateway,
                transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                    status_code=200,
                    body=b"{}",
                ),
            ),
            preprocessor=_build_preprocessor(config),
        )

        with pipeline._source:
            output = pipeline.run_once()

        self.assertEqual(output.status, "failed")
        self.assertEqual(output.error.stage, "preprocess")
        self.assertEqual(output.error.error_code, "LOW_QUALITY")

    def test_live_pipeline_quality_failure_includes_reason_details(self) -> None:
        blurred = blur_image(make_qr_image("AS-9102A"), kernel_size=31)
        frame = FrameData(frame_id="live-frame-2a", image=blurred, timestamp=1700000003, source_id="live-cam-1")
        config = VisionConfig.from_overrides(
            runtime={"run_mode": "live"},
            preprocess={"laplacian_variance_threshold": 150.0, "retry_with_enhancement": False},
        )
        pipeline = VisionPipeline(
            source=StaticFrameSource(frame),
            decoder=HybridDecoder(config.decode),
            scan_result_builder=FormalScanResultBuilder(
                asset_id_parser=AssetIdParser(),
                normalizer=ScanResultNormalizer(),
                deduplicator=ScanResultDeduplicator(config.dedup),
            ),
            api_client=APIClient(
                config.gateway,
                transport=lambda url, payload, timeout_sec, headers: TransportResponse(status_code=200, body=b"{}"),
            ),
            preprocessor=_build_preprocessor(config),
        )

        with pipeline._source:
            output = pipeline.run_once()

        self.assertEqual(output.status, "failed")
        self.assertEqual(output.error.error_code, "LOW_QUALITY")
        self.assertIn("selected_candidate", output.error.extra)
        self.assertIn("candidates", output.error.extra)
        self.assertEqual(output.error.extra["classifier"], "low_quality")

    def test_live_pipeline_asset_id_parse_failure_is_collected(self) -> None:
        frame = FrameData(
            frame_id="live-frame-3",
            image=make_qr_image("NOT_AN_ASSET"),
            timestamp=1700000004,
            source_id="live-cam-1",
        )
        config = VisionConfig.from_overrides(
            runtime={"run_mode": "live"},
            preprocess={"enable_quality_gate": False},
        )
        pipeline = VisionPipeline(
            source=StaticFrameSource(frame),
            decoder=HybridDecoder(config.decode),
            scan_result_builder=FormalScanResultBuilder(
                asset_id_parser=AssetIdParser(),
                normalizer=ScanResultNormalizer(),
                deduplicator=ScanResultDeduplicator(config.dedup),
            ),
            api_client=APIClient(
                config.gateway,
                transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                    status_code=200,
                    body=b"{}",
                ),
            ),
            preprocessor=_build_preprocessor(config),
        )

        with pipeline._source:
            output = pipeline.run_once()

        self.assertEqual(output.status, "failed")
        self.assertEqual(output.error.error_code, "ASSET_ID_PARSE_FAILED")
        self.assertEqual(output.error.extra["classifier"], "parse_fail")

    def test_pipeline_distinguishes_visual_transport_and_business_failures(self) -> None:
        visual_config = VisionConfig.from_overrides(runtime={"run_mode": "live"}, preprocess={"enable_quality_gate": False})
        visual_pipeline = VisionPipeline(
            source=StaticFrameSource(
                FrameData(
                    frame_id="visual-fail-1",
                    image=make_blank_image(),
                    timestamp=1700000004,
                    source_id="live-cam-1",
                )
            ),
            decoder=HybridDecoder(visual_config.decode),
            scan_result_builder=FormalScanResultBuilder(
                asset_id_parser=AssetIdParser(),
                normalizer=ScanResultNormalizer(),
                deduplicator=ScanResultDeduplicator(visual_config.dedup),
            ),
            api_client=APIClient(visual_config.gateway, transport=lambda *args, **kwargs: TransportResponse(status_code=200, body=b"{}")),
            preprocessor=_build_preprocessor(visual_config),
        )

        with visual_pipeline._source:
            visual_output = visual_pipeline.run_once()

        self.assertEqual(visual_output.error.stage, "decoder")
        self.assertEqual(visual_output.error.extra["classifier"], "no_code")
        self.assertIsNone(visual_output.submit_request)

        transport_pipeline = VisionPipeline(
            source=StaticFrameSource(
                FrameData(
                    frame_id="transport-fail-1",
                    image=b"x",
                    timestamp=1700000010,
                    source_id="webcam-0",
                )
            ),
            decoder=StaticDecoder(results=[DecodeResult(raw_text="AS-9102B", symbology="QR")]),
            scan_result_builder=MockScanResultBuilder(asset_id="AS-9102B"),
            api_client=APIClient(VisionConfig().gateway, transport=lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("socket timed out"))),
        )

        with transport_pipeline._source:
            transport_output = transport_pipeline.run_once()

        self.assertEqual(transport_output.error.stage, "gateway")
        self.assertEqual(transport_output.error.extra["classifier"], "submit_fail")
        self.assertEqual(transport_output.error.extra["submit_failure_kind"], "transport")
        self.assertEqual(transport_output.submit_result.status, "network_error")

        business_pipeline = VisionPipeline(
            source=StaticFrameSource(
                FrameData(
                    frame_id="business-fail-1",
                    image=b"x",
                    timestamp=1700000011,
                    source_id="webcam-0",
                )
            ),
            decoder=StaticDecoder(results=[DecodeResult(raw_text="AS-9102C", symbology="QR")]),
            scan_result_builder=MockScanResultBuilder(asset_id="AS-9102C"),
            api_client=APIClient(
                VisionConfig().gateway,
                transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                    status_code=200,
                    body=json.dumps(
                        {
                            "success": False,
                            "code": "BUSY",
                            "message": "scanner busy",
                            "asset_id": payload["asset_id"],
                            "extra": {"queue_depth": 1},
                        }
                    ).encode("utf-8"),
                ),
            ),
        )

        with business_pipeline._source:
            business_output = business_pipeline.run_once()

        self.assertEqual(business_output.error.stage, "gateway")
        self.assertEqual(business_output.error.extra["classifier"], "submit_fail")
        self.assertEqual(business_output.error.extra["submit_failure_kind"], "business")
        self.assertEqual(business_output.submit_result.status, "business_error")

    def test_live_pipeline_duplicate_does_not_submit(self) -> None:
        config = VisionConfig.from_overrides(
            runtime={"run_mode": "live"},
            preprocess={"enable_quality_gate": False},
        )
        builder = FormalScanResultBuilder(
            asset_id_parser=AssetIdParser(),
            normalizer=ScanResultNormalizer(),
            deduplicator=ScanResultDeduplicator(config.dedup),
        )
        frame = FrameData(
            frame_id="dup-frame",
            image=make_qr_image("AS-9103"),
            timestamp=1700000005,
            source_id="live-cam-1",
        )
        source = StaticFrameSource(frame)
        called = {"count": 0}

        def transport(url, payload, timeout_sec, headers):
            called["count"] += 1
            return TransportResponse(
                status_code=200,
                body=json.dumps(
                    {
                        "success": True,
                        "code": "SCAN_ACCEPTED",
                        "message": "accepted",
                        "asset_id": "AS-9103",
                        "extra": {},
                    }
                ).encode("utf-8"),
            )

        pipeline = VisionPipeline(
            source=source,
            decoder=HybridDecoder(config.decode),
            scan_result_builder=builder,
            api_client=APIClient(config.gateway, transport=transport),
            preprocessor=_build_preprocessor(config),
        )

        with source:
            first = pipeline.run_once()
        with source:
            second = pipeline.run_once()

        self.assertEqual(first.status, "submitted")
        self.assertEqual(second.status, "skipped_duplicate")
        self.assertEqual(called["count"], 1)

    def test_live_pipeline_no_code_is_collected(self) -> None:
        frame = FrameData(
            frame_id="live-frame-4",
            image=make_blank_image(),
            timestamp=1700000006,
            source_id="live-cam-1",
        )
        config = VisionConfig.from_overrides(
            runtime={"run_mode": "live"},
            preprocess={"enable_quality_gate": False},
        )
        pipeline = VisionPipeline(
            source=StaticFrameSource(frame),
            decoder=HybridDecoder(config.decode),
            scan_result_builder=FormalScanResultBuilder(
                asset_id_parser=AssetIdParser(),
                normalizer=ScanResultNormalizer(),
                deduplicator=ScanResultDeduplicator(config.dedup),
            ),
            api_client=APIClient(
                config.gateway,
                transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                    status_code=200,
                    body=b"{}",
                ),
            ),
            preprocessor=_build_preprocessor(config),
        )

        with pipeline._source:
            output = pipeline.run_once()

        self.assertEqual(output.status, "failed")
        self.assertEqual(output.error.error_code, "NO_CODE")

    def test_live_pipeline_can_fallback_from_roi_to_full_frame_without_changing_submit_contract(self) -> None:
        frame = FrameData(
            frame_id="live-frame-4a",
            image=combine_images_horizontally(make_blank_image(width=220, height=220), make_qr_image("AS-9104A")),
            timestamp=1700000006,
            source_id="live-cam-1",
        )
        config = VisionConfig.from_overrides(
            runtime={"run_mode": "live"},
            preprocess={
                "enable_quality_gate": False,
                "enable_roi": True,
                "roi": (0.0, 0.0, 0.45, 1.0),
                "roi_fallback_to_full_frame": True,
            },
        )
        pipeline = VisionPipeline(
            source=StaticFrameSource(frame),
            decoder=HybridDecoder(config.decode),
            scan_result_builder=FormalScanResultBuilder(
                asset_id_parser=AssetIdParser(),
                normalizer=ScanResultNormalizer(),
                deduplicator=ScanResultDeduplicator(config.dedup),
            ),
            api_client=APIClient(
                config.gateway,
                transport=lambda url, payload, timeout_sec, headers: TransportResponse(
                    status_code=200,
                    body=json.dumps(
                        {
                            "success": True,
                            "code": "SCAN_ACCEPTED",
                            "message": "accepted",
                            "asset_id": payload["asset_id"],
                            "extra": {"server": "pipeline-roi-fallback"},
                        }
                    ).encode("utf-8"),
                ),
            ),
            preprocessor=_build_preprocessor(config),
        )

        with pipeline._source:
            output = pipeline.run_once()

        self.assertEqual(output.status, "submitted")
        self.assertEqual(output.submit_request.asset_id, "AS-9104A")
        self.assertEqual(output.submit_request.source_id, "live-cam-1")
        self.assertEqual(output.submit_request.frame_time, 1700000006)
        self.assertNotIn("request_seq", output.submit_request.to_payload())

    def test_staged_candidate_metadata_does_not_leak_into_submit_request(self) -> None:
        captured_payload: dict[str, object] = {}
        frame = FrameData(
            frame_id="live-frame-clean-extra",
            image=combine_images_horizontally(make_blank_image(width=220, height=220), make_qr_image("AS-9104B")),
            timestamp=1700000006,
            source_id="live-cam-1",
        )
        config = VisionConfig.from_overrides(
            runtime={"run_mode": "live"},
            preprocess={
                "enable_quality_gate": False,
                "enable_roi": True,
                "roi": (0.0, 0.0, 0.45, 1.0),
                "roi_fallback_to_full_frame": True,
            },
        )
        pipeline = VisionPipeline(
            source=StaticFrameSource(frame),
            decoder=HybridDecoder(config.decode),
            scan_result_builder=FormalScanResultBuilder(
                asset_id_parser=AssetIdParser(),
                normalizer=ScanResultNormalizer(),
                deduplicator=ScanResultDeduplicator(config.dedup),
            ),
            api_client=APIClient(
                config.gateway,
                transport=lambda url, payload, timeout_sec, headers: (
                    captured_payload.update(payload) or TransportResponse(
                        status_code=200,
                        body=json.dumps(
                            {
                                "success": True,
                                "code": "SCAN_ACCEPTED",
                                "message": "accepted",
                                "asset_id": payload["asset_id"],
                                "extra": {"server": "pipeline-roi-clean"},
                            }
                        ).encode("utf-8"),
                    )
                ),
            ),
            preprocessor=_build_preprocessor(config),
        )

        with pipeline._source:
            output = pipeline.run_once()

        self.assertEqual(output.status, "submitted")
        submit_extra = output.submit_request.extra
        payload_extra = captured_payload.get("extra", {})
        self.assertNotIn(DECODE_CANDIDATES_EXTRA_KEY, submit_extra)
        self.assertNotIn(DECODE_CANDIDATE_SUMMARY_EXTRA_KEY, submit_extra)
        self.assertFalse(any(str(key).startswith("_") for key in submit_extra))
        self.assertFalse(any(str(key).startswith("_") for key in payload_extra))

    def test_decoder_failure_returns_compacted_candidate_metadata_not_raw_images(self) -> None:
        config = VisionConfig.from_overrides(
            runtime={"run_mode": "live"},
            preprocess={
                "enable_quality_gate": False,
                "enable_roi": True,
                "roi": (0.0, 0.0, 0.45, 1.0),
                "roi_fallback_to_full_frame": True,
            },
        )
        pipeline = VisionPipeline(
            source=StaticFrameSource(
                FrameData(
                    frame_id="decode-fail-cleanup",
                    image=combine_images_horizontally(make_blank_image(width=220, height=220), make_qr_image("AS-9104C")),
                    timestamp=1700000007,
                    source_id="live-cam-1",
                )
            ),
            decoder=_ExplodingDecoder(),
            scan_result_builder=MockScanResultBuilder(asset_id="AS-IGNORED"),
            api_client=APIClient(config.gateway, transport=lambda *args, **kwargs: TransportResponse(status_code=200, body=b"{}")),
            preprocessor=_build_preprocessor(config),
        )

        with pipeline._source:
            output = pipeline.run_once()

        self.assertEqual(output.status, "failed")
        self.assertEqual(output.error.error_code, "DECODE_FAILED")
        self.assertNotIn(DECODE_CANDIDATES_EXTRA_KEY, output.frame.extra)
        self.assertIn(DECODE_CANDIDATE_SUMMARY_EXTRA_KEY, output.frame.extra)

    def test_pipeline_can_continue_after_failed_frame_and_then_submit(self) -> None:
        config = VisionConfig.from_overrides(
            runtime={"run_mode": "live"},
            preprocess={"enable_quality_gate": False},
        )
        source = _SequenceSource(
            [
                FrameData(frame_id="seq-1", image=make_blank_image(), timestamp=1700000007, source_id="live-cam-1"),
                FrameData(frame_id="seq-2", image=make_qr_image("AS-9104"), timestamp=1700000010, source_id="live-cam-1"),
            ]
        )
        called = {"count": 0}

        def transport(url, payload, timeout_sec, headers):
            del url, timeout_sec, headers
            called["count"] += 1
            return TransportResponse(
                status_code=200,
                body=json.dumps(
                    {
                        "success": True,
                        "code": "SCAN_ACCEPTED",
                        "message": "accepted",
                        "asset_id": payload["asset_id"],
                        "extra": {"server": "pipeline-sequence"},
                    }
                ).encode("utf-8"),
            )

        pipeline = VisionPipeline(
            source=source,
            decoder=HybridDecoder(config.decode),
            scan_result_builder=FormalScanResultBuilder(
                asset_id_parser=AssetIdParser(),
                normalizer=ScanResultNormalizer(),
                deduplicator=ScanResultDeduplicator(config.dedup),
            ),
            api_client=APIClient(config.gateway, transport=transport),
            preprocessor=_build_preprocessor(config),
        )

        with source:
            first = pipeline.run_once()
            second = pipeline.run_once()
            eof = pipeline.run_once()

        self.assertEqual(first.status, "failed")
        self.assertEqual(first.error.error_code, "NO_CODE")
        self.assertEqual(second.status, "submitted")
        self.assertEqual(second.submit_request.asset_id, "AS-9104")
        self.assertEqual(eof.status, "eof")
        self.assertEqual(called["count"], 1)

    def test_pipeline_multi_asset_conflict_frame_is_not_submitted(self) -> None:
        frame = FrameData(
            frame_id="multi-frame-1",
            image=combine_images_horizontally(make_qr_image("AS-9201"), make_qr_image("AS-9202")),
            timestamp=1700000011,
            source_id="live-cam-1",
        )
        config = VisionConfig.from_overrides(
            runtime={"run_mode": "live"},
            preprocess={"enable_quality_gate": False},
            decode={"allow_multi_decode": True},
        )
        called = {"count": 0}

        def transport(url, payload, timeout_sec, headers):
            del url, payload, timeout_sec, headers
            called["count"] += 1
            return TransportResponse(status_code=200, body=b"{}")

        pipeline = VisionPipeline(
            source=StaticFrameSource(frame),
            decoder=HybridDecoder(config.decode),
            scan_result_builder=FormalScanResultBuilder(
                asset_id_parser=AssetIdParser(),
                normalizer=ScanResultNormalizer(),
                deduplicator=ScanResultDeduplicator(config.dedup),
            ),
            api_client=APIClient(config.gateway, transport=transport),
            preprocessor=_build_preprocessor(config),
        )

        with pipeline._source:
            output = pipeline.run_once()

        self.assertEqual(output.status, "failed")
        self.assertEqual(output.error.error_code, "MULTI_RESULT_CONFLICT")
        self.assertEqual(called["count"], 0)

    def test_pipeline_marks_temporary_capture_read_failure_as_recoverable_code(self) -> None:
        pipeline = VisionPipeline(
            source=_TemporaryReadFailSource(),
            decoder=StaticDecoder(results=[]),
            scan_result_builder=MockScanResultBuilder(asset_id="AS-X"),
            api_client=APIClient(VisionConfig().gateway, transport=lambda *args, **kwargs: None),
        )

        output = pipeline.run_once()

        self.assertEqual(output.status, "failed")
        self.assertEqual(output.error.error_code, "READ_FRAME_TEMPORARY_FAILURE")

    def test_pipeline_marks_connection_lost_as_distinct_capture_code(self) -> None:
        pipeline = VisionPipeline(
            source=_ConnectionLostSource(),
            decoder=StaticDecoder(results=[]),
            scan_result_builder=MockScanResultBuilder(asset_id="AS-X"),
            api_client=APIClient(VisionConfig().gateway, transport=lambda *args, **kwargs: None),
        )

        output = pipeline.run_once()

        self.assertEqual(output.status, "failed")
        self.assertEqual(output.error.error_code, "CAPTURE_CONNECTION_LOST")


if __name__ == "__main__":
    unittest.main()
