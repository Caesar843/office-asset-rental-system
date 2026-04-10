from __future__ import annotations

import unittest
from unittest.mock import patch

from capture.base import (
    CaptureConnectionLostError,
    CaptureDependencyError,
    CaptureOpenError,
    CaptureStreamEnded,
    CaptureTemporaryReadError,
)
from capture.ip_camera import IPCameraFrameSource
from capture.video_file import VideoFileFrameSource
from capture.webcam import WebcamFrameSource
import capture.ip_camera as ip_camera_module
import capture.webcam as webcam_module
from tests._fixtures import make_blank_image, make_qr_image, remove_temp_file, save_temp_video


class _ClosedCapture:
    def isOpened(self) -> bool:
        return False

    def release(self) -> None:
        return None


class _ReadFailCapture:
    def __init__(self) -> None:
        self._released = False

    def isOpened(self) -> bool:
        return True

    def read(self):
        return (False, None)

    def release(self) -> None:
        self._released = True

    def set(self, prop, value) -> bool:
        del prop, value
        return True


class _LostCapture(_ReadFailCapture):
    def __init__(self) -> None:
        super().__init__()
        self._opened_once = False

    def isOpened(self) -> bool:
        if not self._opened_once:
            self._opened_once = True
            return True
        return False


class _FakeClosedCV2:
    @staticmethod
    def VideoCapture(source):
        del source
        return _ClosedCapture()


class _FakeReadFailCV2:
    @staticmethod
    def VideoCapture(source):
        del source
        return _ReadFailCapture()


class _FakeLostCV2:
    @staticmethod
    def VideoCapture(source):
        del source
        return _LostCapture()


class CaptureTests(unittest.TestCase):
    def test_webcam_reports_missing_cv2_dependency(self) -> None:
        with patch.object(webcam_module, "cv2", None):
            source = WebcamFrameSource(source_value=0, source_id="webcam-0")
            with self.assertRaises(CaptureDependencyError) as ctx:
                source.open()
        self.assertIn("opencv-python", str(ctx.exception))

    def test_webcam_reports_open_failure(self) -> None:
        with patch.object(webcam_module, "cv2", _FakeClosedCV2()):
            source = WebcamFrameSource(source_value=99, source_id="webcam-0")
            with self.assertRaises(CaptureOpenError) as ctx:
                source.open()
        self.assertIn("unable to open webcam source", str(ctx.exception))

    def test_webcam_reports_read_failure(self) -> None:
        with patch.object(webcam_module, "cv2", _FakeReadFailCV2()):
            source = WebcamFrameSource(source_value=0, source_id="webcam-0")
            source.open()
            try:
                with self.assertRaises(CaptureTemporaryReadError) as ctx:
                    source.read()
            finally:
                source.close()
        self.assertIn("temporary webcam frame read failure", str(ctx.exception))

    def test_webcam_reports_connection_lost(self) -> None:
        with patch.object(webcam_module, "cv2", _FakeLostCV2()):
            source = WebcamFrameSource(source_value=0, source_id="webcam-0")
            source.open()
            try:
                with self.assertRaises(CaptureConnectionLostError) as ctx:
                    source.read()
            finally:
                source.close()
        self.assertIn("became unavailable", str(ctx.exception))
        self.assertTrue(source.supports_reconnect())

    def test_ip_camera_reports_open_failure(self) -> None:
        with patch.object(ip_camera_module, "cv2", _FakeClosedCV2()):
            source = IPCameraFrameSource(stream_url="rtsp://invalid", source_id="ip-1")
            with self.assertRaises(CaptureOpenError) as ctx:
                source.open()
        self.assertIn("unable to open IP camera stream", str(ctx.exception))
        self.assertTrue(source.supports_reconnect())

    def test_ip_camera_reports_temporary_read_failure(self) -> None:
        with patch.object(ip_camera_module, "cv2", _FakeReadFailCV2()):
            source = IPCameraFrameSource(stream_url="rtsp://demo", source_id="ip-1")
            source.open()
            try:
                with self.assertRaises(CaptureTemporaryReadError):
                    source.read()
            finally:
                source.close()

    def test_ip_camera_reports_connection_lost(self) -> None:
        with patch.object(ip_camera_module, "cv2", _FakeLostCV2()):
            source = IPCameraFrameSource(stream_url="rtsp://demo", source_id="ip-1")
            source.open()
            try:
                with self.assertRaises(CaptureConnectionLostError):
                    source.read()
            finally:
                source.close()

    def test_video_file_reads_multiple_frames_then_reaches_eof(self) -> None:
        video_path = save_temp_video([make_qr_image("AS-7001"), make_blank_image()])
        try:
            source = VideoFileFrameSource(video_path=video_path, source_id="video-1")
            source.open()
            try:
                first = source.read()
                second = source.read()
                with self.assertRaises(CaptureStreamEnded):
                    source.read()
            finally:
                source.close()
        finally:
            remove_temp_file(video_path)

        self.assertEqual(first.frame_id, "video-1-1")
        self.assertEqual(second.frame_id, "video-1-2")
        self.assertEqual(first.source_id, "video-1")
        self.assertTrue(source.is_finite())
        self.assertFalse(source.supports_reconnect())

    def test_video_file_reports_invalid_path(self) -> None:
        source = VideoFileFrameSource(video_path="does-not-exist.avi", source_id="video-1")
        with self.assertRaises(CaptureOpenError) as ctx:
            source.open()
        self.assertIn("video file not found", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
