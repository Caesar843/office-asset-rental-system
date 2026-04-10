from __future__ import annotations

import io
from pathlib import Path
from tempfile import NamedTemporaryFile

import barcode
import cv2
import numpy as np
import qrcode
from barcode.writer import ImageWriter
from PIL import Image


def make_qr_image(text: str) -> np.ndarray:
    image = qrcode.make(text).convert("RGB")
    rgb = np.array(image)
    return rgb[:, :, ::-1].copy()


def make_code128_image(text: str) -> np.ndarray:
    code128 = barcode.get("code128", text, writer=ImageWriter())
    buffer = io.BytesIO()
    code128.write(
        buffer,
        options={
            "write_text": False,
            "module_width": 0.28,
            "module_height": 35,
            "quiet_zone": 4,
        },
    )
    buffer.seek(0)
    rgb = np.array(Image.open(buffer).convert("RGB"))
    return rgb[:, :, ::-1].copy()


def make_blank_image(width: int = 320, height: int = 240) -> np.ndarray:
    return np.full((height, width, 3), 255, dtype=np.uint8)


def combine_images_horizontally(*images: np.ndarray) -> np.ndarray:
    heights = [image.shape[0] for image in images]
    widths = [image.shape[1] for image in images]
    canvas = np.full((max(heights), sum(widths) + 20 * (len(images) - 1), 3), 255, dtype=np.uint8)
    x_offset = 0
    for image in images:
        h, w = image.shape[:2]
        y_offset = (canvas.shape[0] - h) // 2
        canvas[y_offset : y_offset + h, x_offset : x_offset + w] = image
        x_offset += w + 20
    return canvas


def blur_image(image: np.ndarray, kernel_size: int = 11) -> np.ndarray:
    return cv2.GaussianBlur(image, (kernel_size, kernel_size), 0)


def save_temp_image(image: np.ndarray) -> str:
    rgb = image[:, :, ::-1]
    with NamedTemporaryFile(delete=False, suffix=".png") as handle:
        Image.fromarray(rgb).save(handle.name)
        return handle.name


def save_temp_video(frames: list[np.ndarray], fps: int = 5) -> str:
    if not frames:
        raise ValueError("frames cannot be empty")
    height, width = frames[0].shape[:2]
    with NamedTemporaryFile(delete=False, suffix=".avi") as handle:
        path = handle.name
    writer = cv2.VideoWriter(path, cv2.VideoWriter_fourcc(*"MJPG"), float(fps), (width, height))
    if not writer.isOpened():
        raise RuntimeError(f"failed to create temporary video writer: {path}")
    try:
        for frame in frames:
            if frame.ndim == 2:
                frame_to_write = cv2.cvtColor(frame, cv2.COLOR_GRAY2BGR)
            else:
                frame_to_write = frame
            if frame_to_write.shape[:2] != (height, width):
                frame_to_write = cv2.resize(frame_to_write, (width, height))
            writer.write(frame_to_write)
    finally:
        writer.release()
    return path


def remove_temp_file(path: str) -> None:
    Path(path).unlink(missing_ok=True)
