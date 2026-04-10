"""Compatibility re-export for ScanSubmitRequest.

The formal single source of truth lives in models/scan_result.py to match the
frozen vision module directory mapping.
"""

from models.scan_result import ScanSubmitRequest

__all__ = ["ScanSubmitRequest"]
