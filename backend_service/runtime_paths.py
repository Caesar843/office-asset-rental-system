from __future__ import annotations

import sys
from pathlib import Path


def ensure_project_paths() -> None:
    """
    Keep the current sliced repo layout directly runnable.

    backend_service is not packaged as an installable module, and its runtime
    depends on sibling serial_comm sources. This helper limits itself to adding
    only the two directories that are required for the current flat import
    layout.
    """

    current_dir = Path(__file__).resolve().parent
    serial_comm_dir = current_dir.parent / "serial_comm"

    for path in (current_dir, serial_comm_dir):
        path_text = str(path)
        if path_text not in sys.path:
            sys.path.insert(0, path_text)


ensure_project_paths()
