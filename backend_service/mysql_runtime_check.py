from __future__ import annotations

import json
import sys

from db_repository import MySQLTransactionRepository


def main() -> int:
    try:
        repository = MySQLTransactionRepository.from_env()
    except Exception as exc:
        payload = {
            "requested_repository_mode": "mysql",
            "repository_mode": "mysql",
            "repository_ready": False,
            "repository_status": "error",
            "startup_error": str(exc),
            "repository_details": {
                "backend": "mysql",
                "ready": False,
                "status": "error",
                "warnings": [],
                "errors": [str(exc)],
                "details": {"exception_type": type(exc).__name__},
            },
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 1

    probe = repository.probe()
    payload = {
        "requested_repository_mode": "mysql",
        "repository_mode": "mysql",
        "repository_ready": probe.ready,
        "repository_status": probe.status,
        "startup_error": None if probe.ready else "; ".join(probe.errors),
        "repository_details": probe.to_dict(),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if probe.ready else 1


if __name__ == "__main__":
    sys.exit(main())
