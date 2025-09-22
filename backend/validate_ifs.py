"""CLI entry point for validating IFs folders.

This script wraps the ``validate_ifs_folder`` helper used by the FastAPI service
so it can be executed directly from the command line. It accepts a single folder
path argument, runs the validation and prints the resulting JSON payload to
stdout.
"""

from __future__ import annotations

import json
import os
import sys
from types import SimpleNamespace
from typing import Any

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

try:
    from app.ifscheck import validate_ifs_folder  # type: ignore[attr-defined]  # noqa: E402
except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency shim
    if exc.name != 'fastapi':
        raise

    class _APIRouter:  # Minimal stub for fastapi.APIRouter
        def post(self, *_args, **_kwargs):
            def decorator(func):
                return func

            return decorator

    sys.modules['fastapi'] = SimpleNamespace(APIRouter=_APIRouter)
    from app.ifscheck import validate_ifs_folder  # type: ignore[attr-defined]  # noqa: E402


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        payload: dict[str, Any] = {
            "valid": False,
            "missingFiles": ["No folder path provided"],
        }
        print(json.dumps(payload))
        return 1

    folder_path = argv[1]

    try:
        result = validate_ifs_folder(folder_path)
    except Exception:  # pragma: no cover - surface any unexpected error
        payload = {"valid": False, "missingFiles": ["Python error"]}
        print(json.dumps(payload))
        return 1

    print(json.dumps(result))
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry point
    sys.exit(main(sys.argv))
