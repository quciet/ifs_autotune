"""CLI entry point for validating IFs folders.

This script wraps the ``validate_ifs_folder`` helper used by the FastAPI service
so it can be executed directly from the command line. It accepts a single folder
path argument, runs the validation and prints the resulting JSON payload to
stdout.
"""

from __future__ import annotations

import argparse
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
    parser = argparse.ArgumentParser(description="Validate an IFs installation folder")
    parser.add_argument("path", nargs="?")
    parser.add_argument("--output-path", dest="output_path")
    parser.add_argument("--input-file", dest="input_file")

    args = parser.parse_args(argv[1:])

    if not args.path:
        payload: dict[str, Any] = {
            "valid": False,
            "missingFiles": ["No folder path provided"],
        }
        print(json.dumps(payload))
        return 1

    try:
        result = validate_ifs_folder(
            args.path,
            output_path=args.output_path,
            input_file=args.input_file,
        )
    except Exception:  # pragma: no cover - surface any unexpected error
        payload = {"valid": False, "missingFiles": ["Python error"]}
        print(json.dumps(payload))
        return 1

    print(json.dumps(result))
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry point
    sys.exit(main(sys.argv))
