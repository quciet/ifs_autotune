"""Helper script to launch IFs runs from the desktop shell.

This module exposes a thin CLI wrapper around the IFs executable so the
Electron backend can trigger model runs without blocking the event loop. It
accepts a handful of parameters that mirror the ones used by BIGPOPA and
prints a JSON payload describing the outcome once the process exits.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import List


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch IFs with custom arguments.")
    parser.add_argument("--ifs-root", required=True, help="Path to the IFs installation root.")
    parser.add_argument("--end-year", type=int, default=2050, help="Final simulation year.")
    parser.add_argument("--start-token", default="5", help="Starting token passed to IFs.")
    parser.add_argument("--log", default="jrs.txt", help="Log file name to pass to IFs.")
    parser.add_argument(
        "--websessionid",
        default="qsdqsqsdqsdqsdqs",
        help="Session identifier forwarded to IFs.",
    )
    return parser


def build_command(args: argparse.Namespace) -> List[str]:
    ifs_root = os.path.abspath(args.ifs_root)
    executable = os.path.join(ifs_root, "net8", "ifs.exe")
    command = [
        executable,
        str(args.start_token),
        str(args.end_year),
        "-1",
        "true",
        "true",
        "1",
        "false",
        "--log",
        args.log,
        "--websessionid",
        args.websessionid,
    ]
    return command


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    command = build_command(args)
    ifs_root = os.path.abspath(args.ifs_root)
    working_dir = os.path.join(ifs_root, "net8")

    try:
        process = subprocess.Popen(command, cwd=working_dir)
    except Exception as exc:  # pragma: no cover - surface unexpected spawn errors
        payload = {
            "status": "error",
            "message": str(exc),
        }
        print(json.dumps(payload))
        return 1

    return_code = process.wait()

    payload = {
        "end_year": args.end_year,
        "log": args.log,
        "session_id": args.websessionid,
        "status": "ok" if return_code == 0 else "error",
    }

    if return_code != 0:
        payload["message"] = f"IFs exited with code {return_code}"

    print(json.dumps(payload))
    return 0 if return_code == 0 else 1


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
