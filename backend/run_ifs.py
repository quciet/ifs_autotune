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
import shutil
import subprocess
import sys
import uuid
from typing import List, Tuple


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch IFs with custom arguments.")
    parser.add_argument("--ifs-root", required=True, help="Path to the IFs installation root.")
    parser.add_argument("--end-year", type=int, default=2050, help="Final simulation year.")
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where IFs artifacts should be written.",
    )
    parser.add_argument(
        "--base-year",
        type=int,
        default=None,
        help="Base year used for the simulation run.",
    )
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
    output_dir = os.path.abspath(args.output_dir)
    base_year = args.base_year
    working_dir = os.path.join(ifs_root, "net8")

    progress_path = os.path.join(ifs_root, "RUNFILES", "progress.txt")

    try:
        process = subprocess.Popen(
            command,
            cwd=working_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except Exception as exc:  # pragma: no cover - surface unexpected spawn errors
        payload = {
            "status": "error",
            "message": str(exc),
        }
        print(json.dumps(payload))
        return 1

    assert process.stdout is not None  # for the type checker
    try:
        for raw_line in process.stdout:
            # Re-emit the IFs output so the desktop shell can relay progress updates
            # to the UI in real time.
            sys.stdout.write(raw_line)
            sys.stdout.flush()
    finally:
        process.stdout.close()

    return_code = process.wait()

    if return_code != 0:
        payload = {
            "status": "error",
            "message": f"IFs exited with code {return_code}",
        }
        print(json.dumps(payload))
        return 1

    try:
        end_year, w_gdp = _read_progress_summary(progress_path)
    except FileNotFoundError:
        payload = {
            "status": "error",
            "message": "progress.txt was not found after the IFs run finished.",
        }
        print(json.dumps(payload))
        return 1
    except ValueError as exc:
        payload = {
            "status": "error",
            "message": str(exc),
        }
        print(json.dumps(payload))
        return 1

    if end_year != args.end_year:
        payload = {
            "status": "error",
            "message": (
                f"Progress file reports end year {end_year}, expected {args.end_year}."
            ),
        }
        print(json.dumps(payload))
        return 1

    try:
        payload = _prepare_run_artifacts(
            ifs_root=ifs_root,
            output_dir=output_dir,
            base_year=base_year,
            end_year=end_year,
            w_gdp=w_gdp,
        )
    except FileNotFoundError:
        error_payload = {
            "status": "error",
            "message": "Working.run.db was not found after the IFs run finished.",
        }
        print(json.dumps(error_payload))
        return 1
    except OSError as exc:
        error_payload = {
            "status": "error",
            "message": str(exc),
        }
        print(json.dumps(error_payload))
        return 1

    try:
        _reset_working_database(ifs_root)
    except FileNotFoundError as exc:
        error_payload = {
            "status": "error",
            "message": f"Unable to reset working database: {exc}",
        }
        print(json.dumps(error_payload))
        return 1
    except OSError as exc:
        error_payload = {
            "status": "error",
            "message": str(exc),
        }
        print(json.dumps(error_payload))
        return 1

    print(json.dumps(payload))
    return 0


def _prepare_run_artifacts(
    *,
    ifs_root: str,
    output_dir: str,
    base_year: int | None,
    end_year: int,
    w_gdp: float,
) -> dict:
    runfiles_dir = os.path.join(os.path.abspath(ifs_root), "RUNFILES")
    source_db = os.path.join(runfiles_dir, "Working.run.db")

    if not os.path.exists(source_db):
        raise FileNotFoundError(source_db)

    os.makedirs(output_dir, exist_ok=True)

    model_id = uuid.uuid4().hex
    destination_db = os.path.join(output_dir, f"model_{model_id}.db")
    shutil.copy2(source_db, destination_db)

    metadata_path = os.path.join(output_dir, f"model_{model_id}.json")
    metadata_contents = {
        "model_id": model_id,
        "base_year": base_year,
        "end_year": end_year,
        "output_file": destination_db,
        "WGDP": w_gdp,
    }

    with open(metadata_path, "w", encoding="utf-8") as metadata_file:
        json.dump(metadata_contents, metadata_file, indent=2)

    return {
        "status": "success",
        "model_id": model_id,
        "base_year": base_year,
        "end_year": end_year,
        "w_gdp": w_gdp,
        "output_file": destination_db,
        "metadata_file": metadata_path,
    }


def _reset_working_database(ifs_root: str) -> None:
    runfiles_dir = os.path.join(os.path.abspath(ifs_root), "RUNFILES")
    base_run = os.path.join(runfiles_dir, "IFsBase.run.db")
    working_run = os.path.join(runfiles_dir, "Working.run.db")

    if not os.path.exists(base_run):
        raise FileNotFoundError(base_run)

    shutil.copy2(base_run, working_run)


def _read_progress_summary(progress_path: str) -> Tuple[int, float]:
    try:
        with open(progress_path, "r", encoding="utf-8") as progress_file:
            last_line: str | None = None
            for raw_line in progress_file:
                stripped = raw_line.strip()
                if stripped:
                    last_line = stripped
    except FileNotFoundError:
        raise

    if not last_line:
        raise ValueError("progress.txt is empty and cannot be parsed.")

    parts = [segment.strip() for segment in last_line.split(",") if segment.strip()]
    if len(parts) < 2:
        raise ValueError("The final line in progress.txt is malformed.")

    try:
        year = int(parts[0])
    except ValueError as exc:
        raise ValueError("Unable to parse the end year from progress.txt.") from exc

    try:
        w_gdp = float(parts[-1])
    except ValueError as exc:
        raise ValueError("Unable to parse WGDP from progress.txt.") from exc

    return year, w_gdp


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
