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
import sqlite3
import subprocess
import sys
from typing import Dict, List, Tuple


# Emit a structured response for Electron consumption.
def emit_stage_response(status: str, stage: str, message: str, data: Dict[str, object]) -> None:
    payload = {
        "status": status,
        "stage": stage,
        "message": message,
        "data": data,
    }
    print(json.dumps(payload))
    sys.stdout.flush()


def ensure_bigpopa_schema(cursor: sqlite3.Cursor) -> None:
# Ensure BIGPOPA schema matches the hashed model workflow expectations.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS model_input (
            ifs_id INTEGER,
            model_id TEXT PRIMARY KEY,
            input_param TEXT,
            input_coef TEXT,
            output_set TEXT,
            FOREIGN KEY (ifs_id) REFERENCES ifs_version(ifs_id)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS model_output (
            ifs_id INTEGER,
            model_id TEXT PRIMARY KEY,
            model_status TEXT,
            fit_var TEXT,
            fit_pooled REAL,
            FOREIGN KEY (ifs_id) REFERENCES ifs_version(ifs_id),
            FOREIGN KEY (model_id) REFERENCES model_input(model_id)
        )
        """
    )


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
    parser.add_argument("--model-id", required=True, help="Existing model identifier to execute.")
    parser.add_argument("--ifs-id", required=True, type=int, help="IFs version identifier.")
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
    model_id = args.model_id
    ifs_id = args.ifs_id

    progress_path = os.path.join(ifs_root, "RUNFILES", "progress.txt")

    # Validate that the requested model configuration exists before launching IFs.
    bigpopa_db_path = os.path.join(output_dir, "bigpopa.db")
    try:
        conn_bp = sqlite3.connect(bigpopa_db_path)
    except sqlite3.Error as exc:
        emit_stage_response(
            "error",
            "run_ifs",
            "Unable to open BIGPOPA database.",
            {"bigpopa_db": bigpopa_db_path, "error": str(exc)},
        )
        return 1

    try:
        try:
            with conn_bp:
                cursor = conn_bp.cursor()
                ensure_bigpopa_schema(cursor)
                cursor.execute(
                    "SELECT 1 FROM model_input WHERE model_id = ?",
                    (model_id,),
                )
                if cursor.fetchone() is None:
                    emit_stage_response(
                        "error",
                        "run_ifs",
                        "Model configuration not found in BIGPOPA database.",
                        {"model_id": model_id},
                    )
                    return 1

            process = subprocess.Popen(
                command,
                cwd=working_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except Exception as exc:  # pragma: no cover - surface unexpected spawn errors
            emit_stage_response(
                "error",
                "run_ifs",
                "Failed to launch IFs executable.",
                {"error": str(exc)},
            )
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
            emit_stage_response(
                "error",
                "run_ifs",
                f"IFs exited with code {return_code}.",
                {"return_code": return_code},
            )
            return 1

        try:
            end_year, w_gdp = _read_progress_summary(progress_path)
        except FileNotFoundError:
            emit_stage_response(
                "error",
                "run_ifs",
                "progress.txt was not found after the IFs run finished.",
                {"progress_path": progress_path},
            )
            return 1
        except ValueError as exc:
            emit_stage_response(
                "error",
                "run_ifs",
                str(exc),
                {},
            )
            return 1

        if end_year != args.end_year:
            emit_stage_response(
                "error",
                "run_ifs",
                f"Progress file reports end year {end_year}, expected {args.end_year}.",
                {"reported_end_year": end_year, "expected_end_year": args.end_year},
            )
            return 1

        try:
            payload = _prepare_run_artifacts(
                ifs_root=ifs_root,
                output_dir=output_dir,
                base_year=base_year,
                end_year=end_year,
                w_gdp=w_gdp,
                model_id=model_id,
            )
        except FileNotFoundError:
            emit_stage_response(
                "error",
                "run_ifs",
                "Working.run.db was not found after the IFs run finished.",
                {"working_run_db": os.path.join(ifs_root, "RUNFILES", "Working.run.db")},
            )
            return 1
        except OSError as exc:
            emit_stage_response(
                "error",
                "run_ifs",
                str(exc),
                {},
            )
            return 1

        try:
            _reset_working_database(ifs_root)
        except FileNotFoundError as exc:
            emit_stage_response(
                "error",
                "run_ifs",
                f"Unable to reset working database: {exc}",
                {},
            )
            return 1
        except OSError as exc:
            emit_stage_response(
                "error",
                "run_ifs",
                str(exc),
                {},
            )
            return 1

        with conn_bp:
            cursor = conn_bp.cursor()
            cursor.execute(
                """
                INSERT INTO model_output (ifs_id, model_id, model_status, fit_var, fit_pooled)
                VALUES (?, ?, 'completed', NULL, NULL)
                ON CONFLICT(model_id) DO UPDATE SET
                    ifs_id=excluded.ifs_id,
                    model_status='completed',
                    fit_var=NULL,
                    fit_pooled=NULL
                """,
                (ifs_id, model_id),
            )

        emit_stage_response(
            "success",
            "run_ifs",
            "IFs run completed successfully.",
            {
                "ifs_id": ifs_id,
                "model_id": model_id,
                "run_folder": payload["model_folder"],
            },
        )
        return 0
    finally:
        conn_bp.close()


def _prepare_run_artifacts(
    *,
    ifs_root: str,
    output_dir: str,
    base_year: int | None,
    end_year: int,
    w_gdp: float,
    model_id: str,
) -> dict:
    runfiles_dir = os.path.join(os.path.abspath(ifs_root), "RUNFILES")
    source_db = os.path.join(runfiles_dir, "Working.run.db")
    scenario_dir = os.path.join(os.path.abspath(ifs_root), "Scenario")
    source_sce = os.path.join(scenario_dir, "Working.sce")

    if not os.path.exists(source_db):
        raise FileNotFoundError(source_db)
    if not os.path.exists(source_sce):
        raise FileNotFoundError(source_sce)

    model_dir = os.path.join(output_dir, model_id)
    os.makedirs(model_dir, exist_ok=True)

    destination_db = os.path.join(model_dir, f"Working.{model_id}.run.db")
    destination_sce = os.path.join(model_dir, f"Working.{model_id}.sce")
    shutil.copy2(source_db, destination_db)
    shutil.copy2(source_sce, destination_sce)

    return {
        "status": "success",
        "model_id": model_id,
        "base_year": base_year,
        "end_year": end_year,
        "w_gdp": w_gdp,
        "output_file": destination_db,
        "working_sce": destination_sce,
        "model_folder": model_dir,
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
