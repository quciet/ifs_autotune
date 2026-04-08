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
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

from artifact_retention import (
    RETENTION_NONE,
    finalize_model_artifacts,
    normalize_artifact_retention_mode,
    reset_directory,
    staging_dir,
)
from model_run_store import find_active_run_id_for_model, load_model_definition, update_model_run
from model_status import (
    FALLBACK_FIT_POOLED,
    IFS_CONFIG_APPLIED,
    IFS_RUN_COMPLETED,
    IFS_RUN_FAILED,
    IFS_RUN_STARTED,
)
from prepare_coeff_param import apply_config_to_ifs_files
from tools.db.bigpopa_schema import ensure_current_bigpopa_schema


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
    parser.add_argument(
        "--artifact-retention",
        default=RETENTION_NONE,
        help="Artifact retention mode: none, best_only, or all",
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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _update_model_run_status(
    conn: sqlite3.Connection,
    ifs_id: int,
    model_id: str,
    *,
    model_status: str,
    fit_pooled: float | None = None,
    fit_var: str | None = None,
) -> None:
    del ifs_id
    with conn:
        run_id = find_active_run_id_for_model(conn, model_id=model_id)
        if run_id is None:
            raise RuntimeError(f"No model_run row exists for model_id={model_id}.")
        update_kwargs: dict[str, object] = {
            "run_id": run_id,
            "model_status": model_status,
            "fit_var": fit_var,
            "fit_pooled": fit_pooled,
        }
        if model_status in {IFS_RUN_FAILED, IFS_RUN_COMPLETED}:
            update_kwargs["completed_at_utc"] = _utc_now_iso()
        update_model_run(conn, **update_kwargs)



def _refresh_dyadic_work_database(ifs_root: str) -> bool:
    data_dir = os.path.join(os.path.abspath(ifs_root), "DATA")
    runfiles_dir = os.path.join(os.path.abspath(ifs_root), "RUNFILES")
    source_db = os.path.join(data_dir, "IFsForDyadic.db")
    work_db = os.path.join(runfiles_dir, "ifsForDyadicWork.db")

    if not os.path.exists(source_db):
        return False

    os.makedirs(runfiles_dir, exist_ok=True)
    shutil.copy2(source_db, work_db)
    return True


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
    artifact_retention_mode = normalize_artifact_retention_mode(args.artifact_retention)

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
        input_param: Dict[str, object] = {}
        input_coef: Dict[str, object] = {}
        output_set: Dict[str, object] = {}
        definition = None
        dataset_id: str | None = None

        try:
            with conn_bp:
                cursor = conn_bp.cursor()
                ensure_current_bigpopa_schema(cursor)
                definition = load_model_definition(conn_bp, model_id)
                cursor.execute(
                    """
                    SELECT ifs_static_id
                    FROM ifs_version
                    WHERE ifs_id = ?
                    LIMIT 1
                    """,
                    (ifs_id,),
                )
                static_row = cursor.fetchone()
        except (sqlite3.Error, RuntimeError) as exc:
            emit_stage_response(
                "error",
                "run_ifs",
                f"Database error while reading model configuration: {exc}",
                {"model_id": model_id},
            )
            return 1

        if definition is None:
            emit_stage_response(
                "error",
                "run_ifs",
                "Model configuration not found in BIGPOPA database.",
                {"model_id": model_id},
            )
            return 1

        input_param = definition.input_param
        input_coef = definition.input_coef
        output_set = definition.output_set
        dataset_id = definition.dataset_id

        if base_year is None:
            emit_stage_response(
                "error",
                "run_ifs",
                "Base year must be provided to configure IFs run.",
                {"model_id": model_id},
            )
            return 1

        if static_row is None or static_row[0] is None:
            emit_stage_response(
                "error",
                "run_ifs",
                "Unable to resolve ifs_static_id for Working.sce parameter policy.",
                {"ifs_id": ifs_id},
            )
            return 1

        try:
            # Apply model configuration to IFs Working.sce and Working.run.db
            apply_config_to_ifs_files(
                ifs_root=Path(args.ifs_root),
                input_param=input_param,
                input_coef=input_coef,
                base_year=args.base_year,
                end_year=args.end_year,
                bigpopa_db_path=bigpopa_db_path,
                ifs_static_id=int(static_row[0]),
            )
        except Exception as exc:
            _update_model_run_status(
                conn_bp,
                ifs_id,
                model_id,
                model_status=IFS_RUN_FAILED,
                fit_pooled=FALLBACK_FIT_POOLED,
            )
            emit_stage_response(
                "error",
                "run_ifs",
                f"Failed to apply model configuration to IFs files: {exc}",
                {"model_id": model_id},
            )
            return 1

        _update_model_run_status(
            conn_bp,
            ifs_id,
            model_id,
            model_status=IFS_CONFIG_APPLIED,
        )

        try:
            dyadic_refreshed = _refresh_dyadic_work_database(ifs_root)
        except OSError as exc:
            _update_model_run_status(
                conn_bp,
                ifs_id,
                model_id,
                model_status=IFS_RUN_FAILED,
                fit_pooled=FALLBACK_FIT_POOLED,
            )
            emit_stage_response(
                "error",
                "run_ifs",
                f"Failed to refresh dyadic working database: {exc}",
                {
                    "source_db": os.path.join(ifs_root, "DATA", "IFsForDyadic.db"),
                    "work_db": os.path.join(ifs_root, "RUNFILES", "ifsForDyadicWork.db"),
                },
            )
            return 1

        if dyadic_refreshed:
            emit_stage_response(
                "info",
                "run_ifs",
                "Refreshed dyadic working database before IFs launch.",
                {"work_db": os.path.join(ifs_root, "RUNFILES", "ifsForDyadicWork.db")},
            )

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
            _update_model_run_status(
                conn_bp,
                ifs_id,
                model_id,
                model_status=IFS_RUN_FAILED,
                fit_pooled=FALLBACK_FIT_POOLED,
            )
            emit_stage_response(
                "error",
                "run_ifs",
                "Failed to launch IFs executable.",
                {"error": str(exc)},
            )
            return 1

        _update_model_run_status(
            conn_bp,
            ifs_id,
            model_id,
            model_status=IFS_RUN_STARTED,
        )

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
            _update_model_run_status(
                conn_bp,
                ifs_id,
                model_id,
                model_status=IFS_RUN_FAILED,
                fit_pooled=FALLBACK_FIT_POOLED,
            )

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
            _update_model_run_status(
                conn_bp,
                ifs_id,
                model_id,
                model_status=IFS_RUN_FAILED,
                fit_pooled=FALLBACK_FIT_POOLED,
            )
            emit_stage_response(
                "error",
                "run_ifs",
                "progress.txt was not found after the IFs run finished.",
                {"progress_path": progress_path},
            )
            return 1
        except ValueError as exc:
            _update_model_run_status(
                conn_bp,
                ifs_id,
                model_id,
                model_status=IFS_RUN_FAILED,
                fit_pooled=FALLBACK_FIT_POOLED,
            )
            emit_stage_response(
                "error",
                "run_ifs",
                str(exc),
                {},
            )
            return 1

        if end_year != args.end_year:
            _update_model_run_status(
                conn_bp,
                ifs_id,
                model_id,
                model_status=IFS_RUN_FAILED,
                fit_pooled=FALLBACK_FIT_POOLED,
            )
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
            _update_model_run_status(
                conn_bp,
                ifs_id,
                model_id,
                model_status=IFS_RUN_FAILED,
                fit_pooled=FALLBACK_FIT_POOLED,
            )
            emit_stage_response(
                "error",
                "run_ifs",
                "Working.run.db was not found after the IFs run finished.",
                {"working_run_db": os.path.join(ifs_root, "RUNFILES", "Working.run.db")},
            )
            return 1
        except OSError as exc:
            _update_model_run_status(
                conn_bp,
                ifs_id,
                model_id,
                model_status=IFS_RUN_FAILED,
                fit_pooled=FALLBACK_FIT_POOLED,
            )
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
            _update_model_run_status(
                conn_bp,
                ifs_id,
                model_id,
                model_status=IFS_RUN_FAILED,
                fit_pooled=FALLBACK_FIT_POOLED,
            )
            emit_stage_response(
                "error",
                "run_ifs",
                f"Unable to reset working database: {exc}",
                {},
            )
            return 1
        except OSError as exc:
            _update_model_run_status(
                conn_bp,
                ifs_id,
                model_id,
                model_status=IFS_RUN_FAILED,
                fit_pooled=FALLBACK_FIT_POOLED,
            )
            emit_stage_response(
                "error",
                "run_ifs",
                str(exc),
                {},
            )
            return 1

        _update_model_run_status(
            conn_bp,
            ifs_id,
            model_id,
            model_status=IFS_RUN_COMPLETED,
            fit_pooled=FALLBACK_FIT_POOLED,
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

        # === STEP 2: Automatically trigger extract_compare.py ===
        retained_artifact_dir: Path | None = None
        try:
            extract_compare_path = Path(__file__).resolve().parent / "extract_compare.py"
            model_db_path = os.path.join(payload["model_folder"], f"Working.{model_id}.run.db")
            input_file_path = os.path.join(output_dir, "StartingPointTable.xlsx")

            if not os.path.exists(model_db_path):
                emit_stage_response(
                    "error",
                    "extract_compare",
                    f"Model run DB not found at {model_db_path}",
                    {"model_id": model_id},
                )
                return 1

            found_pairs = len(output_set)
            bigpopa_db_path = os.path.join(output_dir, "bigpopa.db")
            if found_pairs <= 0:
                emit_stage_response(
                    "error",
                    "extract_compare",
                    f"No output_set found for model_id={model_id}; cannot continue extraction.",
                    {"bigpopa_db": bigpopa_db_path},
                )
                return 1
            emit_stage_response(
                "info",
                "extract_compare",
                f"Located {found_pairs} var:hist pairs in model_run.output_set for model_id={model_id}.",
                {"bigpopa_db": bigpopa_db_path, "output_set_size": found_pairs},
            )

            subprocess.run(
                [
                    sys.executable,
                    str(extract_compare_path),
                    "--ifs-root",
                    ifs_root,
                    "--model-db",
                    model_db_path,
                    "--input-file",
                    input_file_path,
                    "--model-id",
                    model_id,
                    "--ifs-id",
                    str(ifs_id),
                    "--bigpopa-db",
                    bigpopa_db_path,
                ],
                check=True,
            )
            retained_artifact_dir = finalize_model_artifacts(
                conn=conn_bp,
                output_dir=Path(output_dir),
                model_id=model_id,
                dataset_id=dataset_id,
                mode=artifact_retention_mode,
                staged_dir=Path(payload["model_folder"]),
            )

            emit_stage_response(
                "success",
                "extract_compare",
                "Variable extraction and comparison completed.",
                {
                    "model_id": model_id,
                    "ifs_id": ifs_id,
                    "model_folder": str(retained_artifact_dir) if retained_artifact_dir else None,
                },
            )
        except subprocess.CalledProcessError as exc:
            retained_artifact_dir = finalize_model_artifacts(
                conn=conn_bp,
                output_dir=Path(output_dir),
                model_id=model_id,
                dataset_id=dataset_id,
                mode=artifact_retention_mode,
                staged_dir=Path(payload["model_folder"]),
            )
            emit_stage_response(
                "error",
                "extract_compare",
                f"extract_compare.py failed with return code {exc.returncode}",
                {
                    "model_id": model_id,
                    "retained_artifact_dir": str(retained_artifact_dir) if retained_artifact_dir else None,
                },
            )
            return 1
        except Exception as exc:
            emit_stage_response(
                "error",
                "extract_compare",
                f"Unexpected error running extract_compare.py: {exc}",
                {"model_id": model_id},
            )
            return 1

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

    model_dir = str(reset_directory(staging_dir(Path(output_dir), model_id)))

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
