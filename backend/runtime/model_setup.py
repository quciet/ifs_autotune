"""Prepare BIGPOPA model input based on an app-native input profile.

This script is invoked by the desktop shell as part of the "Model Setup"
process. It reads profile-managed parameter, coefficient, output, and ML
settings from ``bigpopa.db`` and records the resolved configuration back into
the same database.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import sqlite3
import sys
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from runtime.dataset_utils import compute_dataset_id, extract_structure_keys
from runtime.artifact_retention import (
    RETENTION_NONE,
    finalize_model_artifacts,
    normalize_artifact_retention_mode,
    reset_directory,
    staging_dir,
)

from db.ifs_metadata import log_version_metadata
from db.input_profiles import resolve_profile
from runtime.ml_method import MLMethodConfig
from runtime.model_run_store import (
    ModelDefinition,
    fetch_latest_result_for_model,
    upsert_seed_model_run,
)
from db.schema import (
    ensure_current_bigpopa_schema,
    ensure_ml_resume_state_table as ensure_unified_ml_resume_state_table,
)
from ifs.common_sce_utils import build_custom_parts, parse_dimension_flag


def _round_numbers(obj: Any, places: int = 6) -> Any:
    if isinstance(obj, float):
        return round(obj, places)
    if isinstance(obj, list):
        return [_round_numbers(x, places) for x in obj]
    if isinstance(obj, dict):
        return {k: _round_numbers(v, places) for k, v in obj.items()}
    return obj


def canonical_config(
    ifs_id: int, input_param: Dict[str, Any], input_coef: Dict[str, Any], output_set: Dict[str, Any]
) -> Dict[str, Any]:
    return {
        "ifs_id": int(ifs_id),
        "input_param": _round_numbers(copy.deepcopy(input_param)),
        "input_coef": _round_numbers(copy.deepcopy(input_coef)),
        "output_set": copy.deepcopy(output_set),
    }


def hash_model_id(config_obj: Dict[str, Any]) -> str:
    canonical_json = json.dumps(config_obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


# Ensure BIGPOPA schema exists without introducing timestamp fields.
def ensure_bigpopa_schema(cursor: sqlite3.Cursor) -> None:
    ensure_current_bigpopa_schema(cursor)


def ensure_model_output_tracking_columns(cursor: sqlite3.Cursor) -> None:
    del cursor


def ensure_ml_proposal_history_table(cursor: sqlite3.Cursor) -> None:
    del cursor


def ensure_ml_resume_state_table(cursor: sqlite3.Cursor) -> None:
    ensure_unified_ml_resume_state_table(cursor)


def _fetch_model_result_snapshot(
    conn: sqlite3.Connection,
    *,
    model_id: str,
) -> tuple[str | None, str | None, float | None, str | None]:
    row = conn.execute(
        """
        SELECT model_status, fit_var, fit_pooled, completed_at_utc
        FROM model_run
        WHERE model_id = ?
        ORDER BY
            CASE WHEN completed_at_utc IS NULL THEN 1 ELSE 0 END,
            completed_at_utc DESC,
            run_id DESC
        LIMIT 1
        """,
        (model_id,),
    ).fetchone()
    if row is None:
        status, fit_pooled, fit_var = fetch_latest_result_for_model(conn, model_id=model_id)
        return status, fit_var, fit_pooled, None
    return (
        row[0] if isinstance(row[0], str) or row[0] is None else str(row[0]),
        row[1] if isinstance(row[1], str) or row[1] is None else str(row[1]),
        float(row[2]) if row[2] is not None else None,
        row[3] if isinstance(row[3], str) or row[3] is None else str(row[3]),
    )


def build_input_param_from_defaults(
    cursor: sqlite3.Cursor, ifs_static_id: int, enabled_param_names: Iterable[str]
) -> Dict[str, float]:
    input_param: Dict[str, float] = {}
    for param_name in enabled_param_names:
        cursor.execute(
            """
            SELECT param_default
            FROM parameter
            WHERE ifs_static_id = ?
              AND LOWER(param_name) = LOWER(?)
            LIMIT 1
            """,
            (ifs_static_id, param_name),
        )
        row = cursor.fetchone()
        if row and row[0] is not None:
            input_param[param_name] = float(row[0])
            continue
        raise ValueError(
            f"Parameter '{param_name}' was selected in the input profile "
            f"but no matching entry was found in bigpopa.db.parameter."
        )
    return input_param


def diagnose_structure_drift(
    cursor: sqlite3.Cursor,
    ifs_id: int,
    input_param: Dict[str, Any],
    input_coef: Dict[str, Any],
    output_set: Dict[str, Any],
) -> Dict[str, Any] | None:
    row = cursor.execute(
        """
        SELECT model_id, dataset_id, input_param, input_coef, output_set
        FROM model_run
        WHERE ifs_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (ifs_id,),
    ).fetchone()
    if not row:
        return None

    reference_model_id, reference_dataset_id, ip_raw, ic_raw, os_raw = row
    try:
        reference_input_param = json.loads(ip_raw)
        reference_input_coef = json.loads(ic_raw)
        reference_output_set = json.loads(os_raw)
    except Exception:
        return None

    current_param_keys, current_coef_keys, current_output_keys = extract_structure_keys(
        input_param, input_coef, output_set
    )
    reference_param_keys, reference_coef_keys, reference_output_keys = extract_structure_keys(
        reference_input_param, reference_input_coef, reference_output_set
    )

    if (
        current_param_keys == reference_param_keys
        and current_coef_keys == reference_coef_keys
        and current_output_keys == reference_output_keys
    ):
        return None

    return {
        "reference_model_id": reference_model_id,
        "reference_dataset_id": reference_dataset_id,
        "current_param_count": len(current_param_keys),
        "reference_param_count": len(reference_param_keys),
        "parameter_keys_added": sorted(current_param_keys - reference_param_keys),
        "parameter_keys_removed": sorted(reference_param_keys - current_param_keys),
        "coefficient_keys_added": sorted(current_coef_keys - reference_coef_keys),
        "coefficient_keys_removed": sorted(reference_coef_keys - current_coef_keys),
        "output_keys_added": sorted(current_output_keys - reference_output_keys),
        "output_keys_removed": sorted(reference_output_keys - current_output_keys),
    }


def format_structure_drift_warning(diagnostics: Dict[str, Any]) -> str:
    added = diagnostics.get("parameter_keys_added") or []
    removed = diagnostics.get("parameter_keys_removed") or []
    changes: List[str] = []
    if added:
        changes.append(f"added parameters: {', '.join(added)}")
    if removed:
        changes.append(f"removed parameters: {', '.join(removed)}")
    if not changes:
        changes.append("the selected structural keys changed")
    return (
        "This setup differs from the latest stored dataset for the same IFs record; "
        + "; ".join(changes)
        + "."
    )


def log(status: str, message: str, **kwargs: Any) -> None:
    payload: Dict[str, Any] = {"status": status, "message": message}
    if kwargs:
        payload.update(kwargs)
    print(json.dumps(payload))
    sys.stdout.flush()


# Emit a structured response for Electron to consume after each stage.
def emit_stage_response(status: str, stage: str, message: str, data: Dict[str, Any]) -> None:
    payload = {
        "status": status,
        "stage": stage,
        "message": message,
        "data": data,
    }
    print(json.dumps(payload))
    sys.stdout.flush()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Randomize coefficients for Model Setup",
    )
    parser.add_argument(
        "--ifs-root",
        required=True,
        help="Path to IFs root folder (with RUNFILES directory)",
    )
    parser.add_argument(
        "--input-profile-id",
        required=True,
        type=int,
        help="Input profile identifier stored in bigpopa.db",
    )
    parser.add_argument(
        "--output-folder",
        required=False,
        help="Path to BIGPOPA output folder (contains bigpopa.db)",
    )
    parser.add_argument(
        "--artifact-retention",
        default=RETENTION_NONE,
        help="Artifact retention mode: none, best_only, or all",
    )
    parser.add_argument(
        "--base-year",
        type=int,
        default=None,
        help="Base year used to seed Working.sce",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Forecast year used to seed Working.sce",
    )
    return parser


_LAST_KNOWN_YEARS: Optional[Tuple[int, int]] = None


def _extract_years_from_sce(path: Path) -> Optional[Tuple[int, int]]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            base_year: Optional[int] = None
            forecast_year: Optional[int] = None
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                parts = [segment.strip() for segment in line.split(",") if segment.strip()]
                if len(parts) < 2:
                    continue
                key = parts[0].lower()

                def _first_int(values: List[str]) -> Optional[int]:
                    for candidate in values:
                        try:
                            return int(float(candidate))
                        except (TypeError, ValueError):
                            continue
                    return None

                if key == "yr_base" and base_year is None:
                    base_year = _first_int(parts[1:])
                elif key == "yr_forecast" and forecast_year is None:
                    forecast_year = _first_int(parts[1:])

                if base_year is not None and forecast_year is not None:
                    break
    except FileNotFoundError:
        return None

    if base_year is not None and forecast_year is not None:
        return base_year, forecast_year

    if _LAST_KNOWN_YEARS is not None:
        return _LAST_KNOWN_YEARS

    return None


def _infer_base_year_from_db(db_path: Path) -> Optional[int]:
    try:
        log(
            "debug",
            "Attempting to connect to database for base year inference",
            database=str(db_path.resolve()),
        )
        conn = sqlite3.connect(str(db_path))
    except Exception:
        log(
            "warn",
            "Unable to connect to database for base year inference",
            database=str(db_path.resolve()),
        )
        return None

    try:
        cursor = conn.cursor()
        try:
            tables = [
                row[0]
                for row in cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            ]
            log(
                "debug",
                "Tables found during base year inference",
                tables=tables,
            )
        except sqlite3.Error:
            log(
                "warn",
                "Failed to enumerate tables for base year inference",
                database=str(db_path.resolve()),
            )
            return None

        candidate_column_names = {"baseyear", "base_year", "yrbase", "yr_base"}

        for table in tables:
            try:
                cursor.execute(f"PRAGMA table_info(\"{table}\")")
            except sqlite3.Error:
                log(
                    "warn",
                    "Failed PRAGMA table_info during base year inference",
                    table=table,
                )
                continue

            columns = cursor.fetchall()
            if not columns:
                continue

            column_names = [column[1] for column in columns if column and column[1]]
            log(
                "debug",
                "Columns inspected for base year inference",
                table=table,
                columns=column_names,
            )

            match: Optional[str] = None
            for column in columns:
                column_name = column[1]
                if not column_name:
                    continue
                normalized = column_name.lower().replace("_", "")
                if normalized in candidate_column_names:
                    match = column_name
                    break

            if match is None:
                continue

            try:
                cursor.execute(f"SELECT \"{match}\" FROM \"{table}\" LIMIT 1")
            except sqlite3.Error:
                log(
                    "warn",
                    "Failed to read potential base year column",
                    table=table,
                    column=match,
                )
                continue

            row = cursor.fetchone()
            if not row:
                continue

            value = row[0]
            if value is None:
                continue

            try:
                log(
                    "debug",
                    "Base year inferred",
                    table=table,
                    column=match,
                    value=float(value),
                )
                return int(float(value))
            except (TypeError, ValueError):
                continue
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return None


def create_working_sce(ifs_root: Path) -> Path:
    scenario_dir = ifs_root / "Scenario"
    scenario_dir.mkdir(parents=True, exist_ok=True)

    sce_path = scenario_dir / "Working.sce"
    try:
        sce_path.unlink()
    except FileNotFoundError:
        pass

    sce_path.touch()

    return sce_path


def _load_param_dimension_map(
    bigpopa_db_path: Path | None,
    ifs_static_id: int | None,
    param_names: Iterable[str],
) -> Dict[str, Any]:
    if bigpopa_db_path is None or ifs_static_id is None:
        return {}

    names = [str(name).strip() for name in param_names if isinstance(name, str) and name.strip()]
    if not names:
        return {}

    dimension_map: Dict[str, Any] = {}
    conn = sqlite3.connect(str(bigpopa_db_path))
    try:
        cursor = conn.cursor()
        for name in names:
            cursor.execute(
                """
                SELECT param_type
                FROM parameter
                WHERE ifs_static_id = ?
                  AND LOWER(param_name) = LOWER(?)
                LIMIT 1
                """,
                (ifs_static_id, name),
            )
            row = cursor.fetchone()
            dimension_map[name.lower()] = row[0] if row else None
    finally:
        conn.close()
    return dimension_map


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    log("info", "=== MODEL SETUP STARTED ===")

    ifs_root = Path(args.ifs_root)
    output_root: Optional[Path] = Path(args.output_folder).resolve() if args.output_folder else None
    base_run_db_path = ifs_root / "RUNFILES" / "IFsBase.run.db"
    if not base_run_db_path.exists():
        log(
            "error",
            "Missing IFsBase.run.db",
            database=str(base_run_db_path.resolve()),
        )
        emit_stage_response(
            "error",
            "model_setup",
            "IFsBase.run.db was not found; cannot proceed with baseline extraction.",
            {"base_run_db": str(base_run_db_path.resolve())},
        )
        return 1

    if output_root is None:
        log(
            "error",
            "Output folder is required to persist BIGPOPA configuration",
        )
        emit_stage_response(
            "error",
            "model_setup",
            "Output folder is required to persist BIGPOPA configuration.",
            {"output_folder": args.output_folder},
        )
        return 1

    existing_sce_path = ifs_root / "Scenario" / "Working.sce"
    existing_years = _extract_years_from_sce(existing_sce_path)

    base_year: Optional[int] = args.base_year
    forecast_year: Optional[int] = args.end_year

    if forecast_year is None and existing_years:
        forecast_year = existing_years[1]

    if base_year is None and existing_years:
        base_year = existing_years[0]

    if base_year is None:
        base_year = _infer_base_year_from_db(base_run_db_path)

    if forecast_year is None:
        log(
            "error",
            "Unable to determine forecast year for Working.sce.",
        )
        emit_stage_response(
            "error",
            "model_setup",
            "Unable to determine forecast year for Working.sce.",
            {},
        )
        return 1

    global _LAST_KNOWN_YEARS
    if base_year is not None:
        _LAST_KNOWN_YEARS = (base_year, forecast_year)
    else:
        _LAST_KNOWN_YEARS = None

    try:
        resolved_profile = resolve_profile(
            output_folder=output_root,
            profile_id=args.input_profile_id,
        )
    except Exception as exc:
        log(
            "error",
            "Unable to resolve input profile",
            profile_id=args.input_profile_id,
            error=str(exc),
        )
        emit_stage_response(
            "error",
            "model_setup",
            str(exc),
            {"input_profile_id": args.input_profile_id},
        )
        return 1

    log(
        "info",
        "Resolved input profile",
        profile_id=resolved_profile.profile_id,
        profile_name=resolved_profile.name,
        parameter_count=len(resolved_profile.input_param),
        coefficient_count=sum(
            len(beta_map)
            for x_map in resolved_profile.input_coef.values()
            for beta_map in x_map.values()
        ),
        output_count=len(resolved_profile.output_set),
    )

    version_payload: Optional[Dict[str, Any]] = None
    ifs_id: Optional[int] = None

    log(
        "debug",
        "Version metadata condition check",
        output_folder=args.output_folder,
        base_year=base_year,
        forecast_year=forecast_year,
    )
    if args.output_folder and base_year is not None:
        try:
            fit_metric = resolved_profile.fit_metric
            ml_method = resolved_profile.ml_settings.ml_method
            version_payload = log_version_metadata(
                ifs_root=ifs_root,
                output_folder=Path(args.output_folder),
                base_year=base_year,
                end_year=forecast_year,
                fit_metric=fit_metric,
                ml_method=ml_method.normalized_value,
            )
        except Exception as exc:
            log(
                "error",
                "Failed to record IFs version metadata",
                error=str(exc),
            )
            emit_stage_response(
                "error",
                "model_setup",
                str(exc),
                {},
            )
            return 1
        else:
            log(
                "info",
                "Static layer linked",
                ifs_static_id=version_payload.get("ifs_static_id"),
            )
            # Clean up version_payload to avoid duplicate keys
            version_payload.pop("status", None)
            version_payload.pop("message", None)
            version_payload["ml_method_runtime"] = ml_method.model_type
            version_payload["ml_method_profile"] = ml_method.raw_value
            version_payload["profile_id"] = resolved_profile.profile_id
            log("info", "IFs version metadata recorded", **version_payload)
            ifs_id_value = version_payload.get("ifs_id")
            if ifs_id_value is not None:
                try:
                    ifs_id = int(ifs_id_value)
                except (TypeError, ValueError):
                    ifs_id = None
    else:
        log(
            "warn",
            "Skipping bigpopa.db registration because output_folder or base_year is missing",
            output_folder=args.output_folder,
            base_year=base_year,
        )

    if ifs_id is None or version_payload is None:
        log(
            "error",
            "Unable to determine ifs_id for configuration persistence",
        )
        emit_stage_response(
            "error",
            "model_setup",
            "Unable to resolve ifs_id; configuration cannot be stored.",
            {},
        )
        return 1

    ifs_static_id = version_payload.get("ifs_static_id") if version_payload else None
    if ifs_static_id is None:
        log(
            "error",
            "Unable to resolve ifs_static_id for default retrieval",
        )
        emit_stage_response(
            "error",
            "model_setup",
            "Unable to resolve ifs_static_id; configuration cannot be stored.",
            {},
        )
        return 1

    if int(ifs_static_id) != int(resolved_profile.ifs_static_id):
        message = (
            "Selected profile does not match the current IFs static layer. "
            f"Expected ifs_static_id={ifs_static_id}, profile is tied to "
            f"ifs_static_id={resolved_profile.ifs_static_id}."
        )
        log(
            "error",
            "Profile static layer mismatch",
            expected_ifs_static_id=ifs_static_id,
            profile_ifs_static_id=resolved_profile.ifs_static_id,
        )
        emit_stage_response(
            "error",
            "model_setup",
            message,
            {
                "ifs_static_id": ifs_static_id,
                "input_profile_id": resolved_profile.profile_id,
                "profile_ifs_static_id": resolved_profile.ifs_static_id,
            },
        )
        return 1

    bigpopa_db_path = output_root / "bigpopa.db"

    input_param: Dict[str, Any] = copy.deepcopy(resolved_profile.input_param)
    input_coef: Dict[str, Dict[str, Dict[str, float]]] = copy.deepcopy(
        resolved_profile.input_coef
    )
    output_set: Dict[str, str] = copy.deepcopy(resolved_profile.output_set)

    try:
        conn_bp = sqlite3.connect(str(bigpopa_db_path))
        cursor = conn_bp.cursor()
        ensure_bigpopa_schema(cursor)

        conn_bp.commit()
    finally:
        try:
            conn_bp.close()
        except Exception:
            pass

    # Extract output_set mapping (Variable → Table) from DataDict sheet
    dataset_id = compute_dataset_id(
        ifs_id=ifs_id,
        input_param=input_param,
        input_coef=input_coef,
        output_set=output_set,
    )

    config_obj = canonical_config(ifs_id, input_param, input_coef, output_set)
    model_id = hash_model_id(config_obj)
    output_dir = reset_directory(
        staging_dir(output_root, model_id)
    )
    artifact_retention_mode = normalize_artifact_retention_mode(args.artifact_retention)

    inserted = 0
    dataset_diagnostics: Dict[str, Any] | None = None
    dataset_warning: str | None = None
    conn_bp = sqlite3.connect(str(bigpopa_db_path))
    try:
        cur_bp = conn_bp.cursor()
        ensure_bigpopa_schema(cur_bp)
        dataset_diagnostics = diagnose_structure_drift(
            cur_bp,
            int(ifs_id),
            input_param,
            input_coef,
            output_set,
        )
        if dataset_diagnostics:
            dataset_warning = format_structure_drift_warning(dataset_diagnostics)
            log(
                "warn",
                "Model setup structure differs from the latest stored dataset for the same IFs record",
                warning=dataset_warning,
                **dataset_diagnostics,
            )
        existing_seed = cur_bp.execute(
            """
            SELECT run_id
            FROM model_run
            WHERE model_id = ?
              AND trial_index IS NULL
              AND batch_index IS NULL
              AND resolution_note = 'model_setup_seed'
            LIMIT 1
            """,
            (model_id,),
        ).fetchone()
        upsert_seed_model_run(
            conn_bp,
            definition=ModelDefinition(
                ifs_id=int(ifs_id),
                model_id=model_id,
                dataset_id=dataset_id,
                input_param=input_param,
                input_coef=input_coef,
                output_set=output_set,
            ),
            model_status=None,
            fit_var=None,
            fit_pooled=None,
            completed_at_utc=None,
        )
        inserted = 0 if existing_seed is not None else 1
        conn_bp.commit()
    finally:
        conn_bp.close()

    extract_compare_path = Path(__file__).resolve().parents[1] / "extract_compare.py"
    extract_args = [
        sys.executable,
        str(extract_compare_path),
        "--ifs-root",
        str(ifs_root),
        "--model-db",
        str(base_run_db_path),
        "--input-profile-id",
        str(resolved_profile.profile_id),
        "--model-id",
        model_id,
        "--ifs-id",
        str(ifs_id),
        "--bigpopa-db",
        str(bigpopa_db_path),
        "--output-dir",
        str(output_dir),
    ]

    extract_return = None
    retained_artifact_dir: Path | None = None
    try:
        extract_proc = subprocess.run(extract_args, check=False)
        extract_return = extract_proc.returncode
        with sqlite3.connect(str(bigpopa_db_path)) as artifact_conn:
            retained_artifact_dir = finalize_model_artifacts(
                conn=artifact_conn,
                output_dir=output_root,
                model_id=model_id,
                dataset_id=dataset_id,
                mode=artifact_retention_mode,
                staged_dir=output_dir,
            )
    except Exception as exc:  # noqa: BLE001
        log("warn", "Failed to execute extract_compare", error=str(exc))
    log(
        "success",
        "Model Setup completed successfully",
        updates=updates,
        sce_variables_appended=0,
        sce_file=str(existing_sce_path.resolve()),
        model_id=model_id,
        ifs_id=ifs_id,
        config_inserted=bool(inserted),
        extract_return=extract_return,
    )

    emit_stage_response(
        "success",
        "model_setup",
        "Model setup completed; configuration stored in database.",
        {
            "ifs_id": ifs_id,
            "model_id": model_id,
            "dataset_id": dataset_id,
            "retained_artifact_dir": str(retained_artifact_dir) if retained_artifact_dir else None,
            "dataset_warning": dataset_warning,
            "dataset_diagnostics": dataset_diagnostics,
        },
    )
    return 0 if extract_return in (None, 0) else int(extract_return)


if __name__ == "__main__":
    sys.exit(main())
