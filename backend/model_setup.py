"""Prepare BIGPOPA model input based on a starting point table.

This script is invoked by the desktop shell as part of the "Model Setup"
process. It reads coefficients from ``StartingPointTable.xlsx`` (``TablFunc``
and ``AnalFunc`` sheets) and records the configuration in ``bigpopa.db``.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import random
import sqlite3
import sys
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dataset_utils import compute_dataset_id, extract_structure_keys

from log_ifs_version import log_version_metadata
from ml_method import MLMethodConfig, load_required_ml_method
from model_run_store import (
    ModelDefinition,
    fetch_latest_result_for_model,
    upsert_seed_model_run,
)
from tools.db.bigpopa_schema import (
    ensure_current_bigpopa_schema,
    ensure_ml_resume_state_table as ensure_unified_ml_resume_state_table,
)

import pandas as pd

from common_sce_utils import build_custom_parts, parse_dimension_flag


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


def _row_enabled(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        try:
            return float(value) == 1.0
        except (TypeError, ValueError):
            return False

    normalized = str(value).strip().lower()
    if normalized in {"true", "on", "yes"}:
        return True

    try:
        return float(normalized) == 1.0
    except (TypeError, ValueError):
        return False


def _resolve_row_name(
    row: Dict[str, Any] | pd.Series,
    candidates: Iterable[str] = ("Name", "Variable", "Name/Variable"),
) -> str | None:
    for candidate in candidates:
        raw = row.get(candidate)
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return None


def extract_enabled_ifsv_names(ifsv_df: pd.DataFrame) -> List[str]:
    if ifsv_df.empty:
        return []

    enabled: Dict[str, str] = {}
    for _, row in ifsv_df.iterrows():
        if not _row_enabled(row.get("Switch", "0")):
            continue
        name = _resolve_row_name(row)
        if not name:
            continue
        enabled.setdefault(name.casefold(), name)
    return [enabled[key] for key in sorted(enabled.keys())]


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
            f"Parameter '{param_name}' was selected in IFsVar "
            f"but no matching entry was found in bigpopa.db.parameter."
        )
    return input_param


def extract_enabled_output_set(data_dict_df: pd.DataFrame) -> Dict[str, str]:
    output_set: Dict[str, str] = {}
    if data_dict_df.empty:
        return output_set

    for _, row in data_dict_df.iterrows():
        if not _row_enabled(row.get("Switch", "0")):
            continue
        variable = row.get("Variable")
        table = row.get("Table")
        if pd.notna(variable) and pd.notna(table):
            output_set[str(variable).strip()] = str(table).strip()
    return output_set


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


def _load_ml_text_settings(starting_point_table: Path) -> Tuple[str, MLMethodConfig]:
    fit_metric = "mse"
    ml_method = load_required_ml_method(starting_point_table)

    df = pd.read_excel(starting_point_table, sheet_name="ML", engine="openpyxl")

    for _, row in df.iterrows():
        method = str(row.get("Method") or "").strip().lower()
        if method != "general":
            continue

        parameter = str(row.get("Parameter") or "").strip().lower()
        value = str(row.get("Value") or "").strip().lower()
        if not value:
            continue

        if parameter == "fit_metric":
            fit_metric = value

    return fit_metric, ml_method


def build_input_param_from_startingpoint(ifsv_df: pd.DataFrame) -> Dict[str, Any]:
    mp: Dict[str, Any] = {}
    if ifsv_df.empty:
        return mp
    for _, row in ifsv_df.iterrows():
        if not _row_enabled(row.get("Switch", "0")):
            continue
        name = _resolve_row_name(row, ("Name/Variable", "Name", "Variable"))
        if not name:
            continue
        value = None
        for candidate in ("Minimum", "Value", "Default", "Min"):
            if candidate in row and row[candidate] is not None:
                value = row[candidate]
                break
        if value is None:
            continue
        if isinstance(value, (int, float)):
            mp[name] = float(value)
        else:
            try:
                mp[name] = float(str(value))
            except (TypeError, ValueError):
                mp[name] = value
    return mp


def build_input_coef_from_working_db(working_run_db_path: str) -> Dict[str, Dict[str, Dict[str, float]]]:
    out: Dict[str, Dict[str, Dict[str, float]]] = {}
    if not os.path.exists(working_run_db_path):
        return out
    conn = sqlite3.connect(working_run_db_path)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT function_name, x_name, beta_name, beta_value
            FROM coefficients
            """
        )
        for func_name, x_name, beta_name, beta_value in cur.fetchall():
            try:
                value = float(beta_value)
            except (TypeError, ValueError):
                continue
            out.setdefault(func_name, {}).setdefault(x_name, {})[beta_name] = value
    except sqlite3.Error:
        pass
    finally:
        conn.close()
    return out


def build_output_set_from_ifsvartab(ifsv_df: pd.DataFrame) -> Dict[str, Any]:
    mp: Dict[str, Any] = {}
    if ifsv_df.empty:
        return mp
    for _, row in ifsv_df.iterrows():
        if not _row_enabled(row.get("Switch", "0")):
            continue
        name = _resolve_row_name(row, ("Name/Variable", "Name", "Variable"))
        if not name:
            continue
        hist_table = row.get("HistTable") or row.get("Table")
        if isinstance(hist_table, str) and hist_table.strip():
            mp[name] = hist_table.strip()
    return mp


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
        "--input-file",
        required=True,
        help="Path to StartingPointTable.xlsx",
    )
    parser.add_argument(
        "--output-folder",
        required=False,
        help="Path to BIGPOPA output folder (contains bigpopa.db)",
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


def _load_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    log(
        "debug",
        "Reading sheet",
        file=str(path.resolve()),
        sheet=sheet_name,
    )
    try:
        frame = pd.read_excel(path, sheet_name=sheet_name)
        log(
            "debug",
            "Sheet loaded",
            sheet=sheet_name,
            rows=int(frame.shape[0]),
            columns=int(frame.shape[1]),
        )
        return frame
    except Exception as exc:
        log(
            "warn",
            "Failed to read sheet",
            sheet=sheet_name,
            error=str(exc),
        )
        return pd.DataFrame()


def _is_enabled(value: Any) -> bool:
    if value is None or pd.isna(value):
        return False
    if isinstance(value, (int, float)):
        try:
            return int(value) == 1
        except (TypeError, ValueError):
            return False
    normalized = str(value).strip().lower()
    return normalized in {"1", "on"}


def _normalize_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, str) and not value.strip():
            return None
        result = float(value)
        if math.isnan(result):
            return None
        return result
    except (TypeError, ValueError):
        return None


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


def add_from_startingpoint(
    ifs_root: Path,
    excel_path: Path,
    ifsv_df: pd.DataFrame | None = None,
    bigpopa_db_path: Path | None = None,
    ifs_static_id: int | None = None,
) -> Tuple[int, Dict[str, float]]:
    sce_path = ifs_root / "Scenario" / "Working.sce"
    input_param_used: Dict[str, float] = {}

    if not sce_path.exists():
        return 0, input_param_used

    years = _extract_years_from_sce(sce_path)
    if not years:
        return 0, input_param_used
    base_year, forecast_year = years
    if forecast_year < base_year:
        return 0, input_param_used

    df: pd.DataFrame
    if ifsv_df is None:
        try:
            df = pd.read_excel(excel_path, sheet_name="IFsVar", engine="openpyxl")
        except Exception:
            return 0, input_param_used
    else:
        df = ifsv_df.copy()

    if df.empty:
        return 0, input_param_used

    value_count = forecast_year - base_year + 1
    if value_count <= 0:
        return 0, input_param_used

    lines_to_append: List[str] = []
    appended = 0

    if "Variable" in df.columns:
        param_names = [str(value).strip() for value in df["Variable"].tolist()]
    elif "Name" in df.columns:
        param_names = [str(value).strip() for value in df["Name"].tolist()]
    else:
        param_names = []
    db_dimension_map = _load_param_dimension_map(bigpopa_db_path, ifs_static_id, param_names)

    for _, row in df.iterrows():
        switch_value = row.get("Switch")
        if not _is_enabled(switch_value):
            continue

        variable = row.get("Variable") or row.get("Name")
        if not isinstance(variable, str) or not variable.strip():
            continue

        # Policy for Writing Working.sce:
        # DIMENSION1 == 1 -> include "World"
        # DIMENSION1 == 0 -> exclude "World"
        # else -> skip parameter.
        # NOTE: bigpopa.db.parameter.param_type is TEXT and may be values like
        # "1", "1.0", "0.0", "", NULL, etc., so always parse via helper.
        dimension_raw = row.get("Dimension1")
        if dimension_raw is None and variable:
            dimension_raw = db_dimension_map.get(variable.strip().lower())
        dimension_value = parse_dimension_flag(dimension_raw)

        minimum = _normalize_number(row.get("Minimum"))
        maximum = _normalize_number(row.get("Maximum"))
        if minimum is None or maximum is None:
            continue

        random_value = random.uniform(minimum, maximum)
        if math.isnan(random_value):
            continue

        parts = build_custom_parts(variable.strip(), dimension_value, value_count, random_value)
        if parts is None:
            continue

        # record the actual randomized value for logging later
        input_param_used[variable.strip()] = random_value

        lines_to_append.append(",".join(parts))
        appended += 1

    if not lines_to_append:
        return 0, input_param_used

    with sce_path.open("a", encoding="utf-8") as handle:
        for line in lines_to_append:
            handle.write(line + "\n")

    return appended, input_param_used


def _randomize_intercept(value: float) -> float:
    if abs(value) < 1e-12:
        return random.uniform(-1.0, 1.0)
    magnitude = random.uniform(abs(value) * 0.5, abs(value) * 1.5)
    sign = -1.0 if random.random() < 0.5 else 1.0
    return magnitude * sign


def _randomize_slope(value: float) -> Optional[float]:
    if abs(value) < 1e-12:
        return None
    if value > 0:
        low, high = value * 0.8, value * 1.2
    else:
        low, high = value * 1.2, value * 0.8
    return random.uniform(low, high)


def _collect_rows(frames: Iterable[pd.DataFrame]) -> Iterable[Dict[str, Any]]:
    for frame in frames:
        if frame.empty:
            continue
        for _, row in frame.iterrows():
            if not _is_enabled(row.get("Switch")):
                continue
            yield dict(row)


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

    input_path = Path(args.input_file)
    if not input_path.exists():
        log(
            "error",
            "Missing StartingPointTable.xlsx",
            file=str(input_path.resolve()),
        )
        emit_stage_response(
            "error",
            "model_setup",
            "StartingPointTable.xlsx was not found; aborting model setup.",
            {"input_file": str(input_path.resolve())},
        )
        return 1

    log(
        "info",
        "Reading StartingPointTable.xlsx",
        file=str(input_path.resolve()),
    )
    try:
        excel_file = pd.ExcelFile(input_path)
    except Exception as exc:
        excel_file = None
        log(
            "warn",
            "Unable to list Excel sheets",
            file=str(input_path.resolve()),
            error=str(exc),
        )
    else:
        log(
            "debug",
            "Excel sheets available",
            file=str(input_path.resolve()),
            sheets=excel_file.sheet_names,
        )
        excel_file.close()

    ifsv_df = pd.DataFrame()
    for sheet_name in ("IFsVar", "IFsVarTab"):
        try:
            ifsv_df = pd.read_excel(input_path, sheet_name=sheet_name, engine="openpyxl")
        except Exception:
            continue
        if not ifsv_df.empty:
            break

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
            fit_metric, ml_method = _load_ml_text_settings(input_path)
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
            version_payload["ml_method_workbook"] = ml_method.raw_value
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

    sheet_order = ["TablFunc", "AnalFunc"]
    log("debug", "Listing Excel sheets to process", sheets=sheet_order)
    sheets = []
    for sheet_name in sheet_order:
        frame = _load_sheet(input_path, sheet_name)
        frame["SourceSheet"] = sheet_name
        log(
            "debug",
            "Collecting rows from sheet",
            sheet=sheet_name,
            rows=int(frame.shape[0]),
        )
        sheets.append(frame)

    total_rows_collected = sum(int(frame.shape[0]) for frame in sheets if not frame.empty)
    collected_rows = list(_collect_rows(sheets))

    valid_rows = []
    example_rows: List[Dict[str, Any]] = []
    for row in collected_rows:
        func_name = str(row.get("Function Name") or "").strip()
        x_var = str(row.get("XVariable") or "").strip()
        y_var = str(row.get("YVariable") or "").strip()
        coef_name = str(row.get("Coefficient") or "").strip()
        if not (func_name and x_var and y_var and coef_name):
            continue
        valid_rows.append(row)
        if len(example_rows) < 5:
            example: Dict[str, Any] = {
                "func": func_name,
                "xvar": x_var,
                "yvar": y_var,
                "coef": coef_name,
            }
            example_rows.append(example)

    log(
        "info",
        "Regression row summary",
        total_rows=total_rows_collected,
        rows_with_switch=len(collected_rows),
        valid_rows=len(valid_rows),
        examples=example_rows,
    )

    updates: List[Dict[str, Any]] = []
    rows_considered = 0
    input_coef: Dict[str, Dict[str, Dict[str, float]]] = {}

    for row in collected_rows:
        func_name = str(row.get("Function Name") or "").strip()
        x_var = str(row.get("XVariable") or "").strip()
        y_var = str(row.get("YVariable") or "").strip()
        coef_name = str(row.get("Coefficient") or "").strip()

        if not (func_name and x_var and y_var and coef_name):
            log(
                "warn",
                "Skipping row with missing identifiers",
                func=func_name,
                xvar=x_var,
                yvar=y_var,
                coef=coef_name,
            )
            continue

        rows_considered += 1
        input_coef.setdefault(func_name, {}).setdefault(x_var, {}).setdefault(coef_name, None)

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

    bigpopa_db_path = output_root / "bigpopa.db"

    input_param: Dict[str, Any] = {}
    input_coef_defaults: Dict[str, Dict[str, Dict[str, float]]] = {}

    try:
        conn_bp = sqlite3.connect(str(bigpopa_db_path))
        cursor = conn_bp.cursor()
        ensure_bigpopa_schema(cursor)

        # ---------------------------
        # FIXED PARAMETER SELECTION
        # ---------------------------

        # Verify required IFsVar structure
        if "Switch" not in ifsv_df.columns or not any(
            column in ifsv_df.columns for column in ("Name", "Variable", "Name/Variable")
        ):
            raise ValueError(
                "IFsVar sheet must contain a 'Switch' column and at least one "
                "parameter-name column: 'Name', 'Variable', or 'Name/Variable'."
            )

        enabled_param_names = extract_enabled_ifsv_names(ifsv_df)
        input_param = build_input_param_from_defaults(
            cursor, int(ifs_static_id), enabled_param_names
        )

        for func_name, x_map in input_coef.items():
            for x_var, coef_map in x_map.items():
                for coef_name in list(coef_map.keys()):
                    cursor.execute(
                        """
                        SELECT beta_default
                        FROM coefficient
                        WHERE ifs_static_id = ?
                          AND LOWER(function_name) = LOWER(?)
                          AND LOWER(x_name) = LOWER(?)
                          AND LOWER(beta_name) = LOWER(?)
                        ORDER BY reg_seq ASC
                        LIMIT 1
                        """,
                        (ifs_static_id, func_name, x_var, coef_name),
                    )
                    row = cursor.fetchone()
                    if row and row[0] is not None:
                        input_coef_defaults.setdefault(func_name, {}).setdefault(x_var, {})[
                            coef_name
                        ] = float(row[0])
        conn_bp.commit()
    finally:
        try:
            conn_bp.close()
        except Exception:
            pass

    input_coef = input_coef_defaults

    # Extract output_set mapping (Variable → Table) from DataDict sheet
    try:
        data_dict_df = pd.read_excel(input_path, sheet_name="DataDict", engine="openpyxl")
        output_set_used = extract_enabled_output_set(data_dict_df)
    except Exception:
        output_set_used = {}

    output_set = output_set_used

    dataset_id = compute_dataset_id(
        ifs_id=ifs_id,
        input_param=input_param,
        input_coef=input_coef,
        output_set=output_set,
    )

    config_obj = canonical_config(ifs_id, input_param, input_coef, output_set)
    model_id = hash_model_id(config_obj)
    output_dir = output_root / model_id
    output_dir.mkdir(parents=True, exist_ok=True)

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

    extract_compare_path = Path(__file__).resolve().with_name("extract_compare.py")
    extract_args = [
        sys.executable,
        str(extract_compare_path),
        "--ifs-root",
        str(ifs_root),
        "--model-db",
        str(base_run_db_path),
        "--input-file",
        str(input_path),
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
    try:
        extract_proc = subprocess.run(extract_args, check=False)
        extract_return = extract_proc.returncode
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
            "dataset_warning": dataset_warning,
            "dataset_diagnostics": dataset_diagnostics,
        },
    )
    return 0 if extract_return in (None, 0) else int(extract_return)


if __name__ == "__main__":
    sys.exit(main())
