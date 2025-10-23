"""Randomize IFs regression coefficients based on a starting point table.

This script is invoked by the desktop shell as part of the "Model Setup"
process. It reads coefficients from ``StartingPointTable.xlsx`` (``TablFunc``
and ``AnalFunc`` sheets) and updates the corresponding entries in
``RUNFILES/Working.run.db``.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from log_ifs_version import log_version_metadata

import pandas as pd


COEFFICIENT_COLUMNS: List[str] = [
    "a",
    "b1",
    "b2",
    "b3",
    "b4",
    "b5",
    "b6",
    "b7",
    "b8",
    "b9",
]


def log(status: str, message: str, **kwargs: Any) -> None:
    payload: Dict[str, Any] = {"status": status, "message": message}
    if kwargs:
        payload.update(kwargs)
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


def add_from_startingpoint(ifs_root: Path, excel_path: Path) -> int:
    sce_path = ifs_root / "Scenario" / "Working.sce"
    if not sce_path.exists():
        return 0

    years = _extract_years_from_sce(sce_path)
    if not years:
        return 0
    base_year, forecast_year = years
    if forecast_year < base_year:
        return 0

    try:
        df = pd.read_excel(excel_path, sheet_name="IFsVar", engine="openpyxl")
    except Exception:
        return 0

    if df.empty:
        return 0

    value_count = forecast_year - base_year + 1
    if value_count <= 0:
        return 0

    lines_to_append: List[str] = []
    appended = 0

    for _, row in df.iterrows():
        switch_value = row.get("Switch")
        if not _is_enabled(switch_value):
            continue

        variable = row.get("Variable") or row.get("Name")
        if not isinstance(variable, str) or not variable.strip():
            continue

        dimension_raw = row.get("Dimension1")
        try:
            dimension_value = int(dimension_raw)
        except (TypeError, ValueError):
            dimension_value = None

        minimum = _normalize_number(row.get("Minimum"))
        maximum = _normalize_number(row.get("Maximum"))
        if minimum is None or maximum is None:
            continue

        midpoint = (minimum + maximum) / 2.0
        if math.isnan(midpoint):
            continue

        value_str = f"{midpoint:.6f}".rstrip("0").rstrip(".")
        if not value_str:
            value_str = "0"

        repeated_values = [value_str] * value_count

        if dimension_value == 1:
            parts = ["CUSTOM", variable.strip(), "World", *repeated_values]
        else:
            parts = ["CUSTOM", variable.strip(), *repeated_values]

        lines_to_append.append(",".join(parts))
        appended += 1

    if not lines_to_append:
        return 0

    with sce_path.open("a", encoding="utf-8") as handle:
        for line in lines_to_append:
            handle.write(line + "\n")

    return appended


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
    db_path = ifs_root / "RUNFILES" / "Working.run.db"
    if not db_path.exists():
        log(
            "error",
            "Missing Working.run.db",
            database=str(db_path.resolve()),
        )
        return 1

    input_path = Path(args.input_file)
    if not input_path.exists():
        log(
            "error",
            "Missing StartingPointTable.xlsx",
            file=str(input_path.resolve()),
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

    existing_sce_path = ifs_root / "Scenario" / "Working.sce"
    existing_years = _extract_years_from_sce(existing_sce_path)

    base_year: Optional[int] = args.base_year
    forecast_year: Optional[int] = args.end_year

    if forecast_year is None and existing_years:
        forecast_year = existing_years[1]

    if base_year is None and existing_years:
        base_year = existing_years[0]

    if base_year is None:
        base_year = _infer_base_year_from_db(db_path)

    if forecast_year is None:
        log(
            "error",
            "Unable to determine forecast year for Working.sce.",
        )
        return 1

    global _LAST_KNOWN_YEARS
    if base_year is not None:
        _LAST_KNOWN_YEARS = (base_year, forecast_year)
    else:
        _LAST_KNOWN_YEARS = None

    log(
        "debug",
        "Version metadata condition check",
        output_folder=args.output_folder,
        base_year=base_year,
        forecast_year=forecast_year,
    )
    if args.output_folder and base_year is not None:
        try:
            version_payload = log_version_metadata(
                ifs_root=ifs_root,
                output_folder=Path(args.output_folder),
                base_year=base_year,
                end_year=forecast_year,
            )
        except Exception as exc:
            log(
                "warn",
                "Failed to record IFs version metadata",
                error=str(exc),
            )
        else:
            log(
                "info",
                "Static layer linked",
                ifs_static_id=version_payload.get("ifs_static_id"),
            )
            # Clean up version_payload to avoid duplicate keys
            version_payload.pop("status", None)
            version_payload.pop("message", None)
            log("info", "IFs version metadata recorded", **version_payload)
    else:
        log(
            "warn",
            "Skipping bigpopa.db registration because output_folder or base_year is missing",
            output_folder=args.output_folder,
            base_year=base_year,
        )

    log("info", "Creating Working.sce for parameters")
    sce_path = create_working_sce(ifs_root)

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
        if not (func_name and x_var and y_var):
            continue
        valid_rows.append(row)
        if len(example_rows) < 5:
            example: Dict[str, Any] = {
                "func": func_name,
                "xvar": x_var,
                "yvar": y_var,
            }
            for coef_name in COEFFICIENT_COLUMNS:
                example[coef_name] = _normalize_number(row.get(coef_name))
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
    rows_matched = 0
    coefs_updated = 0

    try:
        log(
            "info",
            "Connecting to Working.run.db",
            database=str(db_path.resolve()),
        )
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        log("debug", "Tables found in database", tables=tables)

        for table in tables:
            try:
                cursor.execute(f"PRAGMA table_info(\"{table}\")")
            except sqlite3.Error as exc:
                log(
                    "warn",
                    "Failed to inspect table columns",
                    table=table,
                    error=str(exc),
                )
                continue
            columns = [column_row[1] for column_row in cursor.fetchall()]
            log(
                "debug",
                "Columns found in table",
                table=table,
                columns=columns,
            )

        log("info", "Updating coefficients in Working.run.db")

        intercept_cache: Dict[Tuple[str, str], float] = {}

        for row in collected_rows:
            func_name = str(row.get("Function Name") or "").strip()
            x_var = str(row.get("XVariable") or "").strip()
            y_var = str(row.get("YVariable") or "").strip()

            if not (func_name and x_var and y_var):
                log(
                    "warn",
                    "Skipping row with missing identifiers",
                    func=func_name,
                    xvar=x_var,
                    yvar=y_var,
                )
                continue

            rows_considered += 1

            log(
                "debug",
                "Processing regression row",
                func=func_name,
                xvar=x_var,
                yvar=y_var,
            )

            cursor.execute(
                "SELECT Seq FROM ifs_reg WHERE UPPER(Name)=UPPER(?) AND UPPER(InputName)=UPPER(?) AND UPPER(OutputName)=UPPER(?)",
                (func_name, x_var, y_var),
            )
            seq_row = cursor.fetchone()

            if not seq_row:
                log(
                    "warn",
                    "No match in ifs_reg",
                    func=func_name,
                    xvar=x_var,
                    yvar=y_var,
                )
                continue

            seq = seq_row[0]
            log(
                "debug",
                "Found Seq",
                func=func_name,
                xvar=x_var,
                yvar=y_var,
                seq=seq,
            )
            rows_matched += 1

            for coef_name in COEFFICIENT_COLUMNS:
                raw_value = _normalize_number(row.get(coef_name))
                if raw_value is None:
                    continue

                cursor.execute(
                    "SELECT Value FROM ifs_reg_coeff WHERE RegressionName=? AND RegressionSeq=? AND Name=?",
                    (func_name, seq, coef_name),
                )
                existing = cursor.fetchone()
                if existing is None:
                    log(
                        "warn",
                        "Missing coefficient row",
                        func=func_name,
                        seq=seq,
                        coef=coef_name,
                    )
                    continue

                if coef_name == "a":
                    is_anal_func = str(row.get("SourceSheet", "")).lower() == "analfunc"
                    key = (func_name, y_var)
                    if is_anal_func:
                        if key in intercept_cache:
                            new_value = intercept_cache[key]
                        else:
                            new_value = _randomize_intercept(raw_value)
                            intercept_cache[key] = new_value
                    else:
                        new_value = _randomize_intercept(raw_value)
                else:
                    randomized = _randomize_slope(raw_value)
                    if randomized is None:
                        continue
                    new_value = randomized

                log(
                    "debug",
                    "Updating coefficient",
                    func=func_name,
                    seq=seq,
                    coef=coef_name,
                    new_value=float(new_value),
                )
                cursor.execute(
                    "UPDATE ifs_reg_coeff SET Value=? WHERE RegressionName=? AND RegressionSeq=? AND Name=?",
                    (float(new_value), func_name, seq, coef_name),
                )
                coefs_updated += 1
                log(
                    "debug",
                    "Updated regression coefficient",
                    function=func_name,
                    seq=seq,
                    coefficient=coef_name,
                    x_variable=x_var,
                    y_variable=y_var,
                    old=float(existing[0]),
                    new=float(new_value),
                )
                updates.append(
                    {
                        "Function": func_name,
                        "XVariable": x_var,
                        "YVariable": y_var,
                        "Seq": seq,
                        "Coefficient": coef_name,
                        "OldValue": float(existing[0]),
                        "NewValue": float(new_value),
                    }
                )

        conn.commit()
        log(
            "info",
            "Committed coefficient updates",
            total_updates=len(updates),
        )
        log(
            "info",
            "Summary",
            rows_considered=rows_considered,
            rows_matched=rows_matched,
            coefs_updated=coefs_updated,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    appended_variables = add_from_startingpoint(ifs_root, input_path)
    log(
        "success",
        "Model Setup completed successfully",
        updates=updates,
        sce_variables_appended=appended_variables,
        sce_file=str(sce_path.resolve()),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
