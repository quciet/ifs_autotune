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
    try:
        return pd.read_excel(path, sheet_name=sheet_name)
    except Exception:
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
        conn = sqlite3.connect(str(db_path))
    except Exception:
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
        except sqlite3.Error:
            return None

        candidate_column_names = {"baseyear", "base_year", "yrbase", "yr_base"}

        for table in tables:
            try:
                cursor.execute(f"PRAGMA table_info(\"{table}\")")
            except sqlite3.Error:
                continue

            columns = cursor.fetchall()
            if not columns:
                continue

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
                continue

            row = cursor.fetchone()
            if not row:
                continue

            value = row[0]
            if value is None:
                continue

            try:
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

        dimension_raw = row.get("DIMENSION1")
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

    ifs_root = Path(args.ifs_root)
    db_path = ifs_root / "RUNFILES" / "Working.run.db"
    if not db_path.exists():
        print(
            json.dumps(
                {
                    "status": "error",
                    "message": f"Missing Working.run.db at {db_path}",
                }
            )
        )
        return 1

    input_path = Path(args.input_file)
    if not input_path.exists():
        print(
            json.dumps(
                {
                    "status": "error",
                    "message": f"Missing StartingPointTable.xlsx at {input_path}",
                }
            )
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
        base_year = _infer_base_year_from_db(db_path)

    if forecast_year is None:
        print(
            json.dumps(
                {
                    "status": "error",
                    "message": "Unable to determine forecast year for Working.sce.",
                }
            )
        )
        return 1

    global _LAST_KNOWN_YEARS
    if base_year is not None:
        _LAST_KNOWN_YEARS = (base_year, forecast_year)
    else:
        _LAST_KNOWN_YEARS = None

    sce_path = create_working_sce(ifs_root)

    sheets = [
        _load_sheet(input_path, "TablFunc"),
        _load_sheet(input_path, "AnalFunc"),
    ]

    updates: List[Dict[str, Any]] = []

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        for row in _collect_rows(sheets):
            func_name = row.get("Function Name")
            x_var = row.get("XVariable")
            y_var = row.get("YVariable")

            if not all(isinstance(value, str) and value.strip() for value in (func_name, x_var, y_var)):
                continue

            cursor.execute(
                "SELECT Seq FROM ifs_reg WHERE Name=? AND InputName=? AND OutputName=?",
                (func_name, x_var, y_var),
            )
            seq_row = cursor.fetchone()
            if not seq_row:
                continue
            seq = seq_row[0]

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
                    continue

                if coef_name == "a":
                    new_value = _randomize_intercept(raw_value)
                else:
                    randomized = _randomize_slope(raw_value)
                    if randomized is None:
                        continue
                    new_value = randomized

                cursor.execute(
                    "UPDATE ifs_reg_coeff SET Value=? WHERE RegressionName=? AND RegressionSeq=? AND Name=?",
                    (float(new_value), func_name, seq, coef_name),
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
    finally:
        try:
            conn.close()
        except Exception:
            pass

    appended_variables = add_from_startingpoint(ifs_root, input_path)
    print(
        json.dumps(
            {
                "status": "success",
                "updates": updates,
                "sce_variables_appended": appended_variables,
                "sce_file": str(sce_path.resolve()),
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
