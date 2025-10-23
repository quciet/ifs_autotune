"""Log IFs version metadata into the BIGPOPA database."""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Record IFs version metadata in bigpopa.db")
    parser.add_argument("--ifs-root", required=True, help="Path to the IFs root directory")
    parser.add_argument(
        "--output-folder",
        required=True,
        help="Path to the BIGPOPA output folder containing bigpopa.db",
    )
    parser.add_argument("--base-year", type=int, required=True, help="Base year for the model")
    parser.add_argument("--end-year", type=int, required=True, help="End year for the model")
    return parser


def _normalize_version(raw: str) -> str:
    cleaned = re.sub(r"(?i)\bversion\b", "", raw)
    cleaned = cleaned.strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned.replace(" ", "_")
    return cleaned


def _read_version_string(ifs_root: Path) -> str:
    init_db = ifs_root / "IFsInit.db"
    if not init_db.exists():
        raise FileNotFoundError(f"IFsInit.db not found at {init_db}")

    with sqlite3.connect(str(init_db)) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT Value FROM LoadFull WHERE Variable=? ORDER BY rowid DESC LIMIT 1",
                ("ModelVersion$",),
            )
        except sqlite3.OperationalError as exc:
            raise RuntimeError("Unable to query LoadFull table for ModelVersion$") from exc

        row = cursor.fetchone()
        if not row or row[0] is None:
            raise RuntimeError("ModelVersion$ entry not found in LoadFull")
        return str(row[0])


def _ensure_database(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"bigpopa.db not found at {path}")


def _resolve_ifs_databases(ifs_root: Path) -> Tuple[Path, Path]:
    runfiles = ifs_root / "RUNFILES"
    ifs_db = runfiles / "IFs.db"
    ifsvar_db = runfiles / "IFsVar.db"

    missing: list[str] = []
    if not ifs_db.exists():
        missing.append(str(ifs_db))
    if not ifsvar_db.exists():
        missing.append(str(ifsvar_db))

    if missing:
        if len(missing) == 1:
            raise FileNotFoundError(f"Required IFs database not found at {missing[0]}")
        raise FileNotFoundError(
            "Required IFs databases not found: " + ", ".join(missing)
        )

    return ifs_db, ifsvar_db


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        # Some SQLite numeric fields may come back as floats or strings.
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _is_null(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def _normalize_text(value: Any) -> Optional[str]:
    if _is_null(value):
        return None
    text = str(value).strip()
    return text or None


def _prepare_parameter_rows(ifs_static_id: int, frame: pd.DataFrame) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for _, record in frame.iterrows():
        name_text = _normalize_text(record.get("ParameterName"))
        if name_text is None:
            continue

        param_type = _normalize_text(record.get("DIMENSION1"))

        default_value = _coerce_float(record.get("Value"))
        min_value = _coerce_float(record.get("MINIMUM"))
        max_value = _coerce_float(record.get("MAXIMUM"))

        rows.append(
            (
                ifs_static_id,
                name_text,
                param_type,
                default_value,
                min_value,
                max_value,
            )
        )
    return rows


def _prepare_coefficient_rows(ifs_static_id: int, frame: pd.DataFrame) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for _, record in frame.iterrows():
        function_name = _normalize_text(record.get("function_name"))
        if function_name is None:
            continue

        y_name = _normalize_text(record.get("y_name"))
        x_name = _normalize_text(record.get("x_name"))
        reg_seq = _coerce_int(record.get("reg_seq"))
        beta_name = _normalize_text(record.get("beta_name"))
        beta_default = _coerce_float(record.get("beta_default"))

        rows.append(
            (
                ifs_static_id,
                function_name,
                y_name,
                x_name,
                reg_seq,
                beta_name,
                beta_default,
                None,
            )
        )
    return rows


def _populate_real_data(
    cursor: sqlite3.Cursor, ifs_static_id: int, ifs_root: Path
) -> Tuple[int, int]:
    ifs_db, ifsvar_db = _resolve_ifs_databases(ifs_root)
    run_db = ifs_root / "RUNFILES" / "IFsBase.run.db"
    if not run_db.exists():
        raise FileNotFoundError(f"Could not find IFsBase.run.db at {run_db}")

    with sqlite3.connect(str(ifs_db)) as conn:
        parameter_values = pd.read_sql_query(
            "SELECT ParameterName, Value FROM GlobalParameters", conn
        )

    with sqlite3.connect(str(run_db)) as conn:
        coefficient_values = pd.read_sql_query(
            """
            SELECT
                r.Name AS function_name,
                r.OutputName AS y_name,
                r.InputName AS x_name,
                r.Seq AS reg_seq,
                c.Name AS beta_name,
                c.Value AS beta_default
            FROM ifs_reg AS r
            JOIN ifs_reg_coeff AS c
                ON r.Name = c.RegressionName
                AND r.Seq = c.RegressionSeq
            """,
            conn,
        )

    with sqlite3.connect(str(ifsvar_db)) as conn:
        parameter_metadata = pd.read_sql_query(
            "SELECT NAME, DIMENSION1, MINIMUM, MAXIMUM FROM IFSVAR", conn
        )

    merged_parameters = pd.merge(
        parameter_values,
        parameter_metadata,
        how="left",
        left_on="ParameterName",
        right_on="NAME",
    )

    parameter_rows = _prepare_parameter_rows(ifs_static_id, merged_parameters)
    if parameter_rows:
        cursor.executemany(
            """
            INSERT INTO parameter (
                ifs_static_id,
                param_name,
                param_type,
                param_default,
                param_min,
                param_max
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            parameter_rows,
        )

    coefficient_rows = _prepare_coefficient_rows(ifs_static_id, coefficient_values)
    if coefficient_rows:
        cursor.executemany(
            """
            INSERT INTO coefficient (
                ifs_static_id,
                function_name,
                y_name,
                x_name,
                reg_seq,
                beta_name,
                beta_default,
                beta_std
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            coefficient_rows,
        )

    return len(parameter_rows), len(coefficient_rows)


def log_version_metadata(
    *, ifs_root: Path, output_folder: Path, base_year: int, end_year: int
) -> Dict[str, Any]:
    version_raw = _read_version_string(ifs_root)
    version_number = _normalize_version(version_raw)
    fit_metric = "mse"
    ml_method = "neural network"

    db_path = output_folder / "bigpopa.db"
    _ensure_database(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT ifs_static_id
            FROM ifs_static
            WHERE version_number = ? AND base_year = ?
            LIMIT 1
            """,
            (version_number, base_year),
        )
        row = cursor.fetchone()

        num_parameters = 0
        num_coefficients = 0

        if row:
            ifs_static_id = int(row[0])
            cursor.execute(
                "SELECT COUNT(*) FROM parameter WHERE ifs_static_id = ?",
                (ifs_static_id,),
            )
            param_row = cursor.fetchone()
            if param_row and param_row[0] is not None:
                num_parameters = int(param_row[0])

            cursor.execute(
                "SELECT COUNT(*) FROM coefficient WHERE ifs_static_id = ?",
                (ifs_static_id,),
            )
            coeff_row = cursor.fetchone()
            if coeff_row and coeff_row[0] is not None:
                num_coefficients = int(coeff_row[0])
        else:
            cursor.execute(
                """
                INSERT INTO ifs_static (version_number, base_year)
                VALUES (?, ?)
                """,
                (version_number, base_year),
            )
            ifs_static_id = int(cursor.lastrowid)
            num_parameters, num_coefficients = _populate_real_data(
                cursor, ifs_static_id, ifs_root
            )

        cursor.execute(
            """
            SELECT ifs_id FROM ifs_version
            WHERE version_number = ?
              AND base_year = ?
              AND end_year = ?
              AND fit_metric = ?
              AND ml_method = ?
            LIMIT 1
            """,
            (version_number, base_year, end_year, fit_metric, ml_method),
        )
        existing = cursor.fetchone()

        if existing:
            ifs_id = int(existing[0])
            conn.commit()
            return {
                "status": "success",
                "message": "Existing IFs version found — skipping new record.",
                "ifs_id": ifs_id,
                "ifs_static_id": ifs_static_id,
                "version_number": version_number,
                "base_year": base_year,
                "end_year": end_year,
                "fit_metric": fit_metric,
                "ml_method": ml_method,
                "num_parameters": num_parameters,
                "num_coefficients": num_coefficients,
            }

        cursor.execute(
            """
            INSERT INTO ifs_version (
                ifs_static_id, version_number, base_year, end_year, fit_metric, ml_method
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                ifs_static_id,
                version_number,
                base_year,
                end_year,
                fit_metric,
                ml_method,
            ),
        )
        ifs_id = int(cursor.lastrowid)
        conn.commit()
        return {
            "status": "success",
            "message": "Logged IFs version and linked to static layer.",
            "ifs_id": ifs_id,
            "ifs_static_id": ifs_static_id,
            "version_number": version_number,
            "base_year": base_year,
            "end_year": end_year,
            "fit_metric": fit_metric,
            "ml_method": ml_method,
            "num_parameters": num_parameters,
            "num_coefficients": num_coefficients,
        }


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        payload = log_version_metadata(
            ifs_root=Path(args.ifs_root),
            output_folder=Path(args.output_folder),
            base_year=args.base_year,
            end_year=args.end_year,
        )
    except Exception as exc:
        error_payload = {"status": "error", "message": str(exc)}
        print(json.dumps(error_payload))
        sys.stdout.flush()
        return 1

    print(json.dumps(payload))
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
