"""Populate static parameter and coefficient tables for a specific IFs version."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Populate the BIGPOPA static parameter and coefficient tables "
            "for a specific IFs version."
        )
    )
    parser.add_argument("--ifs-root", required=True, help="Path to the IFs root folder")
    parser.add_argument(
        "--output-folder",
        required=True,
        help="Path to the BIGPOPA output folder (contains bigpopa.db)",
    )
    parser.add_argument(
        "--version-number",
        required=True,
        help="Normalized IFs version string (e.g., 8.54_IP1)",
    )
    parser.add_argument("--base-year", required=True, type=int, help="Model base year")
    parser.add_argument("--end-year", required=True, type=int, help="Model end year")
    return parser


def _emit(status: str, message: str, **extras: Any) -> None:
    payload: Dict[str, Any] = {"status": status, "message": message}
    if extras:
        payload.update(extras)
    print(json.dumps(payload))
    sys.stdout.flush()


def _resolve_paths(ifs_root: Path) -> Tuple[Path, Path]:
    runfiles = ifs_root / "RUNFILES"
    ifs_db = runfiles / "IFs.db"
    ifsvar_db = runfiles / "IFsVar.db"
    if not ifs_db.exists():
        raise FileNotFoundError(f"IFs.db not found at {ifs_db}")
    if not ifsvar_db.exists():
        raise FileNotFoundError(f"IFsVar.db not found at {ifsvar_db}")
    return ifs_db, ifsvar_db


def _load_parameters(ifs_db: Path, ifsvar_db: Path) -> pd.DataFrame:
    with sqlite3.connect(str(ifs_db)) as conn:
        global_params = pd.read_sql_query(
            "SELECT ParameterName, Value FROM GlobalParameters", conn
        )

    with sqlite3.connect(str(ifsvar_db)) as conn:
        metadata = pd.read_sql_query(
            "SELECT NAME, DIMENSION1, MINIMUM, MAXIMUM FROM IFSVAR", conn
        )

    merged = pd.merge(
        global_params,
        metadata,
        how="left",
        left_on="ParameterName",
        right_on="NAME",
    )
    return merged


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


def _prepare_parameter_rows(ifs_id: int, frame: pd.DataFrame) -> List[Tuple[Any, ...]]:
    rows: List[Tuple[Any, ...]] = []
    for _, record in frame.iterrows():
        name = record.get("ParameterName")
        if name is None:
            continue
        parameter_type = record.get("DIMENSION1")
        default_value = _coerce_float(record.get("Value"))
        min_value = _coerce_float(record.get("MINIMUM"))
        max_value = _coerce_float(record.get("MAXIMUM"))
        rows.append(
            (
                ifs_id,
                str(name),
                parameter_type if pd.notna(parameter_type) else None,
                default_value,
                min_value,
                max_value,
            )
        )
    return rows


def _ensure_database(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"bigpopa.db not found at {db_path}")


def _fetch_existing_ifs_id(
    cursor: sqlite3.Cursor, version_number: str, base_year: int
) -> Optional[int]:
    cursor.execute(
        """
        SELECT ifs_id
        FROM ifs_version
        WHERE version_number = ? AND base_year = ?
        LIMIT 1
        """,
        (version_number, base_year),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return int(row[0])


def _insert_ifs_version(
    cursor: sqlite3.Cursor, version_number: str, base_year: int, end_year: int
) -> int:
    cursor.execute(
        """
        INSERT INTO ifs_version (
            version_number,
            base_year,
            end_year,
            fit_metric,
            ml_method
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (version_number, base_year, end_year, "mse", "neural network"),
    )
    return int(cursor.lastrowid)


def _insert_parameters(cursor: sqlite3.Cursor, rows: Iterable[Tuple[Any, ...]]) -> int:
    rows_list = list(rows)
    if not rows_list:
        return 0
    cursor.executemany(
        """
        INSERT INTO parameter (
            ifs_id,
            name,
            parameter_type,
            default_value,
            min_value,
            max_value
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows_list,
    )
    return len(rows_list)


def _insert_placeholder_coefficients(cursor: sqlite3.Cursor, ifs_id: int) -> int:
    cursor.execute(
        """
        INSERT INTO coefficient (
            ifs_id,
            function_name,
            dependent_variable,
            independent_variable,
            region_id,
            coefficient_value,
            coefficient_std
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (ifs_id, "ExampleFunc", "GDP", "Capital", 1, 0.0, 1.0),
    )
    return 1


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    ifs_root = Path(args.ifs_root).resolve()
    output_folder = Path(args.output_folder).resolve()
    version_number: str = args.version_number
    base_year: int = args.base_year
    end_year: int = args.end_year

    db_path = output_folder / "bigpopa.db"

    try:
        _ensure_database(db_path)
        ifs_db, ifsvar_db = _resolve_paths(ifs_root)
    except Exception as exc:
        _emit("error", str(exc))
        return 1

    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            existing_id = _fetch_existing_ifs_id(cursor, version_number, base_year)
            if existing_id is not None:
                conn.commit()
                _emit(
                    "success",
                    "Existing version and base year found, skipping static layer population.",
                    ifs_id=existing_id,
                )
                return 0

            ifs_id = _insert_ifs_version(cursor, version_number, base_year, end_year)

            parameters_frame = _load_parameters(ifs_db, ifsvar_db)
            parameter_rows = _prepare_parameter_rows(ifs_id, parameters_frame)
            num_parameters = _insert_parameters(cursor, parameter_rows)
            num_coefficients = _insert_placeholder_coefficients(cursor, ifs_id)

            conn.commit()

    except Exception as exc:
        _emit("error", str(exc))
        return 1

    _emit(
        "success",
        "Static layers populated for new IFs version.",
        ifs_id=ifs_id,
        num_parameters=num_parameters,
        num_coefficients=num_coefficients,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
