"""Log IFs version metadata into the BIGPOPA database."""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Optional


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


def _fetch_existing_version(
    cursor: sqlite3.Cursor,
    version_number: str,
    base_year: int,
    end_year: int,
    fit_metric: str,
    ml_method: str,
) -> Optional[int]:
    cursor.execute(
        """
        SELECT ifs_id
        FROM ifs_version
        WHERE
            version_number = ?
            AND base_year = ?
            AND end_year = ?
            AND fit_metric = ?
            AND ml_method = ?
        ORDER BY ifs_id DESC
        LIMIT 1
        """,
        (version_number, base_year, end_year, fit_metric, ml_method),
    )
    row = cursor.fetchone()
    return int(row[0]) if row else None


def _find_latest_matching_version(
    cursor: sqlite3.Cursor, version_number: str
) -> Optional[Dict[str, Any]]:
    cursor.execute(
        """
        SELECT ifs_id, version_number, base_year, end_year, fit_metric, ml_method
        FROM ifs_version
        WHERE version_number = ?
        ORDER BY ifs_id DESC
        LIMIT 1
        """,
        (version_number,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return {
        "ifs_id": int(row[0]),
        "version_number": str(row[1]),
        "base_year": int(row[2]),
        "end_year": int(row[3]),
        "fit_metric": str(row[4]) if row[4] is not None else None,
        "ml_method": str(row[5]) if row[5] is not None else None,
    }


def _insert_new_version(
    cursor: sqlite3.Cursor,
    version_number: str,
    base_year: int,
    end_year: int,
    fit_metric: str,
    ml_method: str,
) -> int:
    cursor.execute(
        """
        INSERT INTO ifs_version (version_number, base_year, end_year, fit_metric, ml_method)
        VALUES (?, ?, ?, ?, ?)
        """,
        (version_number, base_year, end_year, fit_metric, ml_method),
    )
    return int(cursor.lastrowid)


def _insert_placeholders(cursor: sqlite3.Cursor, ifs_id: int) -> None:
    # Insert a single placeholder coefficient row using EXISTING columns
    cursor.execute(
        """
        INSERT INTO coefficient (
            ifs_id,
            function_name,
            y_name,
            x_name,
            reg_seq,
            x_default,
            x_std
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (ifs_id, 'ExampleFunc', 'GDP', 'Capital', 1, 0.0, 1.0),
    )

    # Insert a single placeholder parameter row using EXISTING columns
    cursor.execute(
        """
        INSERT INTO parameter (
            ifs_id,
            param_name,
            param_type,
            param_default,
            param_min,
            param_max
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (ifs_id, 'tfrmin', 'parameter', 1.2, 0.8, 1.5),
    )


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
        latest_match = _find_latest_matching_version(cursor, version_number)

        # Determine if the metadata matches the most recent entry for this version number.
        matches_latest_version = False
        if latest_match is not None:
            latest_fit_metric = latest_match.get("fit_metric") or ""
            latest_ml_method = latest_match.get("ml_method") or ""
            matches_latest_version = (
                latest_match["base_year"] == base_year
                and latest_match["end_year"] == end_year
                and latest_fit_metric == (fit_metric or "")
                and latest_ml_method == (ml_method or "")
            )

        if matches_latest_version:
            # An identical row already exists for this version number; reuse it.
            existing_id = _fetch_existing_version(
                cursor,
                version_number,
                base_year,
                end_year,
                fit_metric,
                ml_method,
            )
            if existing_id is None:
                existing_id = latest_match["ifs_id"]
            conn.commit()
            return {
                "status": "success",
                "message": "IFs version already exists, skipping.",
                "ifs_id": existing_id,
            }

        # Insert a new IFs version row because at least one tracked field differs from the
        # most recent record for this version number (or there is no prior record).
        ifs_id = _insert_new_version(
            cursor, version_number, base_year, end_year, fit_metric, ml_method
        )

        # Coefficient/parameter data is only refreshed when the version identity changes.
        # That happens when we introduce a brand-new version number or when the base year
        # shifts for an existing version. Updates to end_year, fit_metric, or ml_method reuse
        # the existing support table rows.
        version_number_changed = latest_match is None
        base_year_changed = (
            latest_match is not None and latest_match["base_year"] != base_year
        )
        should_register_support_tables = version_number_changed or base_year_changed

        if should_register_support_tables:
            _insert_placeholders(cursor, ifs_id)
        conn.commit()
        return {
            "status": "success",
            "message": "Inserted new IFs version metadata.",
            "ifs_id": ifs_id,
            "version_number": version_number,
            "base_year": base_year,
            "end_year": end_year,
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
