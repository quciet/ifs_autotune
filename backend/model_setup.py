"""Randomize IFs regression coefficients based on a starting point table.

This script is invoked by the desktop shell as part of the "Model Setup"
process. It reads coefficients from ``StartingPointTable.xlsx`` (``TablFunc``
and ``AnalFunc`` sheets) and updates the corresponding entries in
``RUNFILES/Working.run.db``.
"""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

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
    return parser


def _load_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    try:
        return pd.read_excel(path, sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame()


def _is_enabled(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return int(value) == 1
    return str(value).strip() == "1"


def _normalize_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, str) and not value.strip():
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


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

    db_path = Path(args.ifs_root) / "RUNFILES" / "Working.run.db"
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

    print(json.dumps({"status": "success", "updates": updates}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
