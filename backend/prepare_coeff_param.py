"""Apply BIGPOPA configuration to IFs working files."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict

from common_sce_utils import build_custom_parts, parse_dimension_flag


def _load_param_dimension_map(
    db_path: Path,
    ifs_static_id: int,
    param_names: list[str],
) -> Dict[str, Any]:
    dimension_map: Dict[str, Any] = {}
    with sqlite3.connect(str(db_path)) as conn:
        cursor = conn.cursor()
        for name in param_names:
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
    return dimension_map


def apply_config_to_ifs_files(
    ifs_root,
    input_param,
    input_coef,
    base_year,
    end_year,
    bigpopa_db_path,
    ifs_static_id,
):
    """Write model config into Working.sce and Working.run.db.

    Working.sce policy is centralized via shared helpers:
    - DIMENSION1 == 1 -> include ``World`` in ``CUSTOM`` lines
    - DIMENSION1 == 0 -> no ``World``
    - otherwise -> skip parameter

    ``bigpopa.db.parameter.param_type`` is stored as TEXT, so values such as
    "1", "1.0", "0.0", empty strings, and NULL are parsed via
    ``parse_dimension_flag``.
    """

    # 1. Write parameters to Scenario/Working.sce
    sce_path = Path(ifs_root) / "Scenario" / "Working.sce"
    sce_path.parent.mkdir(parents=True, exist_ok=True)
    sce_path.unlink(missing_ok=True)

    years = end_year - base_year + 1
    param_names = [str(param).strip() for param in input_param.keys() if str(param).strip()]
    dimension_map = _load_param_dimension_map(Path(bigpopa_db_path), int(ifs_static_id), param_names)

    lines: list[str] = []
    for param, val in input_param.items():
        param_name = str(param).strip()
        if not param_name:
            continue
        dim_flag = parse_dimension_flag(dimension_map.get(param_name.lower()))
        parts = build_custom_parts(param_name, dim_flag, years, float(val))
        if parts is None:
            continue
        lines.append(",".join(parts))

    with sce_path.open("w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")

    # 2. Update coefficients in RUNFILES/Working.run.db
    db_path = Path(ifs_root) / "RUNFILES" / "Working.run.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    for func, x_map in input_coef.items():
        for x_name, beta_map in x_map.items():

            cur.execute(
                """
                SELECT Seq FROM ifs_reg
                WHERE UPPER(Name)=UPPER(?)
                  AND UPPER(InputName)=UPPER(?)
                """,
                (func, x_name),
            )
            seq_row = cur.fetchone()
            if not seq_row:
                continue

            seq = seq_row[0]

            for beta_name, new_val in beta_map.items():
                cur.execute(
                    """
                    UPDATE ifs_reg_coeff
                    SET Value = ?
                    WHERE RegressionName = ?
                      AND RegressionSeq = ?
                      AND Name = ?
                    """,
                    (float(new_val), func, seq, beta_name),
                )

    conn.commit()
    conn.close()
