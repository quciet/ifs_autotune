from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from db.ifs_metadata import ensure_ifs_metadata_schema
from runtime.ml_method import MLMethodConfig, normalize_ml_method
from db.schema import (
    INPUT_PROFILE_COEFFICIENT_TABLE,
    INPUT_PROFILE_ML_SETTINGS_TABLE,
    INPUT_PROFILE_OUTPUT_TABLE,
    INPUT_PROFILE_PARAMETER_TABLE,
    INPUT_PROFILE_TABLE,
    ensure_current_bigpopa_schema,
)


DEFAULT_ML_METHOD = "neural network"
DEFAULT_FIT_METRIC = "mse"
DEFAULT_N_SAMPLE = 200
DEFAULT_N_MAX_ITERATION = 30
DEFAULT_N_CONVERGENCE = 10
DEFAULT_MIN_CONVERGENCE_PCT = 0.01 / 100.0
ALLOWED_FIT_METRICS = {"mse", "r2"}


@dataclass(frozen=True)
class ProfileDimensionConfig:
    minimum: float | None = None
    maximum: float | None = None
    step: float | None = None
    level_count: int | None = None


@dataclass(frozen=True)
class ProfileMLSettings:
    ml_method: MLMethodConfig
    fit_metric: str
    n_sample: int
    n_max_iteration: int
    n_convergence: int
    min_convergence_pct: float


@dataclass(frozen=True)
class ResolvedInputProfile:
    profile_id: int
    ifs_static_id: int
    name: str
    fit_metric: str
    ml_settings: ProfileMLSettings
    input_param: dict[str, float]
    input_coef: dict[str, dict[str, dict[str, float]]]
    output_set: dict[str, str]
    parameter_configs: dict[str, ProfileDimensionConfig]
    coefficient_configs: dict[tuple[str, str, str], ProfileDimensionConfig]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on"}


def _normalize_optional_float(value: object, *, field_name: str, errors: list[str]) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        errors.append(f"{field_name} must be numeric.")
        return None
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        errors.append(f"{field_name} must be finite.")
        return None
    return parsed


def _normalize_optional_int(value: object, *, field_name: str, errors: list[str]) -> int | None:
    parsed = _normalize_optional_float(value, field_name=field_name, errors=errors)
    if parsed is None:
        return None
    if not float(parsed).is_integer():
        errors.append(f"{field_name} must be an integer.")
        return None
    return int(parsed)


def _load_payload_from_stdin() -> Any:
    raw = sys.stdin.read()
    if not raw.strip():
        return None
    return json.loads(raw)


def _resolve_db_path(output_folder: Path | str) -> Path:
    output_root = Path(output_folder).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    return output_root / "bigpopa.db"


def _connect(output_folder: Path | str) -> sqlite3.Connection:
    db_path = _resolve_db_path(output_folder)
    conn = sqlite3.connect(str(db_path))
    ensure_current_bigpopa_schema(conn.cursor())
    ensure_ifs_metadata_schema(conn.cursor())
    conn.commit()
    return conn


def _require_profile_row(cursor: sqlite3.Cursor, profile_id: int) -> sqlite3.Row:
    row = cursor.execute(
        f"""
        SELECT profile_id, ifs_static_id, name, description, created_at_utc, updated_at_utc,
               archived, source_type, source_path
        FROM {INPUT_PROFILE_TABLE}
        WHERE profile_id = ?
        """,
        (profile_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Profile {profile_id} was not found.")
    return row


def _row_value(row: Any, key: str, index: int) -> Any:
    if isinstance(row, sqlite3.Row):
        return row[key]
    return row[index]


def _load_profile_summary(cursor: sqlite3.Cursor, profile_id: int) -> dict[str, Any]:
    row = _require_profile_row(cursor, profile_id)
    return {
        "profile_id": int(_row_value(row, "profile_id", 0)),
        "ifs_static_id": int(_row_value(row, "ifs_static_id", 1)),
        "name": str(_row_value(row, "name", 2)),
        "description": _row_value(row, "description", 3),
        "created_at_utc": _row_value(row, "created_at_utc", 4),
        "updated_at_utc": _row_value(row, "updated_at_utc", 5),
        "archived": bool(_row_value(row, "archived", 6)),
        "source_type": _row_value(row, "source_type", 7),
        "source_path": _row_value(row, "source_path", 8),
    }


def _normalize_ml_settings_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    ml_method_text = str(payload.get("ml_method") or DEFAULT_ML_METHOD).strip()
    fit_metric = str(payload.get("fit_metric") or DEFAULT_FIT_METRIC).strip().lower()
    if fit_metric not in ALLOWED_FIT_METRICS:
        errors.append("fit_metric must be one of: mse, r2.")

    try:
        ml_method = normalize_ml_method(ml_method_text)
    except ValueError as exc:
        errors.append(str(exc))
        ml_method = normalize_ml_method(DEFAULT_ML_METHOD)

    n_sample = _normalize_optional_int(
        payload.get("n_sample", DEFAULT_N_SAMPLE),
        field_name="n_sample",
        errors=errors,
    )
    n_max_iteration = _normalize_optional_int(
        payload.get("n_max_iteration", DEFAULT_N_MAX_ITERATION),
        field_name="n_max_iteration",
        errors=errors,
    )
    n_convergence = _normalize_optional_int(
        payload.get("n_convergence", DEFAULT_N_CONVERGENCE),
        field_name="n_convergence",
        errors=errors,
    )
    min_convergence_pct = _normalize_optional_float(
        payload.get("min_convergence_pct", DEFAULT_MIN_CONVERGENCE_PCT),
        field_name="min_convergence_pct",
        errors=errors,
    )

    if n_sample is not None and n_sample < 1:
        errors.append("n_sample must be at least 1.")
    if n_max_iteration is not None and n_max_iteration < 1:
        errors.append("n_max_iteration must be at least 1.")
    if n_convergence is not None and n_convergence < 1:
        errors.append("n_convergence must be at least 1.")
    if min_convergence_pct is not None and min_convergence_pct < 0:
        errors.append("min_convergence_pct must be greater than or equal to 0.")

    return (
        {
            "ml_method": ml_method.normalized_value,
            "fit_metric": fit_metric if fit_metric in ALLOWED_FIT_METRICS else DEFAULT_FIT_METRIC,
            "n_sample": n_sample if n_sample is not None else DEFAULT_N_SAMPLE,
            "n_max_iteration": (
                n_max_iteration if n_max_iteration is not None else DEFAULT_N_MAX_ITERATION
            ),
            "n_convergence": (
                n_convergence if n_convergence is not None else DEFAULT_N_CONVERGENCE
            ),
            "min_convergence_pct": (
                min_convergence_pct
                if min_convergence_pct is not None
                else DEFAULT_MIN_CONVERGENCE_PCT
            ),
        },
        errors,
    )


def _load_parameter_catalog(cursor: sqlite3.Cursor, ifs_static_id: int) -> list[dict[str, Any]]:
    rows = cursor.execute(
        """
        SELECT param_name, param_type, param_default, param_min, param_max
        FROM parameter
        WHERE ifs_static_id = ?
        ORDER BY LOWER(param_name)
        """,
        (ifs_static_id,),
    ).fetchall()
    return [
        {
            "param_name": str(row[0]),
            "param_type": row[1],
            "param_default": float(row[2]) if row[2] is not None else None,
            "param_min": float(row[3]) if row[3] is not None else None,
            "param_max": float(row[4]) if row[4] is not None else None,
        }
        for row in rows
    ]


def _load_coefficient_catalog(cursor: sqlite3.Cursor, ifs_static_id: int) -> list[dict[str, Any]]:
    rows = cursor.execute(
        """
        SELECT function_name, y_name, x_name, reg_seq, beta_name, beta_default, beta_std
        FROM coefficient
        WHERE ifs_static_id = ?
        ORDER BY LOWER(function_name), LOWER(x_name), LOWER(beta_name), reg_seq
        """,
        (ifs_static_id,),
    ).fetchall()
    return [
        {
            "function_name": str(row[0]),
            "y_name": row[1],
            "x_name": str(row[2]),
            "reg_seq": int(row[3]) if row[3] is not None else None,
            "beta_name": str(row[4]),
            "beta_default": float(row[5]) if row[5] is not None else None,
            "beta_std": float(row[6]) if row[6] is not None else None,
        }
        for row in rows
    ]


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def load_output_catalog(ifs_root: Path | str | None) -> list[dict[str, str]]:
    if ifs_root is None:
        return []

    db_path = Path(ifs_root).expanduser().resolve() / "RUNFILES" / "DataDict.db"
    if not db_path.exists():
        return []

    try:
        conn = sqlite3.connect(str(db_path))
    except sqlite3.Error:
        return []

    try:
        candidates = conn.execute(
            """
            SELECT name, type
            FROM sqlite_master
            WHERE type IN ('table', 'view')
            ORDER BY CASE WHEN LOWER(name) = 'datadict' THEN 0 ELSE 1 END, name
            """
        ).fetchall()
        for candidate_name, _candidate_type in candidates:
            try:
                columns = conn.execute(
                    f"PRAGMA table_info({_quote_identifier(candidate_name)})"
                ).fetchall()
            except sqlite3.Error:
                continue
            variable_column = None
            table_column = None
            for column in columns:
                column_name = str(column[1])
                lowered = column_name.casefold()
                if lowered == "variable":
                    variable_column = column_name
                elif lowered == "table":
                    table_column = column_name
            if not variable_column or not table_column:
                continue
            try:
                rows = conn.execute(
                    f"""
                    SELECT DISTINCT
                        TRIM(CAST({_quote_identifier(variable_column)} AS TEXT)) AS variable,
                        TRIM(CAST({_quote_identifier(table_column)} AS TEXT)) AS table_name
                    FROM {_quote_identifier(candidate_name)}
                    WHERE {_quote_identifier(variable_column)} IS NOT NULL
                      AND {_quote_identifier(table_column)} IS NOT NULL
                    ORDER BY variable, table_name
                    """
                ).fetchall()
            except sqlite3.Error:
                continue
            catalog = [
                {"variable": str(row[0]), "table_name": str(row[1])}
                for row in rows
                if str(row[0]).strip() and str(row[1]).strip()
            ]
            if catalog:
                return catalog
    finally:
        conn.close()

    return []


def list_profiles(
    *,
    output_folder: Path | str,
    ifs_static_id: int,
    include_archived: bool = False,
) -> dict[str, Any]:
    with _connect(output_folder) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        query = (
            f"""
            SELECT p.profile_id, p.ifs_static_id, p.name, p.description, p.created_at_utc,
                   p.updated_at_utc, p.archived, p.source_type, p.source_path,
                   COALESCE(param.enabled_param_count, 0) AS enabled_param_count,
                   COALESCE(coef.enabled_coefficient_count, 0) AS enabled_coefficient_count,
                   COALESCE(outputs.enabled_output_count, 0) AS enabled_output_count
            FROM {INPUT_PROFILE_TABLE} AS p
            LEFT JOIN (
                SELECT profile_id, COUNT(*) AS enabled_param_count
                FROM {INPUT_PROFILE_PARAMETER_TABLE}
                WHERE enabled = 1
                GROUP BY profile_id
            ) AS param ON param.profile_id = p.profile_id
            LEFT JOIN (
                SELECT profile_id, COUNT(*) AS enabled_coefficient_count
                FROM {INPUT_PROFILE_COEFFICIENT_TABLE}
                WHERE enabled = 1
                GROUP BY profile_id
            ) AS coef ON coef.profile_id = p.profile_id
            LEFT JOIN (
                SELECT profile_id, COUNT(*) AS enabled_output_count
                FROM {INPUT_PROFILE_OUTPUT_TABLE}
                WHERE enabled = 1
                GROUP BY profile_id
            ) AS outputs ON outputs.profile_id = p.profile_id
            WHERE p.ifs_static_id = ?
            """
            + ("" if include_archived else " AND p.archived = 0")
            + " ORDER BY LOWER(p.name), p.profile_id"
        )
        rows = cursor.execute(query, (ifs_static_id,)).fetchall()
        profiles = [
            {
                "profile_id": int(row["profile_id"]),
                "ifs_static_id": int(row["ifs_static_id"]),
                "name": str(row["name"]),
                "description": row["description"],
                "created_at_utc": row["created_at_utc"],
                "updated_at_utc": row["updated_at_utc"],
                "archived": bool(row["archived"]),
                "source_type": row["source_type"],
                "source_path": row["source_path"],
                "enabled_param_count": int(row["enabled_param_count"]),
                "enabled_coefficient_count": int(row["enabled_coefficient_count"]),
                "enabled_output_count": int(row["enabled_output_count"]),
            }
            for row in rows
        ]
        return {"profiles": profiles}


def create_profile(
    *,
    output_folder: Path | str,
    ifs_static_id: int,
    name: str,
    description: str | None = None,
) -> dict[str, Any]:
    normalized_name = _normalize_text(name)
    if normalized_name is None:
        raise ValueError("Profile name is required.")

    now = _utc_now_iso()
    with _connect(output_folder) as conn:
        cursor = conn.cursor()
        if (
            cursor.execute(
                "SELECT 1 FROM ifs_static WHERE ifs_static_id = ? LIMIT 1",
                (ifs_static_id,),
            ).fetchone()
            is None
        ):
            raise ValueError(f"IFs static layer {ifs_static_id} was not found.")

        cursor.execute(
            f"""
            INSERT INTO {INPUT_PROFILE_TABLE} (
                ifs_static_id, name, description, created_at_utc, updated_at_utc, archived,
                source_type, source_path
            )
            VALUES (?, ?, ?, ?, ?, 0, 'app', NULL)
            """,
            (ifs_static_id, normalized_name, _normalize_text(description), now, now),
        )
        profile_id = int(cursor.lastrowid)
        cursor.execute(
            f"""
            INSERT INTO {INPUT_PROFILE_ML_SETTINGS_TABLE} (
                profile_id, ml_method, fit_metric, n_sample, n_max_iteration,
                n_convergence, min_convergence_pct
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                profile_id,
                normalize_ml_method(DEFAULT_ML_METHOD).normalized_value,
                DEFAULT_FIT_METRIC,
                DEFAULT_N_SAMPLE,
                DEFAULT_N_MAX_ITERATION,
                DEFAULT_N_CONVERGENCE,
                DEFAULT_MIN_CONVERGENCE_PCT,
            ),
        )
        conn.commit()
    return {"profile": get_profile(output_folder=output_folder, profile_id=profile_id)["profile"]}


def update_profile_meta(
    *,
    output_folder: Path | str,
    profile_id: int,
    name: str | None = None,
    description: str | None = None,
) -> dict[str, Any]:
    now = _utc_now_iso()
    with _connect(output_folder) as conn:
        cursor = conn.cursor()
        profile = _load_profile_summary(cursor, profile_id)
        next_name = _normalize_text(name) if name is not None else profile["name"]
        if next_name is None:
            raise ValueError("Profile name is required.")
        next_description = (
            _normalize_text(description)
            if description is not None
            else profile["description"]
        )
        cursor.execute(
            f"""
            UPDATE {INPUT_PROFILE_TABLE}
            SET name = ?, description = ?, updated_at_utc = ?
            WHERE profile_id = ?
            """,
            (next_name, next_description, now, profile_id),
        )
        conn.commit()
    return {"profile": get_profile(output_folder=output_folder, profile_id=profile_id)["profile"]}


def archive_profile(
    *,
    output_folder: Path | str,
    profile_id: int,
    archived: bool = True,
) -> dict[str, Any]:
    with _connect(output_folder) as conn:
        cursor = conn.cursor()
        _load_profile_summary(cursor, profile_id)
        cursor.execute(
            f"""
            UPDATE {INPUT_PROFILE_TABLE}
            SET archived = ?, updated_at_utc = ?
            WHERE profile_id = ?
            """,
            (1 if archived else 0, _utc_now_iso(), profile_id),
        )
        conn.commit()
    return {"profile": get_profile(output_folder=output_folder, profile_id=profile_id)["profile"]}


def duplicate_profile(
    *,
    output_folder: Path | str,
    profile_id: int,
    name: str,
) -> dict[str, Any]:
    normalized_name = _normalize_text(name)
    if normalized_name is None:
        raise ValueError("Duplicate profile name is required.")

    with _connect(output_folder) as conn:
        cursor = conn.cursor()
        profile = _load_profile_summary(cursor, profile_id)
        created = create_profile(
            output_folder=output_folder,
            ifs_static_id=int(profile["ifs_static_id"]),
            name=normalized_name,
            description=profile["description"],
        )
        duplicated_profile_id = int(created["profile"]["profile_id"])
        cursor.execute(
            f"""
            INSERT INTO {INPUT_PROFILE_PARAMETER_TABLE} (
                profile_id, param_name, enabled, minimum, maximum, step, level_count, sort_order
            )
            SELECT ?, param_name, enabled, minimum, maximum, step, level_count, sort_order
            FROM {INPUT_PROFILE_PARAMETER_TABLE}
            WHERE profile_id = ?
            """,
            (duplicated_profile_id, profile_id),
        )
        cursor.execute(
            f"""
            INSERT INTO {INPUT_PROFILE_COEFFICIENT_TABLE} (
                profile_id, function_name, x_name, beta_name, y_name, source_sheet,
                enabled, minimum, maximum, step, level_count, sort_order
            )
            SELECT ?, function_name, x_name, beta_name, y_name, source_sheet,
                   enabled, minimum, maximum, step, level_count, sort_order
            FROM {INPUT_PROFILE_COEFFICIENT_TABLE}
            WHERE profile_id = ?
            """,
            (duplicated_profile_id, profile_id),
        )
        cursor.execute(
            f"""
            INSERT INTO {INPUT_PROFILE_OUTPUT_TABLE} (
                profile_id, variable, table_name, enabled, sort_order
            )
            SELECT ?, variable, table_name, enabled, sort_order
            FROM {INPUT_PROFILE_OUTPUT_TABLE}
            WHERE profile_id = ?
            """,
            (duplicated_profile_id, profile_id),
        )
        cursor.execute(
            f"""
            INSERT OR REPLACE INTO {INPUT_PROFILE_ML_SETTINGS_TABLE} (
                profile_id, ml_method, fit_metric, n_sample, n_max_iteration,
                n_convergence, min_convergence_pct
            )
            SELECT ?, ml_method, fit_metric, n_sample, n_max_iteration,
                   n_convergence, min_convergence_pct
            FROM {INPUT_PROFILE_ML_SETTINGS_TABLE}
            WHERE profile_id = ?
            """,
            (duplicated_profile_id, profile_id),
        )
        cursor.execute(
            f"""
            UPDATE {INPUT_PROFILE_TABLE}
            SET updated_at_utc = ?
            WHERE profile_id = ?
            """,
            (_utc_now_iso(), duplicated_profile_id),
        )
        conn.commit()
    return {"profile": get_profile(output_folder=output_folder, profile_id=duplicated_profile_id)["profile"]}


def delete_profile(
    *,
    output_folder: Path | str,
    profile_id: int,
) -> dict[str, Any]:
    with _connect(output_folder) as conn:
        cursor = conn.cursor()
        _load_profile_summary(cursor, profile_id)
        cursor.execute(
            f"DELETE FROM {INPUT_PROFILE_PARAMETER_TABLE} WHERE profile_id = ?",
            (profile_id,),
        )
        cursor.execute(
            f"DELETE FROM {INPUT_PROFILE_COEFFICIENT_TABLE} WHERE profile_id = ?",
            (profile_id,),
        )
        cursor.execute(
            f"DELETE FROM {INPUT_PROFILE_OUTPUT_TABLE} WHERE profile_id = ?",
            (profile_id,),
        )
        cursor.execute(
            f"DELETE FROM {INPUT_PROFILE_ML_SETTINGS_TABLE} WHERE profile_id = ?",
            (profile_id,),
        )
        cursor.execute(
            f"DELETE FROM {INPUT_PROFILE_TABLE} WHERE profile_id = ?",
            (profile_id,),
        )
        conn.commit()
    return {"deleted_profile_id": profile_id}


def _parameter_row_should_persist(row: dict[str, Any], errors: list[str]) -> tuple[bool, dict[str, Any]]:
    param_name = _normalize_text(row.get("param_name"))
    if param_name is None:
        errors.append("Each parameter row must include param_name.")
        return False, {}
    enabled = _normalize_bool(row.get("enabled", False))
    minimum = _normalize_optional_float(row.get("minimum"), field_name=f"{param_name}.minimum", errors=errors)
    maximum = _normalize_optional_float(row.get("maximum"), field_name=f"{param_name}.maximum", errors=errors)
    step = _normalize_optional_float(row.get("step"), field_name=f"{param_name}.step", errors=errors)
    level_count = _normalize_optional_int(
        row.get("level_count"),
        field_name=f"{param_name}.level_count",
        errors=errors,
    )
    sort_order = _normalize_optional_int(
        row.get("sort_order"),
        field_name=f"{param_name}.sort_order",
        errors=errors,
    )
    if step is not None and step <= 0:
        errors.append(f"{param_name}.step must be greater than 0.")
    if level_count is not None and level_count < 1:
        errors.append(f"{param_name}.level_count must be at least 1.")
    if minimum is not None and maximum is not None and minimum > maximum:
        errors.append(f"{param_name}.minimum must be less than or equal to maximum.")
    normalized = {
        "param_name": param_name,
        "enabled": enabled,
        "minimum": minimum,
        "maximum": maximum,
        "step": step,
        "level_count": level_count,
        "sort_order": sort_order,
    }
    should_persist = enabled or any(
        normalized[key] is not None for key in ("minimum", "maximum", "step", "level_count", "sort_order")
    )
    return should_persist, normalized


def save_parameters(
    *,
    output_folder: Path | str,
    profile_id: int,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    errors: list[str] = []
    normalized_rows: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    for row in rows:
        should_persist, normalized = _parameter_row_should_persist(row, errors)
        if not normalized:
            continue
        key = normalized["param_name"].casefold()
        if key in seen_names:
            errors.append(f"Duplicate parameter row for {normalized['param_name']}.")
            continue
        seen_names.add(key)
        if should_persist:
            normalized_rows.append(normalized)
    if errors:
        raise ValueError(" ".join(errors))

    with _connect(output_folder) as conn:
        cursor = conn.cursor()
        _load_profile_summary(cursor, profile_id)
        cursor.execute(
            f"DELETE FROM {INPUT_PROFILE_PARAMETER_TABLE} WHERE profile_id = ?",
            (profile_id,),
        )
        cursor.executemany(
            f"""
            INSERT INTO {INPUT_PROFILE_PARAMETER_TABLE} (
                profile_id, param_name, enabled, minimum, maximum, step, level_count, sort_order
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    profile_id,
                    row["param_name"],
                    1 if row["enabled"] else 0,
                    row["minimum"],
                    row["maximum"],
                    row["step"],
                    row["level_count"],
                    row["sort_order"],
                )
                for row in normalized_rows
            ],
        )
        cursor.execute(
            f"UPDATE {INPUT_PROFILE_TABLE} SET updated_at_utc = ? WHERE profile_id = ?",
            (_utc_now_iso(), profile_id),
        )
        conn.commit()
    return get_profile(output_folder=output_folder, profile_id=profile_id)


def _coefficient_row_should_persist(row: dict[str, Any], errors: list[str]) -> tuple[bool, dict[str, Any]]:
    function_name = _normalize_text(row.get("function_name"))
    x_name = _normalize_text(row.get("x_name"))
    beta_name = _normalize_text(row.get("beta_name"))
    if function_name is None or x_name is None or beta_name is None:
        errors.append("Each coefficient row must include function_name, x_name, and beta_name.")
        return False, {}
    enabled = _normalize_bool(row.get("enabled", False))
    minimum = _normalize_optional_float(
        row.get("minimum"),
        field_name=f"{function_name}/{x_name}/{beta_name}.minimum",
        errors=errors,
    )
    maximum = _normalize_optional_float(
        row.get("maximum"),
        field_name=f"{function_name}/{x_name}/{beta_name}.maximum",
        errors=errors,
    )
    step = _normalize_optional_float(
        row.get("step"),
        field_name=f"{function_name}/{x_name}/{beta_name}.step",
        errors=errors,
    )
    level_count = _normalize_optional_int(
        row.get("level_count"),
        field_name=f"{function_name}/{x_name}/{beta_name}.level_count",
        errors=errors,
    )
    sort_order = _normalize_optional_int(
        row.get("sort_order"),
        field_name=f"{function_name}/{x_name}/{beta_name}.sort_order",
        errors=errors,
    )
    if step is not None and step <= 0:
        errors.append(f"{function_name}/{x_name}/{beta_name}.step must be greater than 0.")
    if level_count is not None and level_count < 1:
        errors.append(f"{function_name}/{x_name}/{beta_name}.level_count must be at least 1.")
    if minimum is not None and maximum is not None and minimum > maximum:
        errors.append(
            f"{function_name}/{x_name}/{beta_name}.minimum must be less than or equal to maximum."
        )
    normalized = {
        "function_name": function_name,
        "x_name": x_name,
        "beta_name": beta_name,
        "y_name": _normalize_text(row.get("y_name")),
        "source_sheet": _normalize_text(row.get("source_sheet")),
        "enabled": enabled,
        "minimum": minimum,
        "maximum": maximum,
        "step": step,
        "level_count": level_count,
        "sort_order": sort_order,
    }
    should_persist = enabled or any(
        normalized[key] is not None
        for key in ("minimum", "maximum", "step", "level_count", "sort_order")
    )
    return should_persist, normalized


def save_coefficients(
    *,
    output_folder: Path | str,
    profile_id: int,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    errors: list[str] = []
    normalized_rows: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for row in rows:
        should_persist, normalized = _coefficient_row_should_persist(row, errors)
        if not normalized:
            continue
        key = (
            normalized["function_name"].casefold(),
            normalized["x_name"].casefold(),
            normalized["beta_name"].casefold(),
        )
        if key in seen_keys:
            errors.append(
                "Duplicate coefficient row for "
                f"{normalized['function_name']}/{normalized['x_name']}/{normalized['beta_name']}."
            )
            continue
        seen_keys.add(key)
        if should_persist:
            normalized_rows.append(normalized)
    if errors:
        raise ValueError(" ".join(errors))

    with _connect(output_folder) as conn:
        cursor = conn.cursor()
        _load_profile_summary(cursor, profile_id)
        cursor.execute(
            f"DELETE FROM {INPUT_PROFILE_COEFFICIENT_TABLE} WHERE profile_id = ?",
            (profile_id,),
        )
        cursor.executemany(
            f"""
            INSERT INTO {INPUT_PROFILE_COEFFICIENT_TABLE} (
                profile_id, function_name, x_name, beta_name, y_name, source_sheet,
                enabled, minimum, maximum, step, level_count, sort_order
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    profile_id,
                    row["function_name"],
                    row["x_name"],
                    row["beta_name"],
                    row["y_name"],
                    row["source_sheet"],
                    1 if row["enabled"] else 0,
                    row["minimum"],
                    row["maximum"],
                    row["step"],
                    row["level_count"],
                    row["sort_order"],
                )
                for row in normalized_rows
            ],
        )
        cursor.execute(
            f"UPDATE {INPUT_PROFILE_TABLE} SET updated_at_utc = ? WHERE profile_id = ?",
            (_utc_now_iso(), profile_id),
        )
        conn.commit()
    return get_profile(output_folder=output_folder, profile_id=profile_id)


def save_outputs(
    *,
    output_folder: Path | str,
    profile_id: int,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    errors: list[str] = []
    normalized_rows: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()
    for row in rows:
        variable = _normalize_text(row.get("variable"))
        table_name = _normalize_text(row.get("table_name"))
        if variable is None or table_name is None:
            errors.append("Each output row must include variable and table_name.")
            continue
        enabled = _normalize_bool(row.get("enabled", False))
        sort_order = _normalize_optional_int(
            row.get("sort_order"),
            field_name=f"{variable}/{table_name}.sort_order",
            errors=errors,
        )
        key = (variable.casefold(), table_name.casefold())
        if key in seen_keys:
            errors.append(f"Duplicate output row for {variable}/{table_name}.")
            continue
        seen_keys.add(key)
        if enabled:
            normalized_rows.append(
                {
                    "variable": variable,
                    "table_name": table_name,
                    "enabled": True,
                    "sort_order": sort_order,
                }
            )
    if errors:
        raise ValueError(" ".join(errors))

    with _connect(output_folder) as conn:
        cursor = conn.cursor()
        _load_profile_summary(cursor, profile_id)
        cursor.execute(
            f"DELETE FROM {INPUT_PROFILE_OUTPUT_TABLE} WHERE profile_id = ?",
            (profile_id,),
        )
        cursor.executemany(
            f"""
            INSERT INTO {INPUT_PROFILE_OUTPUT_TABLE} (
                profile_id, variable, table_name, enabled, sort_order
            )
            VALUES (?, ?, ?, 1, ?)
            """,
            [
                (profile_id, row["variable"], row["table_name"], row["sort_order"])
                for row in normalized_rows
            ],
        )
        cursor.execute(
            f"UPDATE {INPUT_PROFILE_TABLE} SET updated_at_utc = ? WHERE profile_id = ?",
            (_utc_now_iso(), profile_id),
        )
        conn.commit()
    return get_profile(output_folder=output_folder, profile_id=profile_id)


def save_ml_settings(
    *,
    output_folder: Path | str,
    profile_id: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    normalized, errors = _normalize_ml_settings_payload(payload)
    if errors:
        raise ValueError(" ".join(errors))

    with _connect(output_folder) as conn:
        cursor = conn.cursor()
        _load_profile_summary(cursor, profile_id)
        cursor.execute(
            f"""
            INSERT OR REPLACE INTO {INPUT_PROFILE_ML_SETTINGS_TABLE} (
                profile_id, ml_method, fit_metric, n_sample, n_max_iteration,
                n_convergence, min_convergence_pct
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                profile_id,
                normalized["ml_method"],
                normalized["fit_metric"],
                normalized["n_sample"],
                normalized["n_max_iteration"],
                normalized["n_convergence"],
                normalized["min_convergence_pct"],
            ),
        )
        cursor.execute(
            f"UPDATE {INPUT_PROFILE_TABLE} SET updated_at_utc = ? WHERE profile_id = ?",
            (_utc_now_iso(), profile_id),
        )
        conn.commit()
    return get_profile(output_folder=output_folder, profile_id=profile_id)


def _load_profile_parameter_rows(cursor: sqlite3.Cursor, profile_id: int) -> list[dict[str, Any]]:
    rows = cursor.execute(
        f"""
        SELECT param_name, enabled, minimum, maximum, step, level_count, sort_order
        FROM {INPUT_PROFILE_PARAMETER_TABLE}
        WHERE profile_id = ?
        ORDER BY COALESCE(sort_order, 2147483647), LOWER(param_name)
        """,
        (profile_id,),
    ).fetchall()
    return [
        {
            "param_name": str(row[0]),
            "enabled": bool(row[1]),
            "minimum": float(row[2]) if row[2] is not None else None,
            "maximum": float(row[3]) if row[3] is not None else None,
            "step": float(row[4]) if row[4] is not None else None,
            "level_count": int(row[5]) if row[5] is not None else None,
            "sort_order": int(row[6]) if row[6] is not None else None,
        }
        for row in rows
    ]


def _load_profile_coefficient_rows(cursor: sqlite3.Cursor, profile_id: int) -> list[dict[str, Any]]:
    rows = cursor.execute(
        f"""
        SELECT function_name, x_name, beta_name, y_name, source_sheet, enabled,
               minimum, maximum, step, level_count, sort_order
        FROM {INPUT_PROFILE_COEFFICIENT_TABLE}
        WHERE profile_id = ?
        ORDER BY COALESCE(sort_order, 2147483647), LOWER(function_name), LOWER(x_name), LOWER(beta_name)
        """,
        (profile_id,),
    ).fetchall()
    return [
        {
            "function_name": str(row[0]),
            "x_name": str(row[1]),
            "beta_name": str(row[2]),
            "y_name": row[3],
            "source_sheet": row[4],
            "enabled": bool(row[5]),
            "minimum": float(row[6]) if row[6] is not None else None,
            "maximum": float(row[7]) if row[7] is not None else None,
            "step": float(row[8]) if row[8] is not None else None,
            "level_count": int(row[9]) if row[9] is not None else None,
            "sort_order": int(row[10]) if row[10] is not None else None,
        }
        for row in rows
    ]


def _load_profile_output_rows(cursor: sqlite3.Cursor, profile_id: int) -> list[dict[str, Any]]:
    rows = cursor.execute(
        f"""
        SELECT variable, table_name, enabled, sort_order
        FROM {INPUT_PROFILE_OUTPUT_TABLE}
        WHERE profile_id = ?
        ORDER BY COALESCE(sort_order, 2147483647), LOWER(variable), LOWER(table_name)
        """,
        (profile_id,),
    ).fetchall()
    return [
        {
            "variable": str(row[0]),
            "table_name": str(row[1]),
            "enabled": bool(row[2]),
            "sort_order": int(row[3]) if row[3] is not None else None,
        }
        for row in rows
    ]


def _load_profile_ml_settings_row(cursor: sqlite3.Cursor, profile_id: int) -> dict[str, Any]:
    row = cursor.execute(
        f"""
        SELECT ml_method, fit_metric, n_sample, n_max_iteration, n_convergence, min_convergence_pct
        FROM {INPUT_PROFILE_ML_SETTINGS_TABLE}
        WHERE profile_id = ?
        """,
        (profile_id,),
    ).fetchone()
    if row is None:
        return {
            "ml_method": normalize_ml_method(DEFAULT_ML_METHOD).normalized_value,
            "fit_metric": DEFAULT_FIT_METRIC,
            "n_sample": DEFAULT_N_SAMPLE,
            "n_max_iteration": DEFAULT_N_MAX_ITERATION,
            "n_convergence": DEFAULT_N_CONVERGENCE,
            "min_convergence_pct": DEFAULT_MIN_CONVERGENCE_PCT,
        }
    return {
        "ml_method": str(row[0]),
        "fit_metric": str(row[1]),
        "n_sample": int(row[2]),
        "n_max_iteration": int(row[3]),
        "n_convergence": int(row[4]),
        "min_convergence_pct": float(row[5]),
    }


def _build_parameter_editor_rows(
    catalog: list[dict[str, Any]],
    selections: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    selection_map = {row["param_name"].casefold(): row for row in selections}
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(catalog):
        selection = selection_map.get(item["param_name"].casefold(), {})
        rows.append(
            {
                **item,
                "enabled": bool(selection.get("enabled", False)),
                "minimum": selection.get("minimum"),
                "maximum": selection.get("maximum"),
                "step": selection.get("step"),
                "level_count": selection.get("level_count"),
                "sort_order": selection.get("sort_order", index),
            }
        )
    return rows


def _build_coefficient_editor_rows(
    catalog: list[dict[str, Any]],
    selections: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    selection_map = {
        (
            row["function_name"].casefold(),
            row["x_name"].casefold(),
            row["beta_name"].casefold(),
        ): row
        for row in selections
    }
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(catalog):
        selection = selection_map.get(
            (
                item["function_name"].casefold(),
                item["x_name"].casefold(),
                item["beta_name"].casefold(),
            ),
            {},
        )
        rows.append(
            {
                **item,
                "source_sheet": selection.get("source_sheet"),
                "enabled": bool(selection.get("enabled", False)),
                "minimum": selection.get("minimum"),
                "maximum": selection.get("maximum"),
                "step": selection.get("step"),
                "level_count": selection.get("level_count"),
                "sort_order": selection.get("sort_order", index),
            }
        )
    return rows


def _build_output_editor_rows(
    catalog: list[dict[str, str]],
    selections: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for index, item in enumerate(catalog):
        key = (item["variable"].casefold(), item["table_name"].casefold())
        merged[key] = {
            "variable": item["variable"],
            "table_name": item["table_name"],
            "enabled": False,
            "sort_order": index,
        }
    for index, item in enumerate(selections):
        key = (item["variable"].casefold(), item["table_name"].casefold())
        existing = merged.get(
            key,
            {
                "variable": item["variable"],
                "table_name": item["table_name"],
                "enabled": False,
                "sort_order": index,
            },
        )
        existing["enabled"] = bool(item.get("enabled", False))
        existing["sort_order"] = item.get("sort_order", existing["sort_order"])
        merged[key] = existing
    return list(
        sorted(
            merged.values(),
            key=lambda row: (row["sort_order"], row["variable"], row["table_name"]),
        )
    )


def _validate_profile_data(
    *,
    ifs_static_id: int,
    parameter_catalog: list[dict[str, Any]],
    coefficient_catalog: list[dict[str, Any]],
    parameter_rows: list[dict[str, Any]],
    coefficient_rows: list[dict[str, Any]],
    output_rows: list[dict[str, Any]],
    ml_settings_row: dict[str, Any],
) -> dict[str, Any]:
    errors: list[str] = []

    parameter_catalog_map = {
        item["param_name"].casefold(): item for item in parameter_catalog
    }
    coefficient_catalog_map = {
        (
            item["function_name"].casefold(),
            item["x_name"].casefold(),
            item["beta_name"].casefold(),
        ): item
        for item in coefficient_catalog
    }

    enabled_param_count = 0
    for row in parameter_rows:
        if not row["enabled"]:
            continue
        enabled_param_count += 1
        if row["param_name"].casefold() not in parameter_catalog_map:
            errors.append(
                f"Parameter '{row['param_name']}' does not exist in IFs static layer {ifs_static_id}."
            )
        if row["step"] is not None and row["step"] <= 0:
            errors.append(f"Step for parameter '{row['param_name']}' must be greater than 0.")
        if row["level_count"] is not None and row["level_count"] < 1:
            errors.append(
                f"LevelCount for parameter '{row['param_name']}' must be at least 1."
            )
        if (
            row["minimum"] is not None
            and row["maximum"] is not None
            and row["minimum"] > row["maximum"]
        ):
            errors.append(
                f"Minimum for parameter '{row['param_name']}' must be less than or equal to maximum."
            )

    enabled_coefficient_count = 0
    for row in coefficient_rows:
        if not row["enabled"]:
            continue
        enabled_coefficient_count += 1
        key = (
            row["function_name"].casefold(),
            row["x_name"].casefold(),
            row["beta_name"].casefold(),
        )
        if key not in coefficient_catalog_map:
            errors.append(
                "Coefficient "
                f"'{row['function_name']}/{row['x_name']}/{row['beta_name']}' "
                f"does not exist in IFs static layer {ifs_static_id}."
            )
        if row["step"] is not None and row["step"] <= 0:
            errors.append(
                "Step for coefficient "
                f"'{row['function_name']}/{row['x_name']}/{row['beta_name']}' "
                "must be greater than 0."
            )
        if row["level_count"] is not None and row["level_count"] < 1:
            errors.append(
                "LevelCount for coefficient "
                f"'{row['function_name']}/{row['x_name']}/{row['beta_name']}' "
                "must be at least 1."
            )
        if (
            row["minimum"] is not None
            and row["maximum"] is not None
            and row["minimum"] > row["maximum"]
        ):
            errors.append(
                "Minimum for coefficient "
                f"'{row['function_name']}/{row['x_name']}/{row['beta_name']}' "
                "must be less than or equal to maximum."
            )

    enabled_outputs = [row for row in output_rows if row["enabled"]]
    if not enabled_outputs:
        errors.append("At least one enabled output variable is required.")

    normalized_ml_settings, ml_errors = _normalize_ml_settings_payload(ml_settings_row)
    errors.extend(ml_errors)

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "enabled_param_count": enabled_param_count,
        "enabled_coefficient_count": enabled_coefficient_count,
        "enabled_output_count": len(enabled_outputs),
        "ml_settings": normalized_ml_settings,
    }


def get_profile(
    *,
    output_folder: Path | str,
    profile_id: int,
    ifs_root: Path | str | None = None,
) -> dict[str, Any]:
    with _connect(output_folder) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        profile = _load_profile_summary(cursor, profile_id)
        parameter_catalog = _load_parameter_catalog(cursor, int(profile["ifs_static_id"]))
        coefficient_catalog = _load_coefficient_catalog(cursor, int(profile["ifs_static_id"]))
        parameter_rows = _load_profile_parameter_rows(cursor, profile_id)
        coefficient_rows = _load_profile_coefficient_rows(cursor, profile_id)
        output_rows = _load_profile_output_rows(cursor, profile_id)
        ml_settings_row = _load_profile_ml_settings_row(cursor, profile_id)
        output_catalog = load_output_catalog(ifs_root)
        validation = _validate_profile_data(
            ifs_static_id=int(profile["ifs_static_id"]),
            parameter_catalog=parameter_catalog,
            coefficient_catalog=coefficient_catalog,
            parameter_rows=parameter_rows,
            coefficient_rows=coefficient_rows,
            output_rows=output_rows,
            ml_settings_row=ml_settings_row,
        )

        return {
            "profile": profile,
            "parameter_catalog": _build_parameter_editor_rows(parameter_catalog, parameter_rows),
            "coefficient_catalog": _build_coefficient_editor_rows(
                coefficient_catalog, coefficient_rows
            ),
            "output_catalog": _build_output_editor_rows(output_catalog, output_rows),
            "ml_settings": validation["ml_settings"],
            "validation": {
                "valid": bool(validation["valid"]),
                "errors": list(validation["errors"]),
                "enabled_param_count": int(validation["enabled_param_count"]),
                "enabled_coefficient_count": int(validation["enabled_coefficient_count"]),
                "enabled_output_count": int(validation["enabled_output_count"]),
            },
        }


def validate_profile(
    *,
    output_folder: Path | str,
    profile_id: int,
    ifs_root: Path | str | None = None,
) -> dict[str, Any]:
    detail = get_profile(output_folder=output_folder, profile_id=profile_id, ifs_root=ifs_root)
    return {
        "profile": detail["profile"],
        "validation": detail["validation"],
        "ml_settings": detail["ml_settings"],
    }


def resolve_profile(
    *,
    output_folder: Path | str,
    profile_id: int,
    ifs_root: Path | str | None = None,
) -> ResolvedInputProfile:
    with _connect(output_folder) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        profile = _load_profile_summary(cursor, profile_id)
        if bool(profile["archived"]):
            raise ValueError(f"Profile {profile_id} is archived and cannot be used for runs.")
        parameter_catalog = _load_parameter_catalog(cursor, int(profile["ifs_static_id"]))
        coefficient_catalog = _load_coefficient_catalog(cursor, int(profile["ifs_static_id"]))
        parameter_rows = _load_profile_parameter_rows(cursor, profile_id)
        coefficient_rows = _load_profile_coefficient_rows(cursor, profile_id)
        output_rows = _load_profile_output_rows(cursor, profile_id)
        ml_settings_row = _load_profile_ml_settings_row(cursor, profile_id)
        validation = _validate_profile_data(
            ifs_static_id=int(profile["ifs_static_id"]),
            parameter_catalog=parameter_catalog,
            coefficient_catalog=coefficient_catalog,
            parameter_rows=parameter_rows,
            coefficient_rows=coefficient_rows,
            output_rows=output_rows,
            ml_settings_row=ml_settings_row,
        )
        if not validation["valid"]:
            raise ValueError(" ".join(validation["errors"]))

        parameter_catalog_map = {
            item["param_name"].casefold(): item for item in parameter_catalog
        }
        coefficient_catalog_map = {
            (
                item["function_name"].casefold(),
                item["x_name"].casefold(),
                item["beta_name"].casefold(),
            ): item
            for item in coefficient_catalog
        }

        input_param: dict[str, float] = {}
        parameter_configs: dict[str, ProfileDimensionConfig] = {}
        for row in parameter_rows:
            if not row["enabled"]:
                continue
            catalog_item = parameter_catalog_map[row["param_name"].casefold()]
            default_value = catalog_item["param_default"]
            if default_value is None:
                raise ValueError(
                    f"Parameter '{row['param_name']}' is enabled but has no default value."
                )
            input_param[row["param_name"]] = float(default_value)
            parameter_configs[row["param_name"]] = ProfileDimensionConfig(
                minimum=row["minimum"],
                maximum=row["maximum"],
                step=row["step"],
                level_count=row["level_count"],
            )

        input_coef: dict[str, dict[str, dict[str, float]]] = {}
        coefficient_configs: dict[tuple[str, str, str], ProfileDimensionConfig] = {}
        for row in coefficient_rows:
            if not row["enabled"]:
                continue
            key = (
                row["function_name"].casefold(),
                row["x_name"].casefold(),
                row["beta_name"].casefold(),
            )
            catalog_item = coefficient_catalog_map[key]
            default_value = catalog_item["beta_default"]
            if default_value is None:
                raise ValueError(
                    "Coefficient "
                    f"'{row['function_name']}/{row['x_name']}/{row['beta_name']}' "
                    "is enabled but has no default value."
                )
            input_coef.setdefault(row["function_name"], {}).setdefault(row["x_name"], {})[
                row["beta_name"]
            ] = float(default_value)
            coefficient_configs[
                (row["function_name"], row["x_name"], row["beta_name"])
            ] = ProfileDimensionConfig(
                minimum=row["minimum"],
                maximum=row["maximum"],
                step=row["step"],
                level_count=row["level_count"],
            )

        output_set = {
            row["variable"]: row["table_name"] for row in output_rows if row["enabled"]
        }
        normalized_ml_settings = validation["ml_settings"]
        ml_method = normalize_ml_method(normalized_ml_settings["ml_method"])
        return ResolvedInputProfile(
            profile_id=int(profile["profile_id"]),
            ifs_static_id=int(profile["ifs_static_id"]),
            name=str(profile["name"]),
            fit_metric=str(normalized_ml_settings["fit_metric"]),
            ml_settings=ProfileMLSettings(
                ml_method=ml_method,
                fit_metric=str(normalized_ml_settings["fit_metric"]),
                n_sample=int(normalized_ml_settings["n_sample"]),
                n_max_iteration=int(normalized_ml_settings["n_max_iteration"]),
                n_convergence=int(normalized_ml_settings["n_convergence"]),
                min_convergence_pct=float(normalized_ml_settings["min_convergence_pct"]),
            ),
            input_param=input_param,
            input_coef=input_coef,
            output_set=output_set,
            parameter_configs=parameter_configs,
            coefficient_configs=coefficient_configs,
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage BIGPOPA input profiles.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list")
    list_parser.add_argument("--output-folder", required=True)
    list_parser.add_argument("--ifs-static-id", required=True, type=int)
    list_parser.add_argument("--include-archived", action="store_true")

    create_parser = subparsers.add_parser("create")
    create_parser.add_argument("--output-folder", required=True)
    create_parser.add_argument("--ifs-static-id", required=True, type=int)
    create_parser.add_argument("--name", required=True)
    create_parser.add_argument("--description")

    get_parser = subparsers.add_parser("get")
    get_parser.add_argument("--output-folder", required=True)
    get_parser.add_argument("--profile-id", required=True, type=int)
    get_parser.add_argument("--ifs-root")

    update_meta_parser = subparsers.add_parser("update-meta")
    update_meta_parser.add_argument("--output-folder", required=True)
    update_meta_parser.add_argument("--profile-id", required=True, type=int)
    update_meta_parser.add_argument("--name")
    update_meta_parser.add_argument("--description")

    archive_parser = subparsers.add_parser("archive")
    archive_parser.add_argument("--output-folder", required=True)
    archive_parser.add_argument("--profile-id", required=True, type=int)
    archive_parser.add_argument("--archived", default="true")

    duplicate_parser = subparsers.add_parser("duplicate")
    duplicate_parser.add_argument("--output-folder", required=True)
    duplicate_parser.add_argument("--profile-id", required=True, type=int)
    duplicate_parser.add_argument("--name", required=True)

    delete_parser = subparsers.add_parser("delete")
    delete_parser.add_argument("--output-folder", required=True)
    delete_parser.add_argument("--profile-id", required=True, type=int)

    save_parameters_parser = subparsers.add_parser("save-parameters")
    save_parameters_parser.add_argument("--output-folder", required=True)
    save_parameters_parser.add_argument("--profile-id", required=True, type=int)
    save_parameters_parser.add_argument("--stdin-json", action="store_true")

    save_coefficients_parser = subparsers.add_parser("save-coefficients")
    save_coefficients_parser.add_argument("--output-folder", required=True)
    save_coefficients_parser.add_argument("--profile-id", required=True, type=int)
    save_coefficients_parser.add_argument("--stdin-json", action="store_true")

    save_outputs_parser = subparsers.add_parser("save-outputs")
    save_outputs_parser.add_argument("--output-folder", required=True)
    save_outputs_parser.add_argument("--profile-id", required=True, type=int)
    save_outputs_parser.add_argument("--stdin-json", action="store_true")

    save_ml_parser = subparsers.add_parser("save-ml-settings")
    save_ml_parser.add_argument("--output-folder", required=True)
    save_ml_parser.add_argument("--profile-id", required=True, type=int)
    save_ml_parser.add_argument("--stdin-json", action="store_true")

    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("--output-folder", required=True)
    validate_parser.add_argument("--profile-id", required=True, type=int)
    validate_parser.add_argument("--ifs-root")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "list":
            payload = list_profiles(
                output_folder=args.output_folder,
                ifs_static_id=args.ifs_static_id,
                include_archived=bool(args.include_archived),
            )
        elif args.command == "create":
            payload = create_profile(
                output_folder=args.output_folder,
                ifs_static_id=args.ifs_static_id,
                name=args.name,
                description=args.description,
            )
        elif args.command == "get":
            payload = get_profile(
                output_folder=args.output_folder,
                profile_id=args.profile_id,
                ifs_root=args.ifs_root,
            )
        elif args.command == "update-meta":
            payload = update_profile_meta(
                output_folder=args.output_folder,
                profile_id=args.profile_id,
                name=args.name,
                description=args.description,
            )
        elif args.command == "archive":
            payload = archive_profile(
                output_folder=args.output_folder,
                profile_id=args.profile_id,
                archived=_normalize_bool(args.archived),
            )
        elif args.command == "duplicate":
            payload = duplicate_profile(
                output_folder=args.output_folder,
                profile_id=args.profile_id,
                name=args.name,
            )
        elif args.command == "delete":
            payload = delete_profile(
                output_folder=args.output_folder,
                profile_id=args.profile_id,
            )
        elif args.command == "save-parameters":
            rows = _load_payload_from_stdin() if args.stdin_json else []
            payload = save_parameters(
                output_folder=args.output_folder,
                profile_id=args.profile_id,
                rows=rows or [],
            )
        elif args.command == "save-coefficients":
            rows = _load_payload_from_stdin() if args.stdin_json else []
            payload = save_coefficients(
                output_folder=args.output_folder,
                profile_id=args.profile_id,
                rows=rows or [],
            )
        elif args.command == "save-outputs":
            rows = _load_payload_from_stdin() if args.stdin_json else []
            payload = save_outputs(
                output_folder=args.output_folder,
                profile_id=args.profile_id,
                rows=rows or [],
            )
        elif args.command == "save-ml-settings":
            settings_payload = _load_payload_from_stdin() if args.stdin_json else {}
            payload = save_ml_settings(
                output_folder=args.output_folder,
                profile_id=args.profile_id,
                payload=settings_payload or {},
            )
        elif args.command == "validate":
            payload = validate_profile(
                output_folder=args.output_folder,
                profile_id=args.profile_id,
                ifs_root=args.ifs_root,
            )
        else:
            raise ValueError(f"Unsupported command: {args.command}")
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 1

    print(json.dumps({"ok": True, "data": payload}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
