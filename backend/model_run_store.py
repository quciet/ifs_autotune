from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from model_status import MODEL_REUSED, fit_is_missing, visible_fit_pooled
from tools.db.bigpopa_schema import MODEL_RUN_TABLE, ensure_current_bigpopa_schema

LEGACY_SOURCE_MODEL_INPUT = "model_input"
LEGACY_SOURCE_MODEL_OUTPUT = "model_output"
LEGACY_SOURCE_PROPOSAL_HISTORY = "ml_proposal_history"


@dataclass(frozen=True)
class ModelDefinition:
    ifs_id: int
    model_id: str
    dataset_id: str | None
    input_param: dict[str, Any]
    input_coef: dict[str, Any]
    output_set: dict[str, Any]


@dataclass(frozen=True)
class ModelRunRow:
    run_id: int
    ifs_id: int | None
    model_id: str
    dataset_id: str | None
    input_param: dict[str, Any]
    input_coef: dict[str, Any]
    output_set: dict[str, Any]
    model_status: str | None
    fit_var: str | None
    fit_pooled: float | None
    trial_index: int | None
    batch_index: int | None
    started_at_utc: str | None
    completed_at_utc: str | None
    was_reused: bool
    source_status: str | None
    resolution_note: str | None


def _parse_json_dict(value: object) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def insert_model_run(
    conn: sqlite3.Connection,
    *,
    ifs_id: int | None,
    model_id: str,
    dataset_id: str | None,
    input_param: dict[str, Any],
    input_coef: dict[str, Any],
    output_set: dict[str, Any],
    model_status: str | None = None,
    fit_var: str | None = None,
    fit_pooled: float | None = None,
    trial_index: int | None = None,
    batch_index: int | None = None,
    started_at_utc: str | None = None,
    completed_at_utc: str | None = None,
    was_reused: bool = False,
    source_status: str | None = None,
    resolution_note: str | None = None,
    legacy_source: str | None = None,
    legacy_source_id: int | None = None,
) -> int:
    ensure_current_bigpopa_schema(conn.cursor())
    cursor = conn.cursor()
    cursor.execute(
        f"""
        INSERT INTO {MODEL_RUN_TABLE} (
            ifs_id,
            model_id,
            dataset_id,
            input_param,
            input_coef,
            output_set,
            model_status,
            fit_var,
            fit_pooled,
            trial_index,
            batch_index,
            started_at_utc,
            completed_at_utc,
            was_reused,
            source_status,
            resolution_note,
            legacy_source,
            legacy_source_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ifs_id,
            model_id,
            dataset_id,
            json.dumps(input_param),
            json.dumps(input_coef),
            json.dumps(output_set),
            model_status,
            fit_var,
            fit_pooled,
            trial_index,
            batch_index,
            started_at_utc,
            completed_at_utc,
            1 if was_reused else 0,
            source_status,
            resolution_note,
            legacy_source,
            legacy_source_id,
        ),
    )
    return int(cursor.lastrowid)


def update_model_run(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    model_status: str | None = None,
    fit_var: str | None = None,
    fit_pooled: float | None = None,
    started_at_utc: str | None = None,
    completed_at_utc: str | None = None,
    was_reused: bool | None = None,
    source_status: str | None = None,
    resolution_note: str | None = None,
    trial_index: int | None = None,
    batch_index: int | None = None,
    legacy_source: str | None = None,
    legacy_source_id: int | None = None,
) -> None:
    assignments: list[str] = []
    values: list[object] = []
    for column_name, value in (
        ("model_status", model_status),
        ("fit_var", fit_var),
        ("fit_pooled", fit_pooled),
        ("started_at_utc", started_at_utc),
        ("completed_at_utc", completed_at_utc),
        ("source_status", source_status),
        ("resolution_note", resolution_note),
        ("trial_index", trial_index),
        ("batch_index", batch_index),
        ("legacy_source", legacy_source),
        ("legacy_source_id", legacy_source_id),
    ):
        if value is not None:
            assignments.append(f"{column_name} = ?")
            values.append(value)
    if was_reused is not None:
        assignments.append("was_reused = ?")
        values.append(1 if was_reused else 0)
    if not assignments:
        return
    values.append(run_id)
    conn.execute(
        f"UPDATE {MODEL_RUN_TABLE} SET {', '.join(assignments)} WHERE run_id = ?",
        values,
    )


def upsert_seed_model_run(
    conn: sqlite3.Connection,
    *,
    definition: ModelDefinition,
    model_status: str | None,
    fit_var: str | None,
    fit_pooled: float | None,
    completed_at_utc: str | None,
    source_status: str | None = None,
) -> int:
    ensure_current_bigpopa_schema(conn.cursor())
    existing = conn.execute(
        f"""
        SELECT run_id
        FROM {MODEL_RUN_TABLE}
        WHERE model_id = ?
          AND trial_index IS NULL
          AND batch_index IS NULL
          AND resolution_note = 'model_setup_seed'
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (definition.model_id,),
    ).fetchone()
    if existing is not None:
        run_id = int(existing[0])
        update_model_run(
            conn,
            run_id=run_id,
            model_status=model_status,
            fit_var=fit_var,
            fit_pooled=fit_pooled,
            completed_at_utc=completed_at_utc,
            source_status=source_status,
            resolution_note="model_setup_seed",
        )
        return run_id
    return insert_model_run(
        conn,
        ifs_id=definition.ifs_id,
        model_id=definition.model_id,
        dataset_id=definition.dataset_id,
        input_param=definition.input_param,
        input_coef=definition.input_coef,
        output_set=definition.output_set,
        model_status=model_status,
        fit_var=fit_var,
        fit_pooled=fit_pooled,
        completed_at_utc=completed_at_utc,
        was_reused=False,
        source_status=source_status,
        resolution_note="model_setup_seed",
    )


def latest_run_id(conn: sqlite3.Connection, dataset_id: str | None) -> int | None:
    cursor = conn.cursor()
    ensure_current_bigpopa_schema(cursor)
    if dataset_id is None:
        row = cursor.execute(
            f"SELECT MAX(run_id) FROM {MODEL_RUN_TABLE} WHERE dataset_id IS NULL"
        ).fetchone()
    else:
        row = cursor.execute(
            f"SELECT MAX(run_id) FROM {MODEL_RUN_TABLE} WHERE dataset_id = ?",
            (dataset_id,),
        ).fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def resolve_dataset_id_for_model_id(cursor: sqlite3.Cursor, model_id: str) -> str | None:
    ensure_current_bigpopa_schema(cursor)
    row = cursor.execute(
        f"""
        SELECT dataset_id
        FROM {MODEL_RUN_TABLE}
        WHERE model_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (model_id,),
    ).fetchone()
    return row[0] if row is not None else None


def load_model_definition(conn: sqlite3.Connection, model_id: str) -> ModelDefinition:
    cursor = conn.cursor()
    ensure_current_bigpopa_schema(cursor)
    row = cursor.execute(
        f"""
        SELECT
            ifs_id,
            model_id,
            dataset_id,
            input_param,
            input_coef,
            output_set
        FROM {MODEL_RUN_TABLE}
        WHERE model_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (model_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError("No stored model definition was found for the provided model_id.")
    return ModelDefinition(
        ifs_id=int(row[0]),
        model_id=str(row[1]),
        dataset_id=row[2] if isinstance(row[2], str) or row[2] is None else str(row[2]),
        input_param=_parse_json_dict(row[3]),
        input_coef=_parse_json_dict(row[4]),
        output_set=_parse_json_dict(row[5]),
    )


def fetch_latest_result_for_model(
    conn: sqlite3.Connection,
    *,
    model_id: str,
) -> tuple[str | None, float | None, str | None]:
    cursor = conn.cursor()
    ensure_current_bigpopa_schema(cursor)
    row = cursor.execute(
        f"""
        SELECT model_status, fit_pooled, fit_var
        FROM {MODEL_RUN_TABLE}
        WHERE model_id = ?
          AND fit_pooled IS NOT NULL
        ORDER BY
            CASE WHEN completed_at_utc IS NULL THEN 1 ELSE 0 END,
            completed_at_utc DESC,
            run_id DESC
        LIMIT 1
        """,
        (model_id,),
    ).fetchone()
    if row is not None:
        return (
            row[0] if isinstance(row[0], str) or row[0] is None else str(row[0]),
            float(row[1]) if row[1] is not None else None,
            row[2] if isinstance(row[2], str) or row[2] is None else str(row[2]),
        )
    return None, None, None


def count_completed_trial_runs(conn: sqlite3.Connection, dataset_id: str | None) -> int:
    cursor = conn.cursor()
    ensure_current_bigpopa_schema(cursor)
    if dataset_id is None:
        row = cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {MODEL_RUN_TABLE}
            WHERE dataset_id IS NULL
              AND trial_index IS NOT NULL
              AND completed_at_utc IS NOT NULL
            """
        ).fetchone()
    else:
        row = cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {MODEL_RUN_TABLE}
            WHERE dataset_id = ?
              AND trial_index IS NOT NULL
              AND completed_at_utc IS NOT NULL
            """,
            (dataset_id,),
        ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def resolve_reference_fit(
    conn: sqlite3.Connection,
    *,
    dataset_id: str | None,
    model_id: str | None,
) -> tuple[str | None, float | None, str | None]:
    cursor = conn.cursor()
    ensure_current_bigpopa_schema(cursor)

    if model_id:
        status, fit_pooled, _fit_var = fetch_latest_result_for_model(conn, model_id=model_id)
        return model_id, visible_fit_pooled(status, fit_pooled), None

    if dataset_id is None:
        row = cursor.execute(
            f"""
            SELECT model_id
            FROM {MODEL_RUN_TABLE}
            WHERE dataset_id IS NULL
            ORDER BY
                CASE WHEN trial_index IS NULL THEN 0 ELSE 1 END,
                run_id ASC
            LIMIT 1
            """
        ).fetchone()
    else:
        row = cursor.execute(
            f"""
            SELECT model_id
            FROM {MODEL_RUN_TABLE}
            WHERE dataset_id = ?
            ORDER BY
                CASE WHEN trial_index IS NULL THEN 0 ELSE 1 END,
                run_id ASC
            LIMIT 1
            """,
            (dataset_id,),
        ).fetchone()
    reference_model_id = str(row[0]) if row is not None else None

    if reference_model_id is None:
        return None, None, None

    status, fit_pooled, _fit_var = fetch_latest_result_for_model(conn, model_id=reference_model_id)
    return reference_model_id, visible_fit_pooled(status, fit_pooled), None


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _run_sort_key(row: tuple[Any, ...]) -> tuple[Any, ...]:
    started_at = _parse_iso_timestamp(row[12] if isinstance(row[12], str) or row[12] is None else None)
    completed_at = _parse_iso_timestamp(row[13] if isinstance(row[13], str) or row[13] is None else None)
    primary_timestamp = started_at if started_at is not None else completed_at
    trial_index = row[10] if isinstance(row[10], int) else sys.maxsize
    batch_index = row[11] if isinstance(row[11], int) else sys.maxsize
    model_id = row[2] if isinstance(row[2], str) else ""
    return (
        0 if primary_timestamp is not None else 1,
        primary_timestamp or datetime.max.replace(tzinfo=timezone.utc),
        trial_index,
        batch_index,
        model_id,
        row[0] if isinstance(row[0], int) else 0,
    )


def load_model_run_rows(
    conn: sqlite3.Connection,
    *,
    dataset_id: str | None,
    since_run_id: int | None = None,
) -> list[tuple[Any, ...]]:
    cursor = conn.cursor()
    ensure_current_bigpopa_schema(cursor)
    clauses: list[str] = []
    params: list[object] = []
    if dataset_id is None:
        clauses.append("dataset_id IS NULL")
    else:
        clauses.append("dataset_id = ?")
        params.append(dataset_id)
    if since_run_id is not None:
        clauses.append("run_id >= ?")
        params.append(int(since_run_id))
    where_clause = " AND ".join(clauses)
    rows = cursor.execute(
        f"""
        SELECT
            run_id,
            ifs_id,
            model_id,
            dataset_id,
            input_param,
            input_coef,
            output_set,
            model_status,
            fit_var,
            fit_pooled,
            trial_index,
            batch_index,
            started_at_utc,
            completed_at_utc,
            was_reused,
            source_status,
            resolution_note
        FROM {MODEL_RUN_TABLE}
        WHERE {where_clause}
        """,
        params,
    ).fetchall()
    return sorted(rows, key=_run_sort_key)


def resolve_latest_dataset_id(conn: sqlite3.Connection) -> str | None:
    cursor = conn.cursor()
    ensure_current_bigpopa_schema(cursor)
    row = cursor.execute(
        f"""
        SELECT dataset_id
        FROM {MODEL_RUN_TABLE}
        ORDER BY
            CASE WHEN COALESCE(completed_at_utc, started_at_utc) IS NULL THEN 1 ELSE 0 END,
            COALESCE(completed_at_utc, started_at_utc) DESC,
            run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        raise RuntimeError("No tracked runs were found.")
    return row[0]


def list_recent_dataset_ids(
    conn: sqlite3.Connection,
    *,
    limit: int | None = None,
) -> list[str]:
    cursor = conn.cursor()
    ensure_current_bigpopa_schema(cursor)
    query = f"""
        SELECT dataset_id
        FROM {MODEL_RUN_TABLE}
        WHERE dataset_id IS NOT NULL
          AND TRIM(dataset_id) <> ''
        GROUP BY dataset_id
        ORDER BY MAX(run_id) DESC, dataset_id ASC
    """
    params: list[object] = []
    if limit is not None:
        query += " LIMIT ?"
        params.append(int(limit))
    rows = cursor.execute(query, params).fetchall()
    return [str(row[0]) for row in rows if row and row[0] is not None]


def list_recent_dataset_run_counts(
    conn: sqlite3.Connection,
    *,
    limit: int | None = None,
) -> list[tuple[str, int]]:
    cursor = conn.cursor()
    ensure_current_bigpopa_schema(cursor)
    query = f"""
        SELECT dataset_id, COUNT(*) AS run_count
        FROM {MODEL_RUN_TABLE}
        WHERE dataset_id IS NOT NULL
          AND TRIM(dataset_id) <> ''
        GROUP BY dataset_id
        ORDER BY MAX(run_id) DESC, dataset_id ASC
    """
    params: list[object] = []
    if limit is not None:
        query += " LIMIT ?"
        params.append(int(limit))
    rows = cursor.execute(query, params).fetchall()
    return [
        (str(row[0]), int(row[1]))
        for row in rows
        if row and row[0] is not None and row[1] is not None
    ]


def normalize_run_row(row: tuple[Any, ...]) -> ModelRunRow:
    return ModelRunRow(
        run_id=int(row[0]),
        ifs_id=int(row[1]) if row[1] is not None else None,
        model_id=str(row[2]),
        dataset_id=row[3] if isinstance(row[3], str) or row[3] is None else str(row[3]),
        input_param=_parse_json_dict(row[4]),
        input_coef=_parse_json_dict(row[5]),
        output_set=_parse_json_dict(row[6]),
        model_status=row[7] if isinstance(row[7], str) or row[7] is None else str(row[7]),
        fit_var=row[8] if isinstance(row[8], str) or row[8] is None else str(row[8]),
        fit_pooled=float(row[9]) if row[9] is not None else None,
        trial_index=int(row[10]) if row[10] is not None else None,
        batch_index=int(row[11]) if row[11] is not None else None,
        started_at_utc=row[12] if isinstance(row[12], str) or row[12] is None else str(row[12]),
        completed_at_utc=row[13] if isinstance(row[13], str) or row[13] is None else str(row[13]),
        was_reused=bool(row[14]),
        source_status=row[15] if isinstance(row[15], str) or row[15] is None else str(row[15]),
        resolution_note=row[16] if isinstance(row[16], str) or row[16] is None else str(row[16]),
    )


def is_visible_training_sample(row: ModelRunRow) -> bool:
    return not fit_is_missing(row.model_status, row.fit_pooled) and row.fit_pooled is not None


def find_active_run_id_for_model(
    conn: sqlite3.Connection,
    *,
    model_id: str,
) -> int | None:
    cursor = conn.cursor()
    ensure_current_bigpopa_schema(cursor)
    row = cursor.execute(
        f"""
        SELECT run_id
        FROM {MODEL_RUN_TABLE}
        WHERE model_id = ?
        ORDER BY
            CASE WHEN completed_at_utc IS NULL THEN 0 ELSE 1 END,
            run_id DESC
        LIMIT 1
        """,
        (model_id,),
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None
