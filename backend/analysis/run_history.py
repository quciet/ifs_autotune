from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import sqlite3
import sys

from model_status import fit_is_missing, visible_fit_pooled

@dataclass(frozen=True)
class RunRecord:
    model_id: str
    dataset_id: str | None
    model_status: str | None
    fit_pooled: float | None
    fit_missing: bool
    trial_index: int | None
    batch_index: int | None
    started_at_utc: str | None
    completed_at_utc: str | None
    sequence_index: int
    derived_round_index: int
    model_input_rowid: int | None
    model_output_rowid: int | None
    input_param: dict[str, float]
    input_coef: dict[str, dict[str, dict[str, float]]]


def parse_iso_timestamp(value: str | None) -> datetime | None:
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


def _trial_sort_key(row: tuple[object, ...]) -> tuple[object, ...]:
    started_at_value = row[6] if row[6] is None or isinstance(row[6], str) else None
    completed_at_value = row[7] if row[7] is None or isinstance(row[7], str) else None
    started_at = parse_iso_timestamp(started_at_value)
    completed_at = parse_iso_timestamp(completed_at_value)
    primary_timestamp = started_at if started_at is not None else completed_at
    input_rowid = row[10]
    output_rowid = row[11]

    return (
        0 if primary_timestamp is not None else 1,
        primary_timestamp or datetime.max.replace(tzinfo=timezone.utc),
        0 if started_at is not None else 1,
        input_rowid if isinstance(input_rowid, int) else sys.maxsize,
        output_rowid if isinstance(output_rowid, int) else sys.maxsize,
        row[0] or "",
    )


def ensure_tracking_columns(cursor: sqlite3.Cursor) -> None:
    cursor.execute("PRAGMA table_info(ml_proposal_history)")
    proposal_columns = {row[1] for row in cursor.fetchall()}
    proposal_required = {
        "proposal_event_id",
        "dataset_id",
        "trial_index",
        "batch_index",
        "proposal_status",
        "fit_pooled_visible",
        "started_at_utc",
        "completed_at_utc",
    }
    if proposal_required.issubset(proposal_columns):
        return

    cursor.execute("PRAGMA table_info(model_output)")
    columns = {row[1] for row in cursor.fetchall()}
    required = {"trial_index", "batch_index", "started_at_utc", "completed_at_utc"}
    missing = required.difference(columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise RuntimeError(
            "Neither ml_proposal_history nor model_output contains the required "
            f"ML tracking columns: {missing_text}"
        )


def _has_proposal_history_table(cursor: sqlite3.Cursor) -> bool:
    cursor.execute("PRAGMA table_info(ml_proposal_history)")
    proposal_columns = {row[1] for row in cursor.fetchall()}
    required = {
        "proposal_event_id",
        "dataset_id",
        "trial_index",
        "batch_index",
        "proposal_status",
        "fit_pooled_visible",
        "started_at_utc",
        "completed_at_utc",
    }
    return required.issubset(proposal_columns)


def resolve_latest_dataset_id(cursor: sqlite3.Cursor) -> str | None:
    if _has_proposal_history_table(cursor):
        row = cursor.execute(
            """
            SELECT mph.dataset_id
            FROM ml_proposal_history mph
            ORDER BY COALESCE(mph.completed_at_utc, mph.started_at_utc) DESC, mph.proposal_event_id DESC
            LIMIT 1
            """
        ).fetchone()
        if row is not None:
            return row[0]

    row = cursor.execute(
        """
        SELECT mi.dataset_id
        FROM model_output mo
        JOIN model_input mi ON mi.model_id = mo.model_id
        WHERE mo.trial_index IS NOT NULL
        ORDER BY COALESCE(mo.completed_at_utc, mo.started_at_utc) DESC, mo.rowid DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        raise RuntimeError("No tracked model runs were found.")
    return row[0]


def normalize_requested_dataset_id(dataset_id: str | None) -> str | None:
    if dataset_id is None:
        return None

    normalized = dataset_id.strip()
    if not normalized:
        return None

    return normalized


def _parse_numeric_dict(value: object) -> dict[str, float]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}

    normalized: dict[str, float] = {}
    for key, item in parsed.items():
        if not isinstance(key, str):
            continue
        try:
            normalized[key] = float(item)
        except (TypeError, ValueError):
            continue
    return normalized


def _parse_nested_numeric_dict(value: object) -> dict[str, dict[str, dict[str, float]]]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}

    normalized: dict[str, dict[str, dict[str, float]]] = {}
    for func_name, x_map in parsed.items():
        if not isinstance(func_name, str) or not isinstance(x_map, dict):
            continue
        normalized_x_map: dict[str, dict[str, float]] = {}
        for x_name, beta_map in x_map.items():
            if not isinstance(x_name, str) or not isinstance(beta_map, dict):
                continue
            normalized_beta_map: dict[str, float] = {}
            for beta_name, beta_value in beta_map.items():
                if not isinstance(beta_name, str):
                    continue
                try:
                    normalized_beta_map[beta_name] = float(beta_value)
                except (TypeError, ValueError):
                    continue
            normalized_x_map[x_name] = normalized_beta_map
        normalized[func_name] = normalized_x_map
    return normalized


def parameter_column_names(rows: list[RunRecord]) -> list[str]:
    return sorted({key for row in rows for key in row.input_param.keys()})


def coefficient_column_names(rows: list[RunRecord]) -> list[str]:
    return sorted(
        {
            f"{func_name}.{x_name}.{beta_name}"
            for row in rows
            for func_name, x_map in row.input_coef.items()
            for x_name, beta_map in x_map.items()
            for beta_name in beta_map.keys()
        }
    )


def flatten_run_inputs(row: RunRecord) -> dict[str, float]:
    flattened: dict[str, float] = {}
    for key in sorted(row.input_param.keys()):
        flattened[key] = float(row.input_param[key])

    for func_name in sorted(row.input_coef.keys()):
        x_map = row.input_coef[func_name]
        for x_name in sorted(x_map.keys()):
            beta_map = x_map[x_name]
            for beta_name in sorted(beta_map.keys()):
                flattened[f"{func_name}.{x_name}.{beta_name}"] = float(beta_map[beta_name])

    return flattened


def load_dataset_rows(cursor: sqlite3.Cursor, dataset_id: str | None) -> list[tuple[object, ...]]:
    if _has_proposal_history_table(cursor):
        if dataset_id is None:
            rows = cursor.execute(
                """
                SELECT
                    mph.model_id,
                    mph.dataset_id,
                    mph.proposal_status,
                    mph.fit_pooled_visible,
                    mph.trial_index,
                    mph.batch_index,
                    mph.started_at_utc,
                    mph.completed_at_utc,
                    mi.input_param,
                    mi.input_coef,
                    mi.rowid,
                    mph.proposal_event_id
                FROM ml_proposal_history mph
                LEFT JOIN model_input mi ON mi.model_id = mph.model_id
                WHERE mph.dataset_id IS NULL
                """
            ).fetchall()
        else:
            rows = cursor.execute(
                """
                SELECT
                    mph.model_id,
                    mph.dataset_id,
                    mph.proposal_status,
                    mph.fit_pooled_visible,
                    mph.trial_index,
                    mph.batch_index,
                    mph.started_at_utc,
                    mph.completed_at_utc,
                    mi.input_param,
                    mi.input_coef,
                    mi.rowid,
                    mph.proposal_event_id
                FROM ml_proposal_history mph
                LEFT JOIN model_input mi ON mi.model_id = mph.model_id
                WHERE mph.dataset_id = ?
                """,
                (dataset_id,),
            ).fetchall()
        if rows:
            return sorted(rows, key=_trial_sort_key)
        # Fall back to legacy model_output tracking when the table exists but has
        # not been populated yet in older local databases.

    if dataset_id is None:
        rows = cursor.execute(
            """
            SELECT
                mo.model_id,
                mi.dataset_id,
                mo.model_status,
                mo.fit_pooled,
                mo.trial_index,
                mo.batch_index,
                mo.started_at_utc,
                mo.completed_at_utc,
                mi.input_param,
                mi.input_coef,
                mi.rowid,
                mo.rowid
            FROM model_output mo
            JOIN model_input mi ON mi.model_id = mo.model_id
            WHERE mo.trial_index IS NOT NULL
              AND mi.dataset_id IS NULL
            """
        ).fetchall()
    else:
        rows = cursor.execute(
            """
            SELECT
                mo.model_id,
                mi.dataset_id,
                mo.model_status,
                mo.fit_pooled,
                mo.trial_index,
                mo.batch_index,
                mo.started_at_utc,
                mo.completed_at_utc,
                mi.input_param,
                mi.input_coef,
                mi.rowid,
                mo.rowid
            FROM model_output mo
            JOIN model_input mi ON mi.model_id = mo.model_id
            WHERE mo.trial_index IS NOT NULL
              AND mi.dataset_id = ?
            """,
            (dataset_id,),
        ).fetchall()

    return sorted(rows, key=_trial_sort_key)


def normalize_rows(rows: list[tuple[object, ...]]) -> list[RunRecord]:
    normalized: list[RunRecord] = []
    derived_round_index = 0

    for sequence_index, row in enumerate(rows, start=1):
        trial_index = row[4] if isinstance(row[4], int) else None
        if sequence_index == 1:
            derived_round_index = 1
        elif trial_index == 1:
            derived_round_index += 1

        model_status = row[2] if isinstance(row[2], str) or row[2] is None else str(row[2])
        fit_missing = fit_is_missing(model_status, row[3])
        fit_value = visible_fit_pooled(model_status, row[3])

        normalized.append(
            RunRecord(
                model_id=str(row[0]),
                dataset_id=row[1] if isinstance(row[1], str) or row[1] is None else str(row[1]),
                model_status=model_status,
                fit_pooled=fit_value,
                fit_missing=fit_missing,
                trial_index=trial_index,
                batch_index=row[5] if isinstance(row[5], int) else None,
                started_at_utc=row[6] if isinstance(row[6], str) or row[6] is None else str(row[6]),
                completed_at_utc=row[7] if isinstance(row[7], str) or row[7] is None else str(row[7]),
                sequence_index=sequence_index,
                derived_round_index=derived_round_index,
                model_input_rowid=row[10] if isinstance(row[10], int) else None,
                model_output_rowid=row[11] if isinstance(row[11], int) else None,
                input_param=_parse_numeric_dict(row[8]),
                input_coef=_parse_nested_numeric_dict(row[9]),
            )
        )

    return normalized


def select_latest_slice(rows: list[RunRecord], limit: int) -> list[RunRecord]:
    if limit <= 0:
        raise ValueError("limit must be greater than 0")
    return rows[-limit:]


def load_run_history(
    conn: sqlite3.Connection,
    dataset_id: str | None = None,
) -> tuple[str | None, list[RunRecord]]:
    cursor = conn.cursor()
    ensure_tracking_columns(cursor)
    requested_dataset_id = normalize_requested_dataset_id(dataset_id)
    selected_dataset_id = (
        requested_dataset_id if requested_dataset_id is not None else resolve_latest_dataset_id(cursor)
    )
    rows = normalize_rows(load_dataset_rows(cursor, selected_dataset_id))
    if not rows:
        if requested_dataset_id is not None:
            raise RuntimeError(
                f"No tracked runs were found for requested dataset_id={requested_dataset_id!r}. "
                "Leave the dataset override blank to use the latest dataset."
            )
        raise RuntimeError(f"No tracked runs were found for dataset_id={selected_dataset_id!r}.")
    return selected_dataset_id, rows
