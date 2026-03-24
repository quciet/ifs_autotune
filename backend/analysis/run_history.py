from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import sqlite3
import sys


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
    input_rowid = row[8]
    output_rowid = row[9]

    return (
        0 if primary_timestamp is not None else 1,
        primary_timestamp or datetime.max.replace(tzinfo=timezone.utc),
        0 if started_at is not None else 1,
        input_rowid if isinstance(input_rowid, int) else sys.maxsize,
        output_rowid if isinstance(output_rowid, int) else sys.maxsize,
        row[0] or "",
    )


def ensure_tracking_columns(cursor: sqlite3.Cursor) -> None:
    cursor.execute("PRAGMA table_info(model_output)")
    columns = {row[1] for row in cursor.fetchall()}
    required = {"trial_index", "batch_index", "started_at_utc", "completed_at_utc"}
    missing = required.difference(columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise RuntimeError(
            f"model_output is missing required ML tracking columns: {missing_text}"
        )


def resolve_latest_dataset_id(cursor: sqlite3.Cursor) -> str | None:
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
        raise RuntimeError("No tracked model runs with trial_index were found.")
    return row[0]


def load_dataset_rows(cursor: sqlite3.Cursor, dataset_id: str | None) -> list[tuple[object, ...]]:
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
        fit_missing = model_status == "failed"
        fit_value = None if fit_missing or row[3] is None else float(row[3])

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
                model_input_rowid=row[8] if isinstance(row[8], int) else None,
                model_output_rowid=row[9] if isinstance(row[9], int) else None,
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
    selected_dataset_id = dataset_id if dataset_id is not None else resolve_latest_dataset_id(cursor)
    rows = normalize_rows(load_dataset_rows(cursor, selected_dataset_id))
    if not rows:
        raise RuntimeError(f"No tracked runs were found for dataset_id={selected_dataset_id!r}.")
    return selected_dataset_id, rows
