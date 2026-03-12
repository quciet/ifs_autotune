"""Backfill ML tracking metadata directly on model_output.

Historical cohorts are grouped by ``dataset_id`` to match the surrogate-model
training logic. Trial order uses model insertion order as a proxy.
"""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from model_setup import ensure_bigpopa_schema


def emit(status: str, message: str, **data: object) -> None:
    payload = {"status": status, "message": message}
    if data:
        payload["data"] = data
    print(json.dumps(payload))
    sys.stdout.flush()


@dataclass
class CohortRow:
    model_id: str
    dataset_id: str | None
    input_rowid: int | None
    output_rowid: int
    trial_index: int | None
    batch_index: int | None
    started_at_utc: str | None
    completed_at_utc: str | None


def _table_count(cursor: sqlite3.Cursor, table_name: str) -> int:
    row = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _format_iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat()


def _cohort_seed(dataset_id: str | None) -> str:
    return dataset_id if dataset_id is not None else "__legacy_dataset__"


def _load_rows(cursor: sqlite3.Cursor) -> list[CohortRow]:
    rows = cursor.execute(
        """
        SELECT
            mo.model_id,
            mi.dataset_id,
            mi.rowid,
            mo.rowid,
            mo.trial_index,
            mo.batch_index,
            mo.started_at_utc,
            mo.completed_at_utc
        FROM model_output mo
        JOIN model_input mi ON mi.model_id = mo.model_id
        ORDER BY
            mi.dataset_id ASC,
            CASE WHEN mi.rowid IS NULL THEN mo.rowid ELSE mi.rowid END ASC,
            mo.rowid ASC
        """
    ).fetchall()

    return [
        CohortRow(
            model_id=row[0],
            dataset_id=row[1],
            input_rowid=row[2],
            output_rowid=row[3],
            trial_index=row[4],
            batch_index=row[5],
            started_at_utc=row[6],
            completed_at_utc=row[7],
        )
        for row in rows
    ]


def _assign_trial_indexes(rows: list[CohortRow]) -> tuple[dict[str, int], dict[str, int], int, int]:
    trial_updates: dict[str, int] = {}
    batch_updates: dict[str, int] = {}
    trial_count = 0
    batch_count = 0
    next_trial = 0

    for row in rows:
        existing_trial = row.trial_index if isinstance(row.trial_index, int) else None
        if existing_trial is not None:
            assigned_trial = existing_trial
            next_trial = max(next_trial, existing_trial)
        else:
            next_trial += 1
            assigned_trial = next_trial
            trial_updates[row.model_id] = assigned_trial
            trial_count += 1

        if row.batch_index is None:
            batch_updates[row.model_id] = assigned_trial
            batch_count += 1

    return trial_updates, batch_updates, trial_count, batch_count


def _assign_synthetic_times(
    rows: list[CohortRow],
    *,
    now_utc: datetime,
) -> tuple[dict[str, str], dict[str, str], int]:
    started_updates: dict[str, str] = {}
    completed_updates: dict[str, str] = {}
    updated_rows = 0
    rng = random.Random(_cohort_seed(rows[0].dataset_id))

    ordered = sorted(
        rows,
        key=lambda row: (
            row.trial_index if isinstance(row.trial_index, int) else 0,
            row.input_rowid if row.input_rowid is not None else row.output_rowid,
            row.output_rowid,
        ),
    )

    anchor_completed = _parse_iso(ordered[-1].completed_at_utc) or now_utc

    for row in reversed(ordered):
        existing_completed = _parse_iso(row.completed_at_utc)
        if existing_completed is not None:
            completed_at = existing_completed
        else:
            completed_at = anchor_completed
            completed_updates[row.model_id] = _format_iso(completed_at)

        existing_started = _parse_iso(row.started_at_utc)
        if existing_started is not None:
            started_at = existing_started
        else:
            duration = timedelta(seconds=rng.randint(120, 180))
            started_at = completed_at - duration
            started_updates[row.model_id] = _format_iso(started_at)

        if row.started_at_utc is None or row.completed_at_utc is None:
            updated_rows += 1

        gap = timedelta(seconds=rng.randint(15, 20))
        anchor_completed = started_at - gap

    return started_updates, completed_updates, updated_rows


def backfill_tracking(conn: sqlite3.Connection) -> dict[str, int]:
    cursor = conn.cursor()
    ensure_bigpopa_schema(cursor)

    all_rows = _load_rows(cursor)
    grouped: dict[str | None, list[CohortRow]] = defaultdict(list)
    for row in all_rows:
        grouped[row.dataset_id].append(row)

    trial_updates_total = 0
    batch_updates_total = 0
    started_updates_total = 0
    completed_updates_total = 0
    touched_cohorts = 0
    now_utc = datetime.now(UTC)

    for _dataset_id, cohort_rows in grouped.items():
        trial_updates, batch_updates, trial_count, batch_count = _assign_trial_indexes(cohort_rows)

        for row in cohort_rows:
            if row.model_id in trial_updates:
                row.trial_index = trial_updates[row.model_id]
            if row.model_id in batch_updates:
                row.batch_index = batch_updates[row.model_id]

        started_updates, completed_updates, _ = _assign_synthetic_times(
            cohort_rows,
            now_utc=now_utc,
        )

        for row in cohort_rows:
            new_trial = trial_updates.get(row.model_id)
            new_batch = batch_updates.get(row.model_id)
            new_started = started_updates.get(row.model_id)
            new_completed = completed_updates.get(row.model_id)
            if not any(value is not None for value in (new_trial, new_batch, new_started, new_completed)):
                continue

            cursor.execute(
                """
                UPDATE model_output
                SET
                    trial_index = COALESCE(?, trial_index),
                    batch_index = COALESCE(?, batch_index),
                    started_at_utc = COALESCE(?, started_at_utc),
                    completed_at_utc = COALESCE(?, completed_at_utc)
                WHERE model_id = ?
                """,
                (
                    new_trial,
                    new_batch,
                    new_started,
                    new_completed,
                    row.model_id,
                ),
            )

        if trial_count or batch_count or started_updates or completed_updates:
            touched_cohorts += 1

        trial_updates_total += trial_count
        batch_updates_total += batch_count
        started_updates_total += len(started_updates)
        completed_updates_total += len(completed_updates)

    conn.commit()
    return {
        "cohorts_touched": touched_cohorts,
        "trial_index_backfilled": trial_updates_total,
        "batch_index_backfilled": batch_updates_total,
        "started_at_backfilled": started_updates_total,
        "completed_at_backfilled": completed_updates_total,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill ML tracking columns in bigpopa.db")
    parser.add_argument("--bigpopa-db", required=True, help="Path to bigpopa.db")
    args = parser.parse_args(argv)

    db_path = Path(args.bigpopa_db).expanduser().resolve()
    if not db_path.exists():
        emit("error", "Database file not found.", bigpopa_db=str(db_path))
        return 1

    try:
        conn = sqlite3.connect(str(db_path))
    except sqlite3.Error as exc:
        emit("error", "Unable to open database.", bigpopa_db=str(db_path), error=str(exc))
        return 1

    try:
        cursor = conn.cursor()
        before_input = _table_count(cursor, "model_input")
        before_output = _table_count(cursor, "model_output")
        summary = backfill_tracking(conn)
        after_input = _table_count(cursor, "model_input")
        after_output = _table_count(cursor, "model_output")
        emit(
            "success",
            "ML tracking backfill applied.",
            bigpopa_db=str(db_path),
            model_input_rows_before=before_input,
            model_input_rows_after=after_input,
            model_output_rows_before=before_output,
            model_output_rows_after=after_output,
            **summary,
        )
        return 0
    except sqlite3.Error as exc:
        emit("error", "Unable to backfill ML tracking.", bigpopa_db=str(db_path), error=str(exc))
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
