"""Read lightweight ML progress history from bigpopa.db.

Progress cohorts follow the same exact ``dataset_id`` grouping used by
surrogate-model training sample selection.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from model_status import fit_is_missing, visible_fit_pooled


def repair_model_output_batch_indexes(conn: sqlite3.Connection) -> int:
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(model_output)")
    columns = {row[1] for row in cursor.fetchall()}
    if not {"trial_index", "batch_index"}.issubset(columns):
        return 0

    cursor.execute(
        """
        UPDATE model_output
        SET batch_index = 1
        WHERE trial_index IS NOT NULL
          AND (batch_index IS NULL OR batch_index != 1)
        """
    )
    return int(cursor.rowcount or 0)


def resolve_dataset_id(
    cursor: sqlite3.Cursor,
    dataset_id_arg: str | None,
    model_id_arg: str | None,
) -> str | None:
    if dataset_id_arg is not None:
        return dataset_id_arg

    if not model_id_arg:
        raise ValueError("Either --dataset-id or --model-id is required.")

    dataset_row = cursor.execute(
        "SELECT dataset_id FROM model_input WHERE model_id = ? LIMIT 1",
        (model_id_arg,),
    ).fetchone()
    if not dataset_row:
        raise LookupError("The selected model was not found in model_input.")

    return dataset_row[0]


def resolve_reference_fit(
    cursor: sqlite3.Cursor,
    dataset_id_arg: str | None,
    model_id_arg: str | None,
) -> tuple[str | None, float | None, str | None]:
    baseline_rows = cursor.execute(
        """
        SELECT
            mi.model_id,
            mo.fit_pooled,
            mi.rowid,
            mo.rowid
        FROM model_input mi
        LEFT JOIN model_output mo ON mo.model_id = mi.model_id
        WHERE (
                (? IS NULL AND mi.dataset_id IS NULL)
                OR mi.dataset_id = ?
              )
        ORDER BY
            mi.rowid ASC,
            mo.rowid ASC
        """,
        (dataset_id_arg, dataset_id_arg),
    ).fetchall()
    if baseline_rows:
        if len(baseline_rows) == 1:
            selected_row = baseline_rows[0]
            warning = None
        else:
            selected_row = None
            if model_id_arg:
                selected_row = next(
                    (row for row in baseline_rows if row[0] == model_id_arg),
                    None,
                )
            if selected_row is None:
                selected_row = baseline_rows[0]
            warning = (
                "Multiple default IFs baseline candidates were found for the selected "
                f"dataset; using model {selected_row[0]}."
            )

        status_row = cursor.execute(
            "SELECT model_status FROM model_output WHERE model_id = ? LIMIT 1",
            (selected_row[0],),
        ).fetchone()
        model_status = status_row[0] if status_row else None
        return selected_row[0], visible_fit_pooled(model_status, selected_row[1]), warning

    if not model_id_arg:
        return None, None, None

    input_row = cursor.execute(
        "SELECT model_id FROM model_input WHERE model_id = ? LIMIT 1",
        (model_id_arg,),
    ).fetchone()
    if not input_row:
        return None, None, None

    output_row = cursor.execute(
        "SELECT model_status, fit_pooled FROM model_output WHERE model_id = ? LIMIT 1",
        (model_id_arg,),
    ).fetchone()
    fit_pooled = visible_fit_pooled(output_row[0], output_row[1]) if output_row else None
    return model_id_arg, fit_pooled, None


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


def _trial_sort_key(row: tuple[Any, ...]) -> tuple[Any, ...]:
    started_at = _parse_iso_timestamp(row[5])
    completed_at = _parse_iso_timestamp(row[6])
    primary_timestamp = started_at if started_at is not None else completed_at
    input_rowid = row[8]
    progress_rowid = row[9]

    return (
        0 if primary_timestamp is not None else 1,
        primary_timestamp or datetime.max.replace(tzinfo=timezone.utc),
        0 if started_at is not None else 1,
        input_rowid if isinstance(input_rowid, int) else sys.maxsize,
        progress_rowid if isinstance(progress_rowid, int) else sys.maxsize,
        row[0] or "",
    )


def normalize_trial_row(
    row: tuple[Any, ...],
    *,
    sequence_index: int,
    derived_round_index: int,
) -> dict[str, Any]:
    model_status = row[1]
    fit_missing = fit_is_missing(model_status, row[2])
    fit_pooled = visible_fit_pooled(model_status, row[2])

    return {
        "model_id": row[0],
        "model_status": model_status,
        "fit_pooled": fit_pooled,
        "fit_missing": fit_missing,
        "trial_index": row[3],
        "batch_index": row[4],
        "started_at_utc": row[5],
        "completed_at_utc": row[6],
        "dataset_id": row[7],
        "sequence_index": sequence_index,
        "derived_round_index": derived_round_index,
        "progress_rowid": row[9] if isinstance(row[9], int) else None,
    }


def emit_response(status: str, stage: str, message: str, data: dict[str, Any]) -> None:
    print(
        json.dumps(
            {
                "status": status,
                "stage": stage,
                "message": message,
                "data": data,
            }
        )
    )
    sys.stdout.flush()


def _table_columns(cursor: sqlite3.Cursor, table_name: str) -> set[str]:
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cursor.fetchall()}


def _has_proposal_history_table(cursor: sqlite3.Cursor) -> bool:
    columns = _table_columns(cursor, "ml_proposal_history")
    required = {
        "proposal_event_id",
        "model_id",
        "dataset_id",
        "trial_index",
        "batch_index",
        "proposal_status",
        "fit_pooled_visible",
        "started_at_utc",
        "completed_at_utc",
    }
    return required.issubset(columns)


def _load_progress_rows_from_history(
    cursor: sqlite3.Cursor,
    *,
    dataset_id: str | None,
) -> list[tuple[Any, ...]]:
    if dataset_id is None:
        rows = cursor.execute(
            """
            SELECT
                mph.model_id,
                mph.proposal_status,
                mph.fit_pooled_visible,
                mph.trial_index,
                mph.batch_index,
                mph.started_at_utc,
                mph.completed_at_utc,
                mph.dataset_id,
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
                mph.proposal_status,
                mph.fit_pooled_visible,
                mph.trial_index,
                mph.batch_index,
                mph.started_at_utc,
                mph.completed_at_utc,
                mph.dataset_id,
                mi.rowid,
                mph.proposal_event_id
            FROM ml_proposal_history mph
            LEFT JOIN model_input mi ON mi.model_id = mph.model_id
            WHERE mph.dataset_id = ?
            """,
            (dataset_id,),
        ).fetchall()

    return sorted(rows, key=_trial_sort_key)


def _load_progress_rows_from_model_output(
    cursor: sqlite3.Cursor,
    *,
    dataset_id: str | None,
) -> list[tuple[Any, ...]]:
    if dataset_id is None:
        rows = cursor.execute(
            """
            SELECT
                mo.model_id,
                mo.model_status,
                mo.fit_pooled,
                mo.trial_index,
                mo.batch_index,
                mo.started_at_utc,
                mo.completed_at_utc,
                mi.dataset_id,
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
                mo.model_status,
                mo.fit_pooled,
                mo.trial_index,
                mo.batch_index,
                mo.started_at_utc,
                mo.completed_at_utc,
                mi.dataset_id,
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read ML progress history")
    parser.add_argument("--bigpopa-db", required=True, help="Path to bigpopa.db")
    parser.add_argument(
        "--dataset-id",
        required=False,
        help="Dataset id used to scope progress history explicitly.",
    )
    parser.add_argument(
        "--model-id",
        required=False,
        help="Model id used to resolve the dataset_id cohort for progress history.",
    )
    parser.add_argument(
        "--since-progress-rowid",
        "--since-output-rowid",
        dest="since_progress_rowid",
        required=False,
        type=int,
        help="Only return trials whose progress event rowid is newer than this cursor.",
    )
    args = parser.parse_args(argv)

    db_path = Path(args.bigpopa_db).expanduser().resolve()
    if not db_path.exists():
        emit_response(
            "error",
            "ml_progress",
            "bigpopa.db was not found.",
            {
                "reference_model_id": None,
                "reference_fit_pooled": None,
                "trials": [],
            },
        )
        return 1

    try:
        conn = sqlite3.connect(str(db_path))
    except sqlite3.Error as exc:
        emit_response(
            "error",
            "ml_progress",
            "Unable to open bigpopa.db.",
            {
                "error": str(exc),
                "reference_model_id": None,
                "reference_fit_pooled": None,
                "trials": [],
            },
        )
        return 1

    try:
        cursor = conn.cursor()
        try:
            dataset_id = resolve_dataset_id(cursor, args.dataset_id, args.model_id)
        except LookupError:
            emit_response(
                "success",
                "ml_progress",
                "The selected model was not found in model_input.",
                {
                    "dataset_id": None,
                    "reference_model_id": None,
                    "reference_fit_pooled": None,
                    "trials": [],
                },
            )
            return 0
        except ValueError as exc:
            emit_response(
                "error",
                "ml_progress",
                str(exc),
                {
                    "dataset_id": None,
                    "reference_model_id": None,
                    "reference_fit_pooled": None,
                    "trials": [],
                },
            )
            return 1

        reference_model_id, reference_fit_pooled, reference_warning = resolve_reference_fit(
            cursor,
            dataset_id,
            args.model_id,
        )

        use_proposal_history = _has_proposal_history_table(cursor)
        if use_proposal_history:
            rows = _load_progress_rows_from_history(cursor, dataset_id=dataset_id)
            if not rows:
                repair_model_output_batch_indexes(conn)
                rows = _load_progress_rows_from_model_output(cursor, dataset_id=dataset_id)
        else:
            repair_model_output_batch_indexes(conn)
            rows = _load_progress_rows_from_model_output(cursor, dataset_id=dataset_id)

        trials: list[dict[str, Any]] = []
        derived_round_index = 0
        latest_progress_rowid = max(
            (row[9] for row in rows if isinstance(row[9], int)),
            default=None,
        )

        for sequence_index, row in enumerate(rows, start=1):
            trial_index = row[3]
            if sequence_index == 1:
                derived_round_index = 1
            elif isinstance(trial_index, int) and trial_index == 1:
                derived_round_index += 1

            progress_rowid = row[9] if isinstance(row[9], int) else None
            if (
                args.since_progress_rowid is not None
                and progress_rowid is not None
                and progress_rowid < args.since_progress_rowid
            ):
                continue

            trials.append(
                normalize_trial_row(
                    row,
                    sequence_index=sequence_index,
                    derived_round_index=derived_round_index,
                )
            )

        message = "Loaded ML progress history."
        if reference_warning:
            message = f"{message} {reference_warning}"

        emit_response(
            "success",
            "ml_progress",
            message,
            {
                "dataset_id": dataset_id,
                "reference_model_id": reference_model_id,
                "reference_fit_pooled": reference_fit_pooled,
                "latest_progress_rowid": latest_progress_rowid,
                "latest_output_rowid": latest_progress_rowid,
                "trials": trials,
            },
        )
        return 0
    except sqlite3.Error as exc:
        emit_response(
            "error",
            "ml_progress",
            "Unable to query ML progress history.",
            {
                "error": str(exc),
                "reference_model_id": None,
                "reference_fit_pooled": None,
                "trials": [],
            },
        )
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
