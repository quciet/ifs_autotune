"""Read ML progress history from the unified append-only model_run table."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from typing import Any

from runtime.model_run_store import (
    latest_run_id,
    load_model_run_rows,
    normalize_run_row,
    resolve_dataset_id_for_model_id,
    resolve_reference_fit,
)
from runtime.model_status import fit_is_missing, visible_fit_pooled
from db.schema import ensure_current_bigpopa_schema


def repair_model_output_batch_indexes(conn: sqlite3.Connection) -> int:
    del conn
    return 0


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


def resolve_dataset_id(
    cursor: sqlite3.Cursor,
    dataset_id_arg: str | None,
    model_id_arg: str | None,
) -> str | None:
    if dataset_id_arg is not None:
        return dataset_id_arg

    if not model_id_arg:
        raise ValueError("Either --dataset-id or --model-id is required.")

    dataset_id = resolve_dataset_id_for_model_id(cursor, model_id_arg)
    if model_id_arg and dataset_id is None:
        raise LookupError("The selected model was not found in stored run history.")
    return dataset_id


def normalize_trial_row(
    raw_row: tuple[Any, ...],
    *,
    sequence_index: int,
    derived_round_index: int,
) -> dict[str, Any]:
    row = normalize_run_row(raw_row)
    fit_missing = fit_is_missing(row.model_status, row.fit_pooled)
    fit_pooled = visible_fit_pooled(row.model_status, row.fit_pooled)
    return {
        "model_id": row.model_id,
        "model_status": row.model_status,
        "fit_pooled": fit_pooled,
        "fit_missing": fit_missing,
        "trial_index": row.trial_index,
        "batch_index": row.batch_index,
        "started_at_utc": row.started_at_utc,
        "completed_at_utc": row.completed_at_utc,
        "dataset_id": row.dataset_id,
        "sequence_index": sequence_index,
        "derived_round_index": derived_round_index,
        "progress_rowid": row.run_id,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read ML progress history")
    parser.add_argument("--bigpopa-db", required=True, help="Path to bigpopa.db")
    parser.add_argument("--dataset-id", required=False)
    parser.add_argument("--model-id", required=False)
    parser.add_argument(
        "--since-run-id",
        required=False,
        type=int,
        help="Only return runs whose run_id is newer than this cursor.",
    )
    parser.add_argument(
        "--since-progress-rowid",
        dest="since_run_id_legacy",
        required=False,
        type=int,
        help="Legacy alias for --since-run-id.",
    )
    args = parser.parse_args(argv)

    db_path = args.bigpopa_db
    try:
        conn = sqlite3.connect(str(db_path))
    except sqlite3.Error as exc:
        emit_response(
            "error",
            "ml_progress",
            "Unable to open bigpopa.db.",
            {
                "error": str(exc),
                "dataset_id": None,
                "reference_model_id": None,
                "reference_fit_pooled": None,
                "latest_run_id": None,
                "trials": [],
            },
        )
        return 1

    try:
        cursor = conn.cursor()
        ensure_current_bigpopa_schema(cursor)
        try:
            dataset_id = resolve_dataset_id(cursor, args.dataset_id, args.model_id)
        except LookupError:
            emit_response(
                "success",
                "ml_progress",
                "The selected model was not found in stored run history.",
                {
                    "dataset_id": None,
                    "reference_model_id": None,
                    "reference_fit_pooled": None,
                    "latest_run_id": None,
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
                    "latest_run_id": None,
                    "trials": [],
                },
            )
            return 1

        reference_model_id, reference_fit_pooled, reference_warning = resolve_reference_fit(
            conn,
            dataset_id=dataset_id,
            model_id=args.model_id,
        )

        since_run_id = (
            args.since_run_id
            if args.since_run_id is not None
            else args.since_run_id_legacy
        )
        all_rows = load_model_run_rows(conn, dataset_id=dataset_id, since_run_id=None)
        latest_seen_run_id = latest_run_id(conn, dataset_id)

        trials: list[dict[str, Any]] = []
        derived_round_index = 0
        visible_sequence_index = 0
        for row in all_rows:
            trial_index = row[10] if isinstance(row[10], int) else None
            if trial_index is None:
                continue
            visible_sequence_index += 1
            if visible_sequence_index == 1:
                derived_round_index = 1
            elif trial_index == 1:
                derived_round_index += 1
            run_id = row[0] if isinstance(row[0], int) else None
            if since_run_id is not None and run_id is not None and run_id < since_run_id:
                continue
            trials.append(
                normalize_trial_row(
                    row,
                    sequence_index=visible_sequence_index,
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
                "latest_run_id": latest_seen_run_id,
                "latest_progress_rowid": latest_seen_run_id,
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
                "dataset_id": None,
                "reference_model_id": None,
                "reference_fit_pooled": None,
                "latest_run_id": None,
                "trials": [],
            },
        )
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
