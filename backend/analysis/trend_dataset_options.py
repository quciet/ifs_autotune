from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from runtime.model_run_store import list_recent_dataset_run_counts, resolve_latest_dataset_id
from db.schema import ensure_current_bigpopa_schema


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Return recent BIGPOPA dataset ids for trend-analysis selection."
    )
    parser.add_argument("--bigpopa-db", required=True, help="Path to bigpopa.db")
    args = parser.parse_args(argv)

    bigpopa_db = Path(args.bigpopa_db).expanduser().resolve()
    if not bigpopa_db.exists():
        emit_response(
            "error",
            "trend_dataset_options",
            "Could not find the requested bigpopa.db.",
            {"bigpopa_db": str(bigpopa_db)},
        )
        return 1

    try:
        with sqlite3.connect(str(bigpopa_db)) as conn:
            ensure_current_bigpopa_schema(conn.cursor())
            dataset_rows = list_recent_dataset_run_counts(conn)
            dataset_ids = [dataset_id for dataset_id, _run_count in dataset_rows]
            dataset_run_counts = {
                dataset_id: run_count for dataset_id, run_count in dataset_rows
            }
            try:
                latest_dataset_id = resolve_latest_dataset_id(conn)
            except RuntimeError:
                latest_dataset_id = None
    except Exception as exc:
        emit_response(
            "error",
            "trend_dataset_options",
            "Unable to load dataset options for trend analysis.",
            {
                "bigpopa_db": str(bigpopa_db),
                "error": str(exc),
            },
        )
        return 1

    emit_response(
        "success",
        "trend_dataset_options",
        "Trend analysis dataset options loaded successfully.",
        {
            "latest_dataset_id": latest_dataset_id,
            "dataset_ids": dataset_ids,
            "dataset_run_counts": dataset_run_counts,
            "latest_dataset_run_count": dataset_run_counts.get(latest_dataset_id)
            if latest_dataset_id is not None
            else None,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
