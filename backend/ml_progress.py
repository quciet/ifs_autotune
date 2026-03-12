"""Read lightweight ML progress history from bigpopa.db."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


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
    parser = argparse.ArgumentParser(description="Read ML progress history")
    parser.add_argument("--bigpopa-db", required=True, help="Path to bigpopa.db")
    args = parser.parse_args(argv)

    db_path = Path(args.bigpopa_db).expanduser().resolve()
    if not db_path.exists():
        emit_response(
            "error",
            "ml_progress",
            "bigpopa.db was not found.",
            {"trials": []},
        )
        return 1

    try:
        conn = sqlite3.connect(str(db_path))
    except sqlite3.Error as exc:
        emit_response(
            "error",
            "ml_progress",
            "Unable to open bigpopa.db.",
            {"error": str(exc), "trials": []},
        )
        return 1

    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(model_output)")
        columns = {row[1] for row in cursor.fetchall()}

        required = {"trial_index", "batch_index", "started_at_utc", "completed_at_utc"}
        if not required.issubset(columns):
            emit_response(
                "success",
                "ml_progress",
                "ML progress tracking columns are not available yet.",
                {"trials": []},
            )
            return 0

        rows = cursor.execute(
            """
            SELECT
                model_id,
                model_status,
                fit_pooled,
                trial_index,
                batch_index,
                started_at_utc,
                completed_at_utc
            FROM model_output
            WHERE trial_index IS NOT NULL
            ORDER BY trial_index ASC, completed_at_utc ASC, model_id ASC
            """
        ).fetchall()

        trials = [
            {
                "model_id": row[0],
                "model_status": row[1],
                "fit_pooled": row[2],
                "trial_index": row[3],
                "batch_index": row[4],
                "started_at_utc": row[5],
                "completed_at_utc": row[6],
            }
            for row in rows
        ]

        emit_response(
            "success",
            "ml_progress",
            "Loaded ML progress history.",
            {"trials": trials},
        )
        return 0
    except sqlite3.Error as exc:
        emit_response(
            "error",
            "ml_progress",
            "Unable to query ML progress history.",
            {"error": str(exc), "trials": []},
        )
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
