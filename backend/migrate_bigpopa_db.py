"""Apply additive BIGPOPA schema migrations to an existing database."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from model_setup import ensure_bigpopa_schema


def emit(status: str, message: str, **data: object) -> None:
    payload = {"status": status, "message": message}
    if data:
        payload["data"] = data
    print(json.dumps(payload))
    sys.stdout.flush()


def _table_count(cursor: sqlite3.Cursor, table_name: str) -> int:
    row = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Apply additive BIGPOPA DB migrations")
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

        ensure_bigpopa_schema(cursor)
        conn.commit()

        columns = cursor.execute("PRAGMA table_info(model_output)").fetchall()
        column_names = [row[1] for row in columns]
        after_input = _table_count(cursor, "model_input")
        after_output = _table_count(cursor, "model_output")

        emit(
            "success",
            "BIGPOPA DB migration applied.",
            bigpopa_db=str(db_path),
            model_output_columns=column_names,
            model_input_rows_before=before_input,
            model_input_rows_after=after_input,
            model_output_rows_before=before_output,
            model_output_rows_after=after_output,
        )
        return 0
    except sqlite3.Error as exc:
        emit("error", "Unable to migrate database.", bigpopa_db=str(db_path), error=str(exc))
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
