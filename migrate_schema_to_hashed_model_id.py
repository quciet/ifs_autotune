"""Migration script to ensure BIGPOPA tables support hashed model identifiers."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

try:  # pragma: no cover - defensive import
    from backend.app.settings import settings as backend_settings
except Exception:  # pragma: no cover - fallback when settings are unavailable
    backend_settings = None


@dataclass
class ColumnInfo:
    cid: int
    name: str
    type: str
    notnull: bool
    default_value: Optional[str]
    is_pk: bool

    @classmethod
    def from_row(cls, row: Iterable[object]) -> "ColumnInfo":
        cid, name, col_type, notnull, default_value, pk = row
        return cls(
            cid=int(cid),
            name=str(name),
            type=str(col_type or "").upper(),
            notnull=bool(notnull),
            default_value=str(default_value) if default_value is not None else None,
            is_pk=bool(pk),
        )


def _resolve_database_path(explicit: Optional[str]) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()

    env_path = os.environ.get("BIGPOPA_DB_PATH")
    if env_path:
        return Path(env_path).expanduser().resolve()

    if backend_settings is not None:
        candidate = Path(backend_settings.DB_PATH).expanduser()
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        return candidate.resolve()

    return (Path.cwd() / "bigpopa.db").resolve()


def _fetch_columns(cursor: sqlite3.Cursor, table: str) -> list[ColumnInfo]:
    rows = cursor.execute(f"PRAGMA table_info(\"{table}\")").fetchall()
    return [ColumnInfo.from_row(row) for row in rows]


def _ensure_model_input(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='model_input'"
    )
    if cursor.fetchone() is None:
        raise RuntimeError(
            "Table 'model_input' is missing. The schema must be provisioned before running"
            " the migration."
        )

    columns = _fetch_columns(cursor, "model_input")
    model_id_col = next((col for col in columns if col.name == "model_id"), None)
    if model_id_col and model_id_col.type == "TEXT" and model_id_col.is_pk:
        return

    raise RuntimeError(
        "Table 'model_input' must contain a TEXT PRIMARY KEY column named 'model_id'."
    )


def _ensure_model_output(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='model_output'"
    )
    if cursor.fetchone() is None:
        raise RuntimeError(
            "Table 'model_output' is missing. The schema must be provisioned before running"
            " the migration."
        )

    columns = _fetch_columns(cursor, "model_output")
    model_id_col = next((col for col in columns if col.name == "model_id"), None)
    if model_id_col and model_id_col.type == "TEXT":
        return

    raise RuntimeError(
        "Table 'model_output' must contain a TEXT column named 'model_id'."
    )


def _column_summary(cursor: sqlite3.Cursor, table: str, column: str) -> dict[str, object]:
    columns = _fetch_columns(cursor, table)
    target = next((col for col in columns if col.name == column), None)
    if target is None:
        raise RuntimeError(f"Column {column!r} not found in table {table!r} after migration")
    return {
        "type": target.type,
        "primary_key": target.is_pk,
        "not_null": target.notnull,
    }


def migrate(database: Path) -> dict[str, object]:
    if not database.exists():
        raise FileNotFoundError(
            f"Database file '{database}' does not exist. Provision the schema before running"
            " the migration."
        )

    with sqlite3.connect(str(database)) as conn:
        cursor = conn.cursor()
        _ensure_model_input(cursor)
        _ensure_model_output(cursor)
        conn.commit()

        input_summary = _column_summary(cursor, "model_input", "model_id")
        output_summary = _column_summary(cursor, "model_output", "model_id")

    return {
        "database": str(database),
        "model_input": input_summary,
        "model_output": output_summary,
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Migrate bigpopa.db schema to support hashed model identifiers.",
    )
    parser.add_argument(
        "--db",
        dest="database",
        default=None,
        help="Path to the bigpopa.db database file (defaults to configured path).",
    )

    args = parser.parse_args(argv)

    database = _resolve_database_path(args.database)
    result = migrate(database)
    payload = {
        "status": "success",
        "message": "Schema updated for hashed model identifiers.",
        **result,
    }
    print(json.dumps(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
