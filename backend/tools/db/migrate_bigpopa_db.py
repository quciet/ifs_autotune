"""Upgrade BIGPOPA databases to the unified model_run schema."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from tools.db.bigpopa_schema import migrate_bigpopa_db_if_needed


def emit(status: str, message: str, **data: object) -> None:
    payload = {"status": status, "message": message}
    if data:
        payload["data"] = data
    print(json.dumps(payload))
    sys.stdout.flush()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Upgrade BIGPOPA DB to the unified model_run schema")
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
        summary = migrate_bigpopa_db_if_needed(
            conn,
            db_path=db_path,
            create_backup=True,
        )

        emit(
            "success",
            "BIGPOPA DB migration applied.",
            bigpopa_db=str(db_path),
            **summary,
        )
        return 0
    except sqlite3.Error as exc:
        emit("error", "Unable to migrate database.", bigpopa_db=str(db_path), error=str(exc))
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
