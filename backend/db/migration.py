from __future__ import annotations

from pathlib import Path
from typing import Any

from db.schema import (
    BACKUP_BASENAME,
    UNIFIED_SCHEMA_VERSION,
    migrate_bigpopa_db_if_needed,
)

__all__ = [
    "BACKUP_BASENAME",
    "UNIFIED_SCHEMA_VERSION",
    "migrate_bigpopa_db_if_needed",
]


def migrate_bigpopa_db(
    *,
    db_path: Path,
    create_backup: bool = True,
) -> dict[str, Any]:
    import sqlite3

    with sqlite3.connect(str(db_path)) as conn:
        return migrate_bigpopa_db_if_needed(
            conn,
            db_path=db_path,
            create_backup=create_backup,
        )
