from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path
from typing import Any

from runtime.model_status import MODEL_REUSED


MODEL_RUN_TABLE = "model_run"
INPUT_PROFILE_TABLE = "input_profile"
INPUT_PROFILE_PARAMETER_TABLE = "input_profile_parameter"
INPUT_PROFILE_COEFFICIENT_TABLE = "input_profile_coefficient"
INPUT_PROFILE_OUTPUT_TABLE = "input_profile_output"
INPUT_PROFILE_ML_SETTINGS_TABLE = "input_profile_ml_settings"
LEGACY_TABLE_MODEL_INPUT = "model_input"
LEGACY_TABLE_MODEL_OUTPUT = "model_output"
LEGACY_TABLE_PROPOSAL_HISTORY = "ml_proposal_history"
LEGACY_TABLES = (
    LEGACY_TABLE_MODEL_INPUT,
    LEGACY_TABLE_MODEL_OUTPUT,
    LEGACY_TABLE_PROPOSAL_HISTORY,
)

UNIFIED_SCHEMA_VERSION = 3
BACKUP_BASENAME = "bigpopa.pre_model_run_unified.bak.db"


def table_exists(cursor: sqlite3.Cursor, table_name: str) -> bool:
    row = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def table_columns(cursor: sqlite3.Cursor, table_name: str) -> set[str]:
    if not table_exists(cursor, table_name):
        return set()
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cursor.fetchall()}


def get_user_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("PRAGMA user_version").fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def set_user_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(f"PRAGMA user_version = {int(version)}")


def ensure_model_run_table(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MODEL_RUN_TABLE} (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ifs_id INTEGER,
            model_id TEXT NOT NULL,
            dataset_id TEXT,
            input_param TEXT,
            input_coef TEXT,
            output_set TEXT,
            model_status TEXT,
            fit_var TEXT,
            fit_pooled REAL,
            trial_index INTEGER,
            batch_index INTEGER,
            started_at_utc TEXT,
            completed_at_utc TEXT,
            was_reused INTEGER NOT NULL DEFAULT 0,
            source_status TEXT,
            resolution_note TEXT,
            legacy_source TEXT,
            legacy_source_id INTEGER
        )
        """
    )
    cursor.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_model_run_legacy_source
        ON {MODEL_RUN_TABLE} (legacy_source, legacy_source_id)
        WHERE legacy_source IS NOT NULL AND legacy_source_id IS NOT NULL
        """
    )
    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_model_run_dataset_run
        ON {MODEL_RUN_TABLE} (dataset_id, run_id)
        """
    )
    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_model_run_model
        ON {MODEL_RUN_TABLE} (model_id)
        """
    )
    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_model_run_dataset_model
        ON {MODEL_RUN_TABLE} (dataset_id, model_id)
        """
    )
    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_model_run_started
        ON {MODEL_RUN_TABLE} (started_at_utc)
        """
    )
    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_model_run_completed
        ON {MODEL_RUN_TABLE} (completed_at_utc)
        """
    )


def ensure_ml_resume_state_table(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_resume_state (
            cohort_key TEXT PRIMARY KEY,
            dataset_id TEXT,
            base_year INTEGER,
            end_year INTEGER NOT NULL,
            settings_signature TEXT NOT NULL,
            settings_payload TEXT NOT NULL,
            proposal_seed INTEGER NOT NULL,
            effective_iteration_count INTEGER NOT NULL DEFAULT 0,
            no_improve_counter INTEGER NOT NULL DEFAULT 0,
            best_y_prev REAL,
            updated_at_utc TEXT NOT NULL
        )
        """
    )


def ensure_input_profile_tables(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {INPUT_PROFILE_TABLE} (
            profile_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ifs_static_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            archived INTEGER NOT NULL DEFAULT 0,
            source_type TEXT NOT NULL DEFAULT 'app',
            source_path TEXT
        )
        """
    )
    cursor.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_input_profile_name_per_static
        ON {INPUT_PROFILE_TABLE} (ifs_static_id, name)
        """
    )
    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_input_profile_static_archived
        ON {INPUT_PROFILE_TABLE} (ifs_static_id, archived, updated_at_utc DESC)
        """
    )

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {INPUT_PROFILE_PARAMETER_TABLE} (
            profile_id INTEGER NOT NULL,
            param_name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            minimum REAL,
            maximum REAL,
            step REAL,
            level_count INTEGER,
            sort_order INTEGER,
            PRIMARY KEY (profile_id, param_name),
            FOREIGN KEY (profile_id) REFERENCES {INPUT_PROFILE_TABLE}(profile_id)
        )
        """
    )

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {INPUT_PROFILE_COEFFICIENT_TABLE} (
            profile_id INTEGER NOT NULL,
            function_name TEXT NOT NULL,
            x_name TEXT NOT NULL,
            beta_name TEXT NOT NULL,
            y_name TEXT,
            source_sheet TEXT,
            enabled INTEGER NOT NULL DEFAULT 1,
            minimum REAL,
            maximum REAL,
            step REAL,
            level_count INTEGER,
            sort_order INTEGER,
            PRIMARY KEY (profile_id, function_name, x_name, beta_name),
            FOREIGN KEY (profile_id) REFERENCES {INPUT_PROFILE_TABLE}(profile_id)
        )
        """
    )

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {INPUT_PROFILE_OUTPUT_TABLE} (
            profile_id INTEGER NOT NULL,
            variable TEXT NOT NULL,
            table_name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER,
            PRIMARY KEY (profile_id, variable, table_name),
            FOREIGN KEY (profile_id) REFERENCES {INPUT_PROFILE_TABLE}(profile_id)
        )
        """
    )

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {INPUT_PROFILE_ML_SETTINGS_TABLE} (
            profile_id INTEGER PRIMARY KEY,
            ml_method TEXT NOT NULL,
            fit_metric TEXT NOT NULL,
            n_sample INTEGER NOT NULL DEFAULT 200,
            n_max_iteration INTEGER NOT NULL DEFAULT 30,
            n_convergence INTEGER NOT NULL DEFAULT 10,
            min_convergence_pct REAL NOT NULL DEFAULT 0.0001,
            FOREIGN KEY (profile_id) REFERENCES {INPUT_PROFILE_TABLE}(profile_id)
        )
        """
    )


def ensure_current_bigpopa_schema(cursor: sqlite3.Cursor) -> None:
    ensure_model_run_table(cursor)
    ensure_ml_resume_state_table(cursor)
    ensure_input_profile_tables(cursor)


def backup_bigpopa_db(db_path: Path) -> Path:
    backup_path = db_path.with_name(BACKUP_BASENAME)
    if not backup_path.exists():
        shutil.copy2(db_path, backup_path)
    return backup_path


def _table_count(cursor: sqlite3.Cursor, table_name: str) -> int:
    if not table_exists(cursor, table_name):
        return 0
    row = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _legacy_column_expr(available_columns: set[str], alias: str, column_name: str) -> str:
    if column_name in available_columns:
        return f"{alias}.{column_name}"
    return "NULL"


def _definition_only_input_count(cursor: sqlite3.Cursor) -> int:
    if not table_exists(cursor, LEGACY_TABLE_MODEL_INPUT):
        return 0
    if not table_exists(cursor, MODEL_RUN_TABLE):
        return 0
    row = cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {LEGACY_TABLE_MODEL_INPUT} mi
        WHERE NOT EXISTS (
            SELECT 1
            FROM {MODEL_RUN_TABLE} mr
            WHERE mr.model_id = mi.model_id
        )
        """
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def is_legacy_bigpopa_db(conn: sqlite3.Connection) -> bool:
    cursor = conn.cursor()
    if any(table_exists(cursor, table_name) for table_name in LEGACY_TABLES):
        return True
    return get_user_version(conn) < UNIFIED_SCHEMA_VERSION


def migrate_bigpopa_db_if_needed(
    conn: sqlite3.Connection,
    *,
    db_path: str | Path | None = None,
    create_backup: bool = True,
) -> dict[str, Any]:
    cursor = conn.cursor()
    original_version = get_user_version(conn)
    legacy_present = any(table_exists(cursor, table_name) for table_name in LEGACY_TABLES)
    legacy_model_input_rows_before = _table_count(cursor, LEGACY_TABLE_MODEL_INPUT)
    legacy_model_output_rows_before = _table_count(cursor, LEGACY_TABLE_MODEL_OUTPUT)
    legacy_ml_proposal_history_rows_before = _table_count(cursor, LEGACY_TABLE_PROPOSAL_HISTORY)
    ensure_current_bigpopa_schema(cursor)

    if not legacy_present and original_version >= UNIFIED_SCHEMA_VERSION:
        return {
            "performed": False,
            "original_version": original_version,
            "new_version": original_version,
            "backup_path": None,
            "legacy_tables_dropped": False,
            "model_run_rows": _table_count(cursor, MODEL_RUN_TABLE),
            "migrated_input_only_rows": 0,
            "migrated_proposal_rows": 0,
            "migrated_output_rows": 0,
        }

    db_path_resolved = Path(db_path).expanduser().resolve() if db_path is not None else None
    backup_path: Path | None = None
    if legacy_present and create_backup and db_path_resolved is not None:
        backup_path = backup_bigpopa_db(db_path_resolved)

    proposal_history_rows = 0
    model_output_rows = 0
    input_only_rows = 0

    input_columns = table_columns(cursor, LEGACY_TABLE_MODEL_INPUT)
    proposal_columns = table_columns(cursor, LEGACY_TABLE_PROPOSAL_HISTORY)
    output_columns = table_columns(cursor, LEGACY_TABLE_MODEL_OUTPUT)

    def input_expr(column_name: str) -> str:
        return _legacy_column_expr(input_columns, "mi", column_name)

    def proposal_expr(column_name: str) -> str:
        return _legacy_column_expr(proposal_columns, "mph", column_name)

    def output_expr(column_name: str) -> str:
        return _legacy_column_expr(output_columns, "mo", column_name)

    if table_exists(cursor, LEGACY_TABLE_PROPOSAL_HISTORY):
        cursor.execute(
            f"""
            INSERT INTO {MODEL_RUN_TABLE} (
                ifs_id,
                model_id,
                dataset_id,
                input_param,
                input_coef,
                output_set,
                model_status,
                fit_var,
                fit_pooled,
                trial_index,
                batch_index,
                started_at_utc,
                completed_at_utc,
                was_reused,
                source_status,
                resolution_note,
                legacy_source,
                legacy_source_id
            )
            SELECT
                COALESCE({proposal_expr('ifs_id')}, {input_expr('ifs_id')}),
                mph.model_id,
                mph.dataset_id,
                {input_expr('input_param')},
                {input_expr('input_coef')},
                {input_expr('output_set')},
                mph.proposal_status,
                NULL,
                mph.fit_pooled_visible,
                mph.trial_index,
                mph.batch_index,
                mph.started_at_utc,
                mph.completed_at_utc,
                COALESCE(mph.was_reused, 0),
                mph.source_status,
                COALESCE(mph.resolution_note, 'legacy_ml_proposal_history_migration'),
                ?,
                mph.proposal_event_id
            FROM {LEGACY_TABLE_PROPOSAL_HISTORY} mph
            LEFT JOIN {LEGACY_TABLE_MODEL_INPUT} mi ON mi.model_id = mph.model_id
            WHERE NOT EXISTS (
                SELECT 1
                FROM {MODEL_RUN_TABLE} mr
                WHERE mr.legacy_source = ?
                  AND mr.legacy_source_id = mph.proposal_event_id
            )
            """,
            (LEGACY_TABLE_PROPOSAL_HISTORY, LEGACY_TABLE_PROPOSAL_HISTORY),
        )
        proposal_history_rows = int(cursor.rowcount or 0)

    if table_exists(cursor, LEGACY_TABLE_MODEL_OUTPUT):
        cursor.execute(
            f"""
            INSERT INTO {MODEL_RUN_TABLE} (
                ifs_id,
                model_id,
                dataset_id,
                input_param,
                input_coef,
                output_set,
                model_status,
                fit_var,
                fit_pooled,
                trial_index,
                batch_index,
                started_at_utc,
                completed_at_utc,
                was_reused,
                source_status,
                resolution_note,
                legacy_source,
                legacy_source_id
            )
            SELECT
                COALESCE({output_expr('ifs_id')}, {input_expr('ifs_id')}),
                mo.model_id,
                {input_expr('dataset_id')},
                {input_expr('input_param')},
                {input_expr('input_coef')},
                {input_expr('output_set')},
                mo.model_status,
                {output_expr('fit_var')},
                mo.fit_pooled,
                {output_expr('trial_index')},
                {output_expr('batch_index')},
                {output_expr('started_at_utc')},
                {output_expr('completed_at_utc')},
                CASE WHEN mo.model_status IN (?, 'reused') THEN 1 ELSE 0 END,
                mo.model_status,
                'legacy_model_output_migration',
                ?,
                mo.rowid
            FROM {LEGACY_TABLE_MODEL_OUTPUT} mo
            LEFT JOIN {LEGACY_TABLE_MODEL_INPUT} mi ON mi.model_id = mo.model_id
            WHERE NOT EXISTS (
                SELECT 1
                FROM {MODEL_RUN_TABLE} mr
                WHERE mr.legacy_source = ?
                  AND mr.legacy_source_id = mo.rowid
            )
              AND NOT EXISTS (
                SELECT 1
                FROM {MODEL_RUN_TABLE} mr
                WHERE mr.model_id = mo.model_id
                  AND (
                        (mr.trial_index IS NULL AND {output_expr('trial_index')} IS NULL)
                        OR mr.trial_index = {output_expr('trial_index')}
                      )
                  AND (
                        (mr.batch_index IS NULL AND {output_expr('batch_index')} IS NULL)
                        OR mr.batch_index = {output_expr('batch_index')}
                      )
                  AND (
                        COALESCE(mr.started_at_utc, '') = COALESCE({output_expr('started_at_utc')}, '')
                        OR COALESCE(mr.completed_at_utc, '') = COALESCE({output_expr('completed_at_utc')}, '')
                      )
              )
            """,
            (
                MODEL_REUSED,
                LEGACY_TABLE_MODEL_OUTPUT,
                LEGACY_TABLE_MODEL_OUTPUT,
            ),
        )
        model_output_rows = int(cursor.rowcount or 0)

    if table_exists(cursor, LEGACY_TABLE_MODEL_INPUT):
        cursor.execute(
            f"""
            INSERT INTO {MODEL_RUN_TABLE} (
                ifs_id,
                model_id,
                dataset_id,
                input_param,
                input_coef,
                output_set,
                model_status,
                fit_var,
                fit_pooled,
                trial_index,
                batch_index,
                started_at_utc,
                completed_at_utc,
                was_reused,
                source_status,
                resolution_note,
                legacy_source,
                legacy_source_id
            )
            SELECT
                mi.ifs_id,
                mi.model_id,
                mi.dataset_id,
                mi.input_param,
                mi.input_coef,
                mi.output_set,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                0,
                NULL,
                'legacy_model_input_definition',
                ?,
                mi.rowid
            FROM {LEGACY_TABLE_MODEL_INPUT} mi
            WHERE NOT EXISTS (
                SELECT 1
                FROM {MODEL_RUN_TABLE} mr
                WHERE mr.model_id = mi.model_id
            )
            """,
            (LEGACY_TABLE_MODEL_INPUT,),
        )
        input_only_rows = int(cursor.rowcount or 0)

    invalid_payload_rows = cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {MODEL_RUN_TABLE}
        WHERE COALESCE(TRIM(input_param), '') = ''
           OR COALESCE(TRIM(input_coef), '') = ''
           OR COALESCE(TRIM(output_set), '') = ''
        """
    ).fetchone()
    invalid_payload_count = int(invalid_payload_rows[0]) if invalid_payload_rows else 0
    if invalid_payload_count > 0:
        raise RuntimeError(
            "Unified DB migration failed validation because some model_run rows are missing required JSON payloads."
        )

    legacy_tables_dropped = False
    if legacy_present:
        for table_name in LEGACY_TABLES:
            if table_exists(cursor, table_name):
                cursor.execute(f"DROP TABLE {table_name}")
        legacy_tables_dropped = True

    set_user_version(conn, UNIFIED_SCHEMA_VERSION)
    conn.commit()

    return {
        "performed": legacy_present or original_version < UNIFIED_SCHEMA_VERSION,
        "original_version": original_version,
        "new_version": UNIFIED_SCHEMA_VERSION,
        "backup_path": str(backup_path) if backup_path is not None else None,
        "legacy_tables_dropped": legacy_tables_dropped,
        "model_run_rows": _table_count(cursor, MODEL_RUN_TABLE),
        "migrated_input_only_rows": input_only_rows,
        "migrated_proposal_rows": proposal_history_rows,
        "migrated_output_rows": model_output_rows,
        "legacy_model_input_rows_before": legacy_model_input_rows_before,
        "legacy_model_output_rows_before": legacy_model_output_rows_before,
        "legacy_ml_proposal_history_rows_before": legacy_ml_proposal_history_rows_before,
    }
