from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import validate_ifs
from tools.db.bigpopa_schema import BACKUP_BASENAME, UNIFIED_SCHEMA_VERSION, migrate_bigpopa_db_if_needed


def _create_legacy_db(
    db_path: Path,
    *,
    include_proposal_history: bool = False,
    overlapping_history: bool = False,
    include_input_only_model: bool = False,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE model_input (
                ifs_id INTEGER,
                model_id TEXT PRIMARY KEY,
                dataset_id TEXT,
                input_param TEXT,
                input_coef TEXT,
                output_set TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE model_output (
                ifs_id INTEGER,
                model_id TEXT PRIMARY KEY,
                model_status TEXT,
                fit_var TEXT,
                fit_pooled REAL,
                trial_index INTEGER,
                batch_index INTEGER,
                started_at_utc TEXT,
                completed_at_utc TEXT
            )
            """
        )
        if include_proposal_history:
            conn.execute(
                """
                CREATE TABLE ml_proposal_history (
                    proposal_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ifs_id INTEGER,
                    model_id TEXT NOT NULL,
                    dataset_id TEXT,
                    trial_index INTEGER NOT NULL,
                    batch_index INTEGER NOT NULL,
                    proposal_status TEXT,
                    fit_pooled_visible REAL,
                    started_at_utc TEXT,
                    completed_at_utc TEXT,
                    was_reused INTEGER NOT NULL DEFAULT 0,
                    source_status TEXT,
                    resolution_note TEXT
                )
                """
            )

        conn.executemany(
            """
            INSERT INTO model_input (ifs_id, model_id, dataset_id, input_param, input_coef, output_set)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    1,
                    "model-a",
                    "dataset-1",
                    json.dumps({"alpha": 1.0}),
                    json.dumps({"demo": {"x": {"beta": 2.0}}}),
                    json.dumps({"WGDP": "hist_wgdp"}),
                ),
                (
                    1,
                    "model-b",
                    "dataset-1",
                    json.dumps({"alpha": 1.1}),
                    json.dumps({"demo": {"x": {"beta": 2.1}}}),
                    json.dumps({"WGDP": "hist_wgdp"}),
                ),
            ]
            + (
                [
                    (
                        1,
                        "definition-only",
                        "dataset-1",
                        json.dumps({"alpha": 9.9}),
                        json.dumps({"demo": {"x": {"beta": 9.9}}}),
                        json.dumps({"WGDP": "hist_wgdp"}),
                    )
                ]
                if include_input_only_model
                else []
            ),
        )
        conn.executemany(
            """
            INSERT INTO model_output (
                ifs_id,
                model_id,
                model_status,
                fit_var,
                fit_pooled,
                trial_index,
                batch_index,
                started_at_utc,
                completed_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    1,
                    "model-a",
                    "fit_evaluated",
                    json.dumps({"WGDP": 1.5}),
                    1.5,
                    1,
                    1,
                    "2026-03-20T00:00:00Z",
                    "2026-03-20T00:01:00Z",
                ),
                (
                    1,
                    "model-b",
                    "failed",
                    None,
                    1e6,
                    2,
                    1,
                    "2026-03-20T00:02:00Z",
                    "2026-03-20T00:03:00Z",
                ),
            ],
        )
        if include_proposal_history:
            rows = [
                (
                    1,
                    "model-a",
                    "dataset-1",
                    1,
                    1,
                    "fit_evaluated",
                    1.5,
                    "2026-03-20T00:00:00Z",
                    "2026-03-20T00:01:00Z",
                    0,
                    "fit_evaluated",
                    "proposal_row_a",
                ),
                (
                    1,
                    "model-b",
                    "dataset-1",
                    2 if overlapping_history else 3,
                    1,
                    "model_reused" if not overlapping_history else "failed",
                    1e6 if overlapping_history else 1.5,
                    "2026-03-20T00:02:00Z" if overlapping_history else "2026-03-20T00:04:00Z",
                    "2026-03-20T00:03:00Z" if overlapping_history else "2026-03-20T00:04:01Z",
                    0 if overlapping_history else 1,
                    "failed" if overlapping_history else "fit_evaluated",
                    "proposal_row_b",
                ),
            ]
            conn.executemany(
                """
                INSERT INTO ml_proposal_history (
                    ifs_id,
                    model_id,
                    dataset_id,
                    trial_index,
                    batch_index,
                    proposal_status,
                    fit_pooled_visible,
                    started_at_utc,
                    completed_at_utc,
                    was_reused,
                    source_status,
                    resolution_note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        conn.execute("PRAGMA user_version = 0")
        conn.commit()


def test_migration_upgrades_legacy_db_and_drops_old_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "bigpopa.db"
    _create_legacy_db(db_path, include_input_only_model=True)

    with sqlite3.connect(db_path) as conn:
        summary = migrate_bigpopa_db_if_needed(conn, db_path=db_path, create_backup=True)

    assert summary["performed"] is True
    assert summary["new_version"] == UNIFIED_SCHEMA_VERSION
    assert summary["legacy_tables_dropped"] is True
    assert Path(str(summary["backup_path"])) == db_path.with_name(BACKUP_BASENAME)
    assert db_path.with_name(BACKUP_BASENAME).exists()

    with sqlite3.connect(db_path) as conn:
        table_names = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        rows = conn.execute(
            """
            SELECT model_id, resolution_note
            FROM model_run
            ORDER BY run_id
            """
        ).fetchall()
        version = conn.execute("PRAGMA user_version").fetchone()[0]

    assert "model_input" not in table_names
    assert "model_output" not in table_names
    assert "ml_proposal_history" not in table_names
    assert "model_run" in table_names
    assert version == UNIFIED_SCHEMA_VERSION
    assert rows == [
        ("model-a", "legacy_model_output_migration"),
        ("model-b", "legacy_model_output_migration"),
        ("definition-only", "legacy_model_input_definition"),
    ]


def test_migration_deduplicates_overlapping_output_and_proposal_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "bigpopa.db"
    _create_legacy_db(db_path, include_proposal_history=True, overlapping_history=True)

    with sqlite3.connect(db_path) as conn:
        summary = migrate_bigpopa_db_if_needed(conn, db_path=db_path, create_backup=False)

    assert summary["migrated_proposal_rows"] == 2
    assert summary["migrated_output_rows"] == 0

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT model_id, trial_index, batch_index, legacy_source
            FROM model_run
            ORDER BY run_id
            """
        ).fetchall()

    assert rows == [
        ("model-a", 1, 1, "ml_proposal_history"),
        ("model-b", 2, 1, "ml_proposal_history"),
    ]


def test_migration_keeps_non_overlapping_history_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "bigpopa.db"
    _create_legacy_db(db_path, include_proposal_history=True, overlapping_history=False)

    with sqlite3.connect(db_path) as conn:
        summary = migrate_bigpopa_db_if_needed(conn, db_path=db_path, create_backup=False)

    assert summary["migrated_proposal_rows"] == 2
    assert summary["migrated_output_rows"] == 1

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT model_id, trial_index, model_status, was_reused
            FROM model_run
            ORDER BY run_id
            """
        ).fetchall()

    assert rows == [
        ("model-a", 1, "fit_evaluated", 0),
        ("model-b", 3, "model_reused", 1),
        ("model-b", 2, "failed", 0),
    ]


def test_ensure_working_db_copies_and_upgrades_legacy_template(tmp_path: Path, monkeypatch) -> None:
    workspace = tmp_path / "workspace"
    template_db = workspace / "desktop" / "input" / "template" / "bigpopa_clean.db"
    _create_legacy_db(template_db)
    (workspace / "desktop" / "input" / "template" / "StartingPointTable_clean.xlsx").write_text(
        "placeholder",
        encoding="utf-8",
    )
    monkeypatch.chdir(workspace)

    summary = validate_ifs.ensure_working_db()

    working_db = workspace / "desktop" / "output" / "bigpopa.db"
    assert summary is not None
    assert summary["performed"] is True
    assert working_db.exists()
    assert working_db.with_name(BACKUP_BASENAME).exists()

    with sqlite3.connect(working_db) as conn:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        legacy = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('model_input','model_output','ml_proposal_history')"
        ).fetchall()

    assert version == UNIFIED_SCHEMA_VERSION
    assert legacy == []
