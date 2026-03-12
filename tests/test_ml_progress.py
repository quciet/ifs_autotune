from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import ml_progress


def build_progress_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE model_input (
            model_id TEXT PRIMARY KEY,
            dataset_id TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE model_output (
            model_id TEXT PRIMARY KEY,
            model_status TEXT,
            fit_pooled REAL,
            trial_index INTEGER,
            batch_index INTEGER,
            started_at_utc TEXT,
            completed_at_utc TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO model_input (model_id, dataset_id) VALUES (?, ?)",
        [
            ("seed-model", "dataset-1"),
            ("trial-ok", "dataset-1"),
            ("trial-failed", "dataset-1"),
            ("other-dataset", "dataset-2"),
        ],
    )
    conn.executemany(
        """
        INSERT INTO model_output (
            model_id,
            model_status,
            fit_pooled,
            trial_index,
            batch_index,
            started_at_utc,
            completed_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "trial-ok",
                "completed",
                12.5,
                1,
                9,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
            ),
            (
                "trial-failed",
                "failed",
                1e6,
                2,
                17,
                "2026-03-12T10:06:00Z",
                "2026-03-12T10:07:00Z",
            ),
            (
                "other-dataset",
                "completed",
                3.0,
                3,
                23,
                "2026-03-12T10:08:00Z",
                "2026-03-12T10:09:00Z",
            ),
        ],
    )
    conn.commit()
    conn.close()


def test_main_normalizes_failed_trials_as_missing(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(db_path)

    exit_code = ml_progress.main(
        ["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert payload["data"]["dataset_id"] == "dataset-1"
    assert payload["data"]["trials"] == [
        {
            "model_id": "trial-ok",
            "model_status": "completed",
            "fit_pooled": 12.5,
            "fit_missing": False,
            "trial_index": 1,
            "batch_index": 1,
            "started_at_utc": "2026-03-12T10:00:00Z",
            "completed_at_utc": "2026-03-12T10:05:00Z",
            "dataset_id": "dataset-1",
        },
        {
            "model_id": "trial-failed",
            "model_status": "failed",
            "fit_pooled": None,
            "fit_missing": True,
            "trial_index": 2,
            "batch_index": 1,
            "started_at_utc": "2026-03-12T10:06:00Z",
            "completed_at_utc": "2026-03-12T10:07:00Z",
            "dataset_id": "dataset-1",
        },
    ]


def test_repair_batch_indexes_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(db_path)
    conn = sqlite3.connect(db_path)

    try:
        repaired_rows = ml_progress.repair_model_output_batch_indexes(conn)
        assert repaired_rows == 3

        repeated_rows = ml_progress.repair_model_output_batch_indexes(conn)
        assert repeated_rows == 0

        rows = conn.execute(
            "SELECT model_id, batch_index FROM model_output ORDER BY trial_index"
        ).fetchall()
    finally:
        conn.close()

    assert rows == [
        ("trial-ok", 1),
        ("trial-failed", 1),
        ("other-dataset", 1),
    ]


def test_resolve_dataset_id_can_fall_back_from_model_id(tmp_path: Path) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(db_path)
    conn = sqlite3.connect(db_path)

    try:
        dataset_id = ml_progress.resolve_dataset_id(conn.cursor(), None, "seed-model")
    finally:
        conn.close()

    assert dataset_id == "dataset-1"
