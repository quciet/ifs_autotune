from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import ml_progress
from model_status import FIT_EVALUATED, IFS_RUN_COMPLETED, IFS_RUN_FAILED


def build_progress_db(
    db_path: Path,
    *,
    input_rows: list[tuple[str, str | None]] | None = None,
    output_rows: list[tuple[str, str, float | None, int | None, int | None, str | None, str | None]]
    | None = None,
) -> None:
    if input_rows is None:
        input_rows = [
            ("seed-model", "dataset-1"),
            ("trial-ok", "dataset-1"),
            ("trial-failed", "dataset-1"),
            ("other-dataset", "dataset-2"),
        ]

    if output_rows is None:
        output_rows = [
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
        ]

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
        input_rows,
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
        output_rows,
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
    assert payload["data"]["reference_model_id"] == "seed-model"
    assert payload["data"]["reference_fit_pooled"] is None
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
            "sequence_index": 1,
            "derived_round_index": 1,
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
            "sequence_index": 2,
            "derived_round_index": 1,
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


def test_main_derives_sequence_and_round_indexes_from_timestamp_order(
    tmp_path: Path, capsys
) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        input_rows=[
            ("seed-model", "dataset-1"),
            ("round-1-last", "dataset-1"),
            ("round-2-first", "dataset-1"),
            ("round-3-first", "dataset-1"),
        ],
        output_rows=[
            (
                "round-3-first",
                "completed",
                5.0,
                1,
                None,
                "2026-03-12T12:00:00Z",
                "2026-03-12T12:05:00Z",
            ),
            (
                "round-1-last",
                "completed",
                12.5,
                250,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
            ),
            (
                "round-2-first",
                "completed",
                7.25,
                1,
                None,
                "2026-03-12T11:00:00Z",
                "2026-03-12T11:05:00Z",
            ),
        ],
    )

    exit_code = ml_progress.main(
        ["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    trials = payload["data"]["trials"]

    assert [trial["model_id"] for trial in trials] == [
        "round-1-last",
        "round-2-first",
        "round-3-first",
    ]
    assert [trial["trial_index"] for trial in trials] == [250, 1, 1]
    assert [trial["sequence_index"] for trial in trials] == [1, 2, 3]
    assert [trial["derived_round_index"] for trial in trials] == [1, 2, 3]


def test_main_falls_back_to_completed_timestamp_when_started_missing(
    tmp_path: Path, capsys
) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        input_rows=[
            ("seed-model", "dataset-1"),
            ("started-first", "dataset-1"),
            ("completed-fallback", "dataset-1"),
        ],
        output_rows=[
            (
                "completed-fallback",
                "completed",
                9.0,
                2,
                None,
                None,
                "2026-03-12T10:02:00Z",
            ),
            (
                "started-first",
                "completed",
                8.0,
                1,
                None,
                "2026-03-12T10:01:00Z",
                "2026-03-12T10:03:00Z",
            ),
        ],
    )

    exit_code = ml_progress.main(
        ["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    trials = payload["data"]["trials"]

    assert [trial["model_id"] for trial in trials] == [
        "started-first",
        "completed-fallback",
    ]
    assert [trial["sequence_index"] for trial in trials] == [1, 2]


def test_main_uses_stable_tie_breakers_when_timestamps_match(
    tmp_path: Path, capsys
) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        input_rows=[
            ("seed-model", "dataset-1"),
            ("alpha", "dataset-1"),
            ("beta", "dataset-1"),
        ],
        output_rows=[
            (
                "beta",
                "completed",
                2.0,
                2,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
            ),
            (
                "alpha",
                "completed",
                1.0,
                1,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
            ),
        ],
    )

    exit_code = ml_progress.main(
        ["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    trials = payload["data"]["trials"]

    assert [trial["model_id"] for trial in trials] == ["alpha", "beta"]
    assert [trial["sequence_index"] for trial in trials] == [1, 2]


def test_main_returns_reference_fit_from_dataset_baseline_and_excludes_it_from_trials(
    tmp_path: Path, capsys
) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        input_rows=[
            ("seed-model", "dataset-1"),
            ("first-trial", "dataset-1"),
            ("best-trial", "dataset-1"),
        ],
        output_rows=[
            (
                "seed-model",
                "completed",
                15.0,
                None,
                None,
                "2026-03-12T09:00:00Z",
                "2026-03-12T09:05:00Z",
            ),
            (
                "first-trial",
                "completed",
                8.0,
                1,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
            ),
            (
                "best-trial",
                "completed",
                3.0,
                2,
                None,
                "2026-03-12T10:06:00Z",
                "2026-03-12T10:07:00Z",
            ),
        ],
    )

    exit_code = ml_progress.main(["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["data"]["dataset_id"] == "dataset-1"
    assert payload["data"]["reference_model_id"] == "seed-model"
    assert payload["data"]["reference_fit_pooled"] == 15.0
    assert [trial["model_id"] for trial in payload["data"]["trials"]] == [
        "first-trial",
        "best-trial",
    ]


def test_main_returns_reference_fit_from_exact_seed_model_id(
    tmp_path: Path, capsys
) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        input_rows=[
            ("seed-model", "dataset-1"),
            ("first-trial", "dataset-1"),
            ("best-trial", "dataset-1"),
        ],
        output_rows=[
            (
                "seed-model",
                "completed",
                15.0,
                None,
                None,
                "2026-03-12T09:00:00Z",
                "2026-03-12T09:05:00Z",
            ),
            (
                "first-trial",
                "completed",
                8.0,
                1,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
            ),
            (
                "best-trial",
                "completed",
                3.0,
                2,
                None,
                "2026-03-12T10:06:00Z",
                "2026-03-12T10:07:00Z",
            ),
        ],
    )

    exit_code = ml_progress.main(
        ["--bigpopa-db", str(db_path), "--model-id", "seed-model"]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["data"]["dataset_id"] == "dataset-1"
    assert payload["data"]["reference_model_id"] == "seed-model"
    assert payload["data"]["reference_fit_pooled"] == 15.0
    assert [trial["model_id"] for trial in payload["data"]["trials"]] == [
        "first-trial",
        "best-trial",
    ]


def test_main_returns_null_reference_fit_when_dataset_baseline_has_no_output(
    tmp_path: Path, capsys
) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        input_rows=[
            ("seed-model", "dataset-1"),
            ("trial-ok", "dataset-1"),
        ],
        output_rows=[
            (
                "trial-ok",
                "completed",
                12.5,
                1,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
            ),
        ],
    )

    exit_code = ml_progress.main(["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["data"]["reference_model_id"] == "seed-model"
    assert payload["data"]["reference_fit_pooled"] is None


def test_main_hides_fallback_fit_for_new_missing_fit_statuses(
    tmp_path: Path, capsys
) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        input_rows=[
            ("seed-model", "dataset-1"),
            ("ifs-failed", "dataset-1"),
            ("fit-missing", "dataset-1"),
            ("fit-ok", "dataset-1"),
        ],
        output_rows=[
            (
                "ifs-failed",
                IFS_RUN_FAILED,
                1e6,
                1,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
            ),
            (
                "fit-missing",
                IFS_RUN_COMPLETED,
                1e6,
                2,
                None,
                "2026-03-12T10:06:00Z",
                "2026-03-12T10:07:00Z",
            ),
            (
                "fit-ok",
                FIT_EVALUATED,
                4.5,
                3,
                None,
                "2026-03-12T10:08:00Z",
                "2026-03-12T10:09:00Z",
            ),
        ],
    )

    exit_code = ml_progress.main(["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["data"]["trials"] == [
        {
            "model_id": "ifs-failed",
            "model_status": IFS_RUN_FAILED,
            "fit_pooled": None,
            "fit_missing": True,
            "trial_index": 1,
            "batch_index": 1,
            "started_at_utc": "2026-03-12T10:00:00Z",
            "completed_at_utc": "2026-03-12T10:05:00Z",
            "dataset_id": "dataset-1",
            "sequence_index": 1,
            "derived_round_index": 1,
        },
        {
            "model_id": "fit-missing",
            "model_status": IFS_RUN_COMPLETED,
            "fit_pooled": None,
            "fit_missing": True,
            "trial_index": 2,
            "batch_index": 1,
            "started_at_utc": "2026-03-12T10:06:00Z",
            "completed_at_utc": "2026-03-12T10:07:00Z",
            "dataset_id": "dataset-1",
            "sequence_index": 2,
            "derived_round_index": 1,
        },
        {
            "model_id": "fit-ok",
            "model_status": FIT_EVALUATED,
            "fit_pooled": 4.5,
            "fit_missing": False,
            "trial_index": 3,
            "batch_index": 1,
            "started_at_utc": "2026-03-12T10:08:00Z",
            "completed_at_utc": "2026-03-12T10:09:00Z",
            "dataset_id": "dataset-1",
            "sequence_index": 3,
            "derived_round_index": 1,
        },
    ]


def test_reference_fit_hides_persisted_fallback_when_baseline_is_not_evaluated(
    tmp_path: Path, capsys
) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        input_rows=[
            ("seed-model", "dataset-1"),
            ("trial-ok", "dataset-1"),
        ],
        output_rows=[
            (
                "seed-model",
                IFS_RUN_COMPLETED,
                1e6,
                None,
                None,
                "2026-03-12T09:00:00Z",
                "2026-03-12T09:05:00Z",
            ),
            (
                "trial-ok",
                FIT_EVALUATED,
                12.5,
                1,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
            ),
        ],
    )

    exit_code = ml_progress.main(["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["data"]["reference_model_id"] == "seed-model"
    assert payload["data"]["reference_fit_pooled"] is None
