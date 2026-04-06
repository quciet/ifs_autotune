from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import ml_progress
from model_run_store import insert_model_run
from model_status import FIT_EVALUATED, IFS_RUN_COMPLETED, IFS_RUN_FAILED, MODEL_REUSED
from tools.db.bigpopa_schema import ensure_current_bigpopa_schema


def build_progress_db(
    db_path: Path,
    *,
    seed_rows: list[tuple[str, str | None, str | None, float | None]] | None = None,
    trial_rows: list[
        tuple[
            str,
            str | None,
            str,
            float | None,
            int | None,
            int | None,
            str | None,
            str | None,
            bool,
        ]
    ]
    | None = None,
) -> None:
    if seed_rows is None:
        seed_rows = [
            ("seed-model", "dataset-1", None, None),
            ("other-seed", "dataset-2", None, None),
        ]

    if trial_rows is None:
        trial_rows = [
            (
                "trial-ok",
                "dataset-1",
                "completed",
                12.5,
                1,
                1,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
                False,
            ),
            (
                "trial-failed",
                "dataset-1",
                "failed",
                1e6,
                2,
                1,
                "2026-03-12T10:06:00Z",
                "2026-03-12T10:07:00Z",
                False,
            ),
            (
                "other-dataset",
                "dataset-2",
                "completed",
                3.0,
                1,
                1,
                "2026-03-12T10:08:00Z",
                "2026-03-12T10:09:00Z",
                False,
            ),
        ]

    conn = sqlite3.connect(db_path)
    try:
        ensure_current_bigpopa_schema(conn.cursor())
        for model_id, dataset_id, model_status, fit_pooled in seed_rows:
            insert_model_run(
                conn,
                ifs_id=1,
                model_id=model_id,
                dataset_id=dataset_id,
                input_param={"alpha": 1.0},
                input_coef={"demo": {"x": {"beta": 2.0}}},
                output_set={"WGDP": "hist_wgdp"},
                model_status=model_status,
                fit_pooled=fit_pooled,
                completed_at_utc="2026-03-12T09:00:00Z" if fit_pooled is not None else None,
                resolution_note="model_setup_seed",
            )
        for (
            model_id,
            dataset_id,
            model_status,
            fit_pooled,
            trial_index,
            batch_index,
            started_at_utc,
            completed_at_utc,
            was_reused,
        ) in trial_rows:
            insert_model_run(
                conn,
                ifs_id=1,
                model_id=model_id,
                dataset_id=dataset_id,
                input_param={"alpha": 1.0},
                input_coef={"demo": {"x": {"beta": 2.0}}},
                output_set={"WGDP": "hist_wgdp"},
                model_status=model_status,
                fit_pooled=fit_pooled,
                trial_index=trial_index,
                batch_index=batch_index,
                started_at_utc=started_at_utc,
                completed_at_utc=completed_at_utc,
                was_reused=was_reused,
                source_status=model_status,
            )
        conn.commit()
    finally:
        conn.close()


def test_main_normalizes_failed_trials_as_missing(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(db_path)

    exit_code = ml_progress.main(["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert payload["data"]["dataset_id"] == "dataset-1"
    assert payload["data"]["reference_model_id"] == "seed-model"
    assert payload["data"]["reference_fit_pooled"] is None
    assert payload["data"]["latest_run_id"] == 4
    assert payload["data"]["latest_progress_rowid"] == 4
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
            "progress_rowid": 3,
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
            "progress_rowid": 4,
        },
    ]


def test_main_can_return_incremental_trials_since_run_id(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(db_path)

    exit_code = ml_progress.main(
        [
            "--bigpopa-db",
            str(db_path),
            "--dataset-id",
            "dataset-1",
            "--since-run-id",
            "4",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["data"]["latest_run_id"] == 4
    assert [trial["progress_rowid"] for trial in payload["data"]["trials"]] == [4]


def test_main_reports_latest_run_id_even_when_no_new_trials(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(db_path)

    exit_code = ml_progress.main(
        [
            "--bigpopa-db",
            str(db_path),
            "--dataset-id",
            "dataset-1",
            "--since-run-id",
            "5",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["data"]["latest_run_id"] == 4
    assert payload["data"]["trials"] == []


def test_resolve_dataset_id_can_fall_back_from_model_id(tmp_path: Path) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(db_path)
    conn = sqlite3.connect(db_path)

    try:
        dataset_id = ml_progress.resolve_dataset_id(conn.cursor(), None, "seed-model")
    finally:
        conn.close()

    assert dataset_id == "dataset-1"


def test_main_returns_reused_runs_as_separate_progress_events(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        seed_rows=[("seed-model", "dataset-1", None, None)],
        trial_rows=[
            (
                "reused-model",
                "dataset-1",
                FIT_EVALUATED,
                12.5,
                1,
                1,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
                False,
            ),
            (
                "reused-model",
                "dataset-1",
                MODEL_REUSED,
                12.5,
                2,
                1,
                "2026-03-12T11:00:00Z",
                "2026-03-12T11:00:01Z",
                True,
            ),
        ],
    )

    exit_code = ml_progress.main(["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [trial["model_status"] for trial in payload["data"]["trials"]] == [
        FIT_EVALUATED,
        MODEL_REUSED,
    ]
    assert [trial["trial_index"] for trial in payload["data"]["trials"]] == [1, 2]


def test_main_derives_sequence_and_round_indexes_from_timestamp_order(
    tmp_path: Path, capsys
) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        seed_rows=[("seed-model", "dataset-1", None, None)],
        trial_rows=[
            (
                "round-3-first",
                "dataset-1",
                "completed",
                5.0,
                1,
                None,
                "2026-03-12T12:00:00Z",
                "2026-03-12T12:05:00Z",
                False,
            ),
            (
                "round-1-last",
                "dataset-1",
                "completed",
                12.5,
                250,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
                False,
            ),
            (
                "round-2-first",
                "dataset-1",
                "completed",
                7.25,
                1,
                None,
                "2026-03-12T11:00:00Z",
                "2026-03-12T11:05:00Z",
                False,
            ),
        ],
    )

    exit_code = ml_progress.main(["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"])

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
        seed_rows=[("seed-model", "dataset-1", None, None)],
        trial_rows=[
            (
                "completed-fallback",
                "dataset-1",
                "completed",
                9.0,
                2,
                None,
                None,
                "2026-03-12T10:02:00Z",
                False,
            ),
            (
                "started-first",
                "dataset-1",
                "completed",
                8.0,
                1,
                None,
                "2026-03-12T10:01:00Z",
                "2026-03-12T10:03:00Z",
                False,
            ),
        ],
    )

    exit_code = ml_progress.main(["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [trial["model_id"] for trial in payload["data"]["trials"]] == [
        "started-first",
        "completed-fallback",
    ]


def test_main_uses_stable_tie_breakers_when_timestamps_match(
    tmp_path: Path, capsys
) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        seed_rows=[("seed-model", "dataset-1", None, None)],
        trial_rows=[
            (
                "beta",
                "dataset-1",
                "completed",
                2.0,
                2,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
                False,
            ),
            (
                "alpha",
                "dataset-1",
                "completed",
                1.0,
                1,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
                False,
            ),
        ],
    )

    exit_code = ml_progress.main(["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [trial["model_id"] for trial in payload["data"]["trials"]] == ["alpha", "beta"]
    assert [trial["sequence_index"] for trial in payload["data"]["trials"]] == [1, 2]


def test_main_returns_reference_fit_from_dataset_baseline_and_excludes_it_from_trials(
    tmp_path: Path, capsys
) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        seed_rows=[("seed-model", "dataset-1", "completed", 15.0)],
        trial_rows=[
            (
                "first-trial",
                "dataset-1",
                "completed",
                8.0,
                1,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
                False,
            ),
            (
                "best-trial",
                "dataset-1",
                "completed",
                3.0,
                2,
                None,
                "2026-03-12T10:06:00Z",
                "2026-03-12T10:07:00Z",
                False,
            ),
        ],
    )

    exit_code = ml_progress.main(["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
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
        seed_rows=[("seed-model", "dataset-1", "completed", 15.0)],
        trial_rows=[
            (
                "first-trial",
                "dataset-1",
                "completed",
                8.0,
                1,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
                False,
            ),
            (
                "best-trial",
                "dataset-1",
                "completed",
                3.0,
                2,
                None,
                "2026-03-12T10:06:00Z",
                "2026-03-12T10:07:00Z",
                False,
            ),
        ],
    )

    exit_code = ml_progress.main(["--bigpopa-db", str(db_path), "--model-id", "seed-model"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["data"]["dataset_id"] == "dataset-1"
    assert payload["data"]["reference_model_id"] == "seed-model"
    assert payload["data"]["reference_fit_pooled"] == 15.0


def test_main_returns_null_reference_fit_when_dataset_baseline_has_no_output(
    tmp_path: Path, capsys
) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        seed_rows=[("seed-model", "dataset-1", None, None)],
        trial_rows=[
            (
                "trial-ok",
                "dataset-1",
                "completed",
                12.5,
                1,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
                False,
            ),
        ],
    )

    exit_code = ml_progress.main(["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["data"]["reference_model_id"] == "seed-model"
    assert payload["data"]["reference_fit_pooled"] is None


def test_main_hides_fallback_fit_for_missing_fit_statuses(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        seed_rows=[("seed-model", "dataset-1", None, None)],
        trial_rows=[
            (
                "ifs-failed",
                "dataset-1",
                IFS_RUN_FAILED,
                1e6,
                1,
                1,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
                False,
            ),
            (
                "fit-missing",
                "dataset-1",
                IFS_RUN_COMPLETED,
                1e6,
                2,
                1,
                "2026-03-12T10:06:00Z",
                "2026-03-12T10:07:00Z",
                False,
            ),
            (
                "fit-ok",
                "dataset-1",
                FIT_EVALUATED,
                4.5,
                3,
                1,
                "2026-03-12T10:08:00Z",
                "2026-03-12T10:09:00Z",
                False,
            ),
        ],
    )

    exit_code = ml_progress.main(["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [(trial["model_id"], trial["fit_missing"], trial["fit_pooled"]) for trial in payload["data"]["trials"]] == [
        ("ifs-failed", True, None),
        ("fit-missing", True, None),
        ("fit-ok", False, 4.5),
    ]


def test_reference_fit_hides_persisted_fallback_when_baseline_is_not_evaluated(
    tmp_path: Path, capsys
) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_progress_db(
        db_path,
        seed_rows=[("seed-model", "dataset-1", IFS_RUN_COMPLETED, 1e6)],
        trial_rows=[
            (
                "trial-ok",
                "dataset-1",
                FIT_EVALUATED,
                12.5,
                1,
                None,
                "2026-03-12T10:00:00Z",
                "2026-03-12T10:05:00Z",
                False,
            ),
        ],
    )

    exit_code = ml_progress.main(["--bigpopa-db", str(db_path), "--dataset-id", "dataset-1"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["data"]["reference_model_id"] == "seed-model"
    assert payload["data"]["reference_fit_pooled"] is None
