from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from analysis import trend_dataset_options
from runtime.model_run_store import insert_model_run
from db.schema import ensure_current_bigpopa_schema


def _build_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        ensure_current_bigpopa_schema(conn.cursor())
        rows = [
            ("dataset-a", "a-1", 1, 0.9, "2026-03-24T00:00:00Z"),
            ("dataset-a", "a-2", 2, 0.7, "2026-03-24T00:02:00Z"),
            ("dataset-b", "b-1", 1, 0.8, "2026-03-24T01:00:00Z"),
            ("dataset-b", "b-2", 2, 0.6, "2026-03-24T01:02:00Z"),
        ]
        for dataset_id, model_id, trial_index, fit_pooled, started_at_utc in rows:
            insert_model_run(
                conn,
                ifs_id=1,
                model_id=model_id,
                dataset_id=dataset_id,
                input_param={"alpha": float(trial_index)},
                input_coef={"demo": {"x": {"beta": float(trial_index)}}},
                output_set={"WGDP": "hist_wgdp"},
                model_status="completed",
                fit_pooled=fit_pooled,
                trial_index=trial_index,
                batch_index=1,
                started_at_utc=started_at_utc,
                completed_at_utc=started_at_utc,
            )


def test_trend_dataset_options_returns_recent_datasets(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "bigpopa.db"
    _build_db(db_path)

    exit_code = trend_dataset_options.main(["--bigpopa-db", str(db_path)])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert payload["data"]["latest_dataset_id"] == "dataset-b"
    assert payload["data"]["dataset_ids"] == ["dataset-b", "dataset-a"]
    assert payload["data"]["dataset_run_counts"] == {
        "dataset-b": 2,
        "dataset-a": 2,
    }
    assert payload["data"]["latest_dataset_run_count"] == 2


def test_trend_dataset_options_reports_missing_db(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "missing.db"

    exit_code = trend_dataset_options.main(["--bigpopa-db", str(db_path)])

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "error"
    assert "bigpopa.db" in payload["message"].lower()
