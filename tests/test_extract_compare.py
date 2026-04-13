from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from ifs import extract_compare
from runtime.model_run_store import insert_model_run
from runtime.model_status import FALLBACK_FIT_POOLED, IFS_RUN_COMPLETED
from db.schema import ensure_current_bigpopa_schema


def _create_fixture(root: Path, *, fit_metric: str) -> tuple[Path, Path, Path]:
    ifs_root = root / "ifs"
    runfiles_dir = ifs_root / "RUNFILES"
    runfiles_dir.mkdir(parents=True)
    output_dir = root / "output"
    model_dir = output_dir / "model-1"
    model_dir.mkdir(parents=True)
    model_db = model_dir / "Working.model-1.run.db"
    bigpopa_db = output_dir / "bigpopa.db"

    with sqlite3.connect(bigpopa_db) as conn:
        ensure_current_bigpopa_schema(conn.cursor())
        conn.execute(
            """
            CREATE TABLE ifs_version (
                ifs_id INTEGER PRIMARY KEY,
                fit_metric TEXT
            )
            """
        )
        insert_model_run(
            conn,
            ifs_id=7,
            model_id="model-1",
            dataset_id="dataset-1",
            input_param={},
            input_coef={},
            output_set={"WGDP": "hist_wgdp"},
            model_status=IFS_RUN_COMPLETED,
            fit_var=None,
            fit_pooled=FALLBACK_FIT_POOLED,
            trial_index=1,
            batch_index=1,
            started_at_utc="2026-03-12T10:00:00Z",
        )
        conn.execute(
            "INSERT INTO ifs_version (ifs_id, fit_metric) VALUES (?, ?)",
            (7, fit_metric),
        )

    with sqlite3.connect(model_db) as conn:
        conn.execute("CREATE TABLE ifs_var_blob (VariableName TEXT, Data BLOB)")
        conn.execute(
            "INSERT INTO ifs_var_blob (VariableName, Data) VALUES (?, ?)",
            ("WGDP", b"parquet-bytes"),
        )

    with sqlite3.connect(runfiles_dir / "IFsHistSeries.db") as conn:
        conn.execute("CREATE TABLE hist_wgdp (year INTEGER, value REAL)")
        conn.execute("INSERT INTO hist_wgdp (year, value) VALUES (?, ?)", (2020, 1.0))

    return ifs_root, model_db, bigpopa_db


def _fake_parquet_conversion(model_dir: Path):
    def _runner(*args, **kwargs):
        for parquet_path in model_dir.glob("*.parquet"):
            csv_path = parquet_path.with_suffix(".csv")
            csv_path.write_text("0,1\n2020,1.0\n", encoding="utf-8")
        return None

    return _runner


def test_main_handles_missing_pooled_mse_fit_as_completed_with_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    ifs_root, model_db, bigpopa_db = _create_fixture(tmp_path, fit_metric="mse")
    responses: list[dict[str, object]] = []

    monkeypatch.setattr(
        extract_compare,
        "combine_var_hist",
        lambda *args, **kwargs: pd.DataFrame({"v": [None], "v_h": [None]}),
    )
    monkeypatch.setattr(
        extract_compare.subprocess,
        "run",
        _fake_parquet_conversion(model_db.parent),
    )
    monkeypatch.setattr(
        extract_compare,
        "emit_stage_response",
        lambda status, stage, message, data: responses.append(
            {"status": status, "stage": stage, "message": message, "data": data}
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "extract_compare.py",
            "--ifs-root",
            str(ifs_root),
            "--model-db",
            str(model_db),
            "--model-id",
            "model-1",
            "--ifs-id",
            "7",
            "--bigpopa-db",
            str(bigpopa_db),
        ],
    )

    exit_code = extract_compare.main()

    with sqlite3.connect(bigpopa_db) as conn:
        row = conn.execute(
            """
            SELECT model_status, fit_var, fit_pooled
            FROM model_run
            WHERE model_id = ?
            ORDER BY run_id DESC
            LIMIT 1
            """,
            ("model-1",),
        ).fetchone()

    assert exit_code == 0
    assert row == (IFS_RUN_COMPLETED, None, FALLBACK_FIT_POOLED)
    assert responses[-1]["status"] == "success"
    assert "without a pooled fit metric" in str(responses[-1]["message"])
    assert responses[-1]["data"]["fit_pooled"] is None
    assert responses[-1]["data"]["persisted_fit_pooled"] == FALLBACK_FIT_POOLED


def test_main_handles_missing_pooled_r2_fit_as_completed_with_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    ifs_root, model_db, bigpopa_db = _create_fixture(tmp_path, fit_metric="r2")
    responses: list[dict[str, object]] = []

    monkeypatch.setattr(
        extract_compare,
        "combine_var_hist",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "v": [1.0, 2.0, 3.0],
                "v_h": [5.0, 5.0, 5.0],
                "1": ["USA", "USA", "USA"],
                "0": [2020, 2021, 2022],
            }
        ),
    )
    monkeypatch.setattr(
        extract_compare.subprocess,
        "run",
        _fake_parquet_conversion(model_db.parent),
    )
    monkeypatch.setattr(
        extract_compare,
        "emit_stage_response",
        lambda status, stage, message, data: responses.append(
            {"status": status, "stage": stage, "message": message, "data": data}
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "extract_compare.py",
            "--ifs-root",
            str(ifs_root),
            "--model-db",
            str(model_db),
            "--model-id",
            "model-1",
            "--ifs-id",
            "7",
            "--bigpopa-db",
            str(bigpopa_db),
        ],
    )

    exit_code = extract_compare.main()

    with sqlite3.connect(bigpopa_db) as conn:
        row = conn.execute(
            """
            SELECT model_status, fit_var, fit_pooled
            FROM model_run
            WHERE model_id = ?
            ORDER BY run_id DESC
            LIMIT 1
            """,
            ("model-1",),
        ).fetchone()

    assert exit_code == 0
    assert row == (IFS_RUN_COMPLETED, None, FALLBACK_FIT_POOLED)
    assert responses[-1]["status"] == "success"
    assert "without a pooled fit metric" in str(responses[-1]["message"])
    assert responses[-1]["data"]["fit_pooled"] is None
    assert responses[-1]["data"]["persisted_fit_pooled"] == FALLBACK_FIT_POOLED
