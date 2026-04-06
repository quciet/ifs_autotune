from __future__ import annotations

import json
import io
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import run_ifs
from model_run_store import insert_model_run
from model_status import FALLBACK_FIT_POOLED, IFS_RUN_COMPLETED
from tools.db.bigpopa_schema import ensure_current_bigpopa_schema


def _create_bigpopa_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        ensure_current_bigpopa_schema(conn.cursor())
        conn.execute(
            "CREATE TABLE ifs_version (ifs_id INTEGER PRIMARY KEY, ifs_static_id INTEGER)"
        )
        insert_model_run(
            conn,
            ifs_id=7,
            model_id="model-1",
            dataset_id="dataset-1",
            input_param={},
            input_coef={},
            output_set={"WGDP": "hist_wgdp"},
            model_status=None,
            started_at_utc="2026-03-12T09:00:00Z",
        )
        conn.execute(
            "INSERT INTO ifs_version (ifs_id, ifs_static_id) VALUES (?, ?)",
            (7, 11),
        )


def test_refresh_dyadic_work_database_copies_source_when_present(tmp_path: Path) -> None:
    ifs_root = tmp_path / "ifs"
    source_db = ifs_root / "DATA" / "IFsForDyadic.db"
    work_db = ifs_root / "RUNFILES" / "ifsForDyadicWork.db"
    source_db.parent.mkdir(parents=True)
    work_db.parent.mkdir(parents=True)
    source_db.write_text("new dyadic data", encoding="utf-8")
    work_db.write_text("stale data", encoding="utf-8")

    copied = run_ifs._refresh_dyadic_work_database(str(ifs_root))

    assert copied is True
    assert work_db.read_text(encoding="utf-8") == "new dyadic data"


def test_refresh_dyadic_work_database_noops_when_source_missing(tmp_path: Path) -> None:
    ifs_root = tmp_path / "ifs"

    copied = run_ifs._refresh_dyadic_work_database(str(ifs_root))

    assert copied is False
    assert not (ifs_root / "RUNFILES" / "ifsForDyadicWork.db").exists()


def test_main_stops_before_launch_when_dyadic_refresh_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ifs_root = tmp_path / "ifs"
    output_dir = tmp_path / "output"
    _create_bigpopa_db(output_dir / "bigpopa.db")

    responses: list[dict[str, object]] = []
    popen_called = False

    monkeypatch.setattr(run_ifs, "apply_config_to_ifs_files", lambda **kwargs: None)

    def fake_refresh(_: str) -> bool:
        raise OSError("copy failed")

    def fake_emit(status: str, stage: str, message: str, data: dict[str, object]) -> None:
        responses.append(
            {"status": status, "stage": stage, "message": message, "data": data}
        )

    def fake_popen(*args, **kwargs):
        nonlocal popen_called
        popen_called = True
        raise AssertionError("subprocess.Popen should not be called")

    monkeypatch.setattr(run_ifs, "_refresh_dyadic_work_database", fake_refresh)
    monkeypatch.setattr(run_ifs, "emit_stage_response", fake_emit)
    monkeypatch.setattr(run_ifs.subprocess, "Popen", fake_popen)

    result = run_ifs.main(
        [
            "--ifs-root",
            str(ifs_root),
            "--output-dir",
            str(output_dir),
            "--model-id",
            "model-1",
            "--ifs-id",
            "7",
            "--base-year",
            "2020",
            "--end-year",
            "2030",
        ]
    )

    assert result == 1
    assert popen_called is False
    assert responses[-1]["status"] == "error"
    assert responses[-1]["stage"] == "run_ifs"
    assert "Failed to refresh dyadic working database" in str(responses[-1]["message"])


def test_main_refreshes_dyadic_db_before_launch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ifs_root = tmp_path / "ifs"
    output_dir = tmp_path / "output"
    _create_bigpopa_db(output_dir / "bigpopa.db")

    call_order: list[str] = []
    responses: list[dict[str, object]] = []

    monkeypatch.setattr(run_ifs, "apply_config_to_ifs_files", lambda **kwargs: None)

    def fake_refresh(_: str) -> bool:
        call_order.append("refresh")
        return True

    def fake_emit(status: str, stage: str, message: str, data: dict[str, object]) -> None:
        responses.append(
            {"status": status, "stage": stage, "message": message, "data": data}
        )

    def fake_popen(*args, **kwargs):
        call_order.append("popen")
        raise RuntimeError("stop after launch ordering check")

    monkeypatch.setattr(run_ifs, "_refresh_dyadic_work_database", fake_refresh)
    monkeypatch.setattr(run_ifs, "emit_stage_response", fake_emit)
    monkeypatch.setattr(run_ifs.subprocess, "Popen", fake_popen)

    result = run_ifs.main(
        [
            "--ifs-root",
            str(ifs_root),
            "--output-dir",
            str(output_dir),
            "--model-id",
            "model-1",
            "--ifs-id",
            "7",
            "--base-year",
            "2020",
            "--end-year",
            "2030",
        ]
    )

    assert result == 1
    assert call_order == ["refresh", "popen"]
    assert any(
        response["status"] == "info"
        and response["stage"] == "run_ifs"
        and "Refreshed dyadic working database" in str(response["message"])
        for response in responses
    )


def test_main_keeps_ifs_completed_status_when_extract_compare_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ifs_root = tmp_path / "ifs"
    output_dir = tmp_path / "output"
    model_folder = output_dir / "model-1"
    model_folder.mkdir(parents=True)
    _create_bigpopa_db(output_dir / "bigpopa.db")
    (model_folder / "Working.model-1.run.db").write_text("db", encoding="utf-8")

    monkeypatch.setattr(run_ifs, "apply_config_to_ifs_files", lambda **kwargs: None)
    monkeypatch.setattr(run_ifs, "_refresh_dyadic_work_database", lambda _: False)
    monkeypatch.setattr(run_ifs, "_read_progress_summary", lambda _: (2030, 123.0))
    monkeypatch.setattr(
        run_ifs,
        "_prepare_run_artifacts",
        lambda **kwargs: {"model_folder": str(model_folder)},
    )
    monkeypatch.setattr(run_ifs, "_reset_working_database", lambda _: None)

    class FakeProcess:
        def __init__(self) -> None:
            self.stdout = io.StringIO("")

        def wait(self) -> int:
            return 0

    monkeypatch.setattr(run_ifs.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    monkeypatch.setattr(
        run_ifs.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            subprocess.CalledProcessError(returncode=2, cmd=args[0])
        ),
    )

    result = run_ifs.main(
        [
            "--ifs-root",
            str(ifs_root),
            "--output-dir",
            str(output_dir),
            "--model-id",
            "model-1",
            "--ifs-id",
            "7",
            "--base-year",
            "2020",
            "--end-year",
            "2030",
        ]
    )

    with sqlite3.connect(output_dir / "bigpopa.db") as conn:
        row = conn.execute(
            """
            SELECT model_status, fit_pooled
            FROM model_run
            WHERE model_id = ?
            ORDER BY run_id DESC
            LIMIT 1
            """,
            ("model-1",),
        ).fetchone()

    assert result == 1
    assert row == (IFS_RUN_COMPLETED, FALLBACK_FIT_POOLED)


def test_main_treats_handled_missing_pooled_fit_as_successful_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ifs_root = tmp_path / "ifs"
    output_dir = tmp_path / "output"
    model_folder = output_dir / "model-1"
    model_folder.mkdir(parents=True)
    _create_bigpopa_db(output_dir / "bigpopa.db")
    (model_folder / "Working.model-1.run.db").write_text("db", encoding="utf-8")
    responses: list[dict[str, object]] = []

    monkeypatch.setattr(run_ifs, "apply_config_to_ifs_files", lambda **kwargs: None)
    monkeypatch.setattr(run_ifs, "_refresh_dyadic_work_database", lambda _: False)
    monkeypatch.setattr(run_ifs, "_read_progress_summary", lambda _: (2030, 123.0))
    monkeypatch.setattr(
        run_ifs,
        "_prepare_run_artifacts",
        lambda **kwargs: {"model_folder": str(model_folder)},
    )
    monkeypatch.setattr(run_ifs, "_reset_working_database", lambda _: None)
    monkeypatch.setattr(
        run_ifs,
        "emit_stage_response",
        lambda status, stage, message, data: responses.append(
            {"status": status, "stage": stage, "message": message, "data": data}
        ),
    )

    class FakeProcess:
        def __init__(self) -> None:
            self.stdout = io.StringIO("")

        def wait(self) -> int:
            return 0

    monkeypatch.setattr(run_ifs.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    monkeypatch.setattr(
        run_ifs.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], returncode=0),
    )

    result = run_ifs.main(
        [
            "--ifs-root",
            str(ifs_root),
            "--output-dir",
            str(output_dir),
            "--model-id",
            "model-1",
            "--ifs-id",
            "7",
            "--base-year",
            "2020",
            "--end-year",
            "2030",
        ]
    )

    with sqlite3.connect(output_dir / "bigpopa.db") as conn:
        row = conn.execute(
            """
            SELECT model_status, fit_pooled
            FROM model_run
            WHERE model_id = ?
            ORDER BY run_id DESC
            LIMIT 1
            """,
            ("model-1",),
        ).fetchone()

    assert result == 0
    assert row == (IFS_RUN_COMPLETED, FALLBACK_FIT_POOLED)
    assert responses[-1]["status"] == "success"
    assert responses[-1]["stage"] == "extract_compare"
