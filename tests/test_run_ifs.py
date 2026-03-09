from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import run_ifs


def _create_bigpopa_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE model_input (model_id TEXT PRIMARY KEY, input_param TEXT, input_coef TEXT)"
        )
        conn.execute(
            "CREATE TABLE ifs_version (ifs_id INTEGER PRIMARY KEY, ifs_static_id INTEGER)"
        )
        conn.execute(
            "INSERT INTO model_input (model_id, input_param, input_coef) VALUES (?, ?, ?)",
            ("model-1", json.dumps({}), json.dumps({})),
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
