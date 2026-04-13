from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from ifs import validate_ifs


def _create_minimal_ifs_root(root: Path) -> Path:
    (root / "DATA").mkdir(parents=True, exist_ok=True)
    (root / "RUNFILES").mkdir(parents=True, exist_ok=True)
    (root / "Scenario").mkdir(parents=True, exist_ok=True)
    (root / "net8").mkdir(parents=True, exist_ok=True)

    for file_path in (
        root / "DATA" / "SAMBase.db",
        root / "RUNFILES" / "DataDict.db",
        root / "RUNFILES" / "IFsHistSeries.db",
        root / "net8" / "ifs.exe",
    ):
        file_path.write_text("", encoding="utf-8")

    with sqlite3.connect(root / "IFsInit.db") as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS IFsInit (Variable TEXT, Value TEXT)")
        conn.executemany(
            "INSERT INTO IFsInit (Variable, Value) VALUES (?, ?)",
            [
                ("LastYearHistory", "2020"),
                ("FirstYearForecast", "2021"),
            ],
        )
        conn.commit()

    return root


def test_validation_succeeds_for_valid_folders_without_selected_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ifs_root = _create_minimal_ifs_root(tmp_path / "ifs")
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    monkeypatch.setattr(
        validate_ifs,
        "ensure_static_metadata",
        lambda **kwargs: {"ifs_static_id": 11},
    )

    result = validate_ifs.validate_ifs_folder(
        str(ifs_root),
        output_path=str(output_dir),
        input_profile_id=None,
        migration_summary={"performed": False},
    )

    assert result["valid"] is True
    assert result["ifs_static_id"] == 11
    assert result["pathChecks"]["inputProfile"]["exists"] is False
    assert result["pathChecks"]["inputProfile"]["valid"] is False
    assert "inputFile" not in result["pathChecks"]


def test_validation_reports_invalid_selected_profile_without_using_excel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ifs_root = _create_minimal_ifs_root(tmp_path / "ifs")
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    monkeypatch.setattr(
        validate_ifs,
        "ensure_static_metadata",
        lambda **kwargs: {"ifs_static_id": 7},
    )
    monkeypatch.setattr(
        validate_ifs,
        "validate_profile",
        lambda **kwargs: {
            "profile": {
                "profile_id": 19,
                "ifs_static_id": 7,
                "name": "Invalid profile",
            },
            "validation": {
                "valid": False,
                "errors": ["At least one enabled output variable is required."],
            },
        },
    )

    result = validate_ifs.validate_ifs_folder(
        str(ifs_root),
        output_path=str(output_dir),
        input_profile_id=19,
        migration_summary={"performed": False},
    )

    assert result["valid"] is True
    assert result["profileReady"] is False
    assert result["pathChecks"]["inputProfile"]["profileId"] == 19
    assert result["pathChecks"]["inputProfile"]["valid"] is False
    assert result["pathChecks"]["inputProfile"]["errors"] == [
        "At least one enabled output variable is required."
    ]


def test_packaged_validate_cli_main_uses_sys_argv_when_omitted(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        validate_ifs,
        "_initialize_working_files",
        lambda: {"performed": False},
    )
    monkeypatch.setattr(sys, "argv", ["validate_ifs.py"])

    exit_code = validate_ifs.main()
    payload = json.loads(capsys.readouterr().out.strip())

    assert exit_code == 1
    assert payload == {"valid": False, "missingFiles": ["No folder path provided"]}


def test_frontend_and_electron_contracts_do_not_expose_excel_validation_fields() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    api_source = (repo_root / "frontend" / "src" / "api.ts").read_text(encoding="utf-8")
    global_source = (repo_root / "frontend" / "src" / "global.d.ts").read_text(encoding="utf-8")
    main_source = (repo_root / "desktop" / "main.js").read_text(encoding="utf-8")
    preload_source = (repo_root / "desktop" / "preload.js").read_text(encoding="utf-8")

    forbidden_tokens = [
        "inputExcelPath",
        "inputFilePath",
        "ValidationInputFileCheck",
        "getDefaultInputFile",
        "StartingPointTable.xlsx",
    ]

    for token in forbidden_tokens:
        assert token not in api_source
        assert token not in global_source
        assert token not in main_source
        assert token not in preload_source
