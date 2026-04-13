from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import extract_compare
import ml_driver
import ml_progress
import model_setup
import run_ifs
import trend_analysis
import trend_dataset_options
import validate_ifs
from analysis import trend_analysis as trend_analysis_impl
from analysis import trend_dataset_options as trend_dataset_options_impl
from ifs import extract_compare as extract_compare_impl
from ifs import run_ifs as run_ifs_impl
from ifs import validate_ifs as validate_ifs_impl
from runtime import ml_driver as ml_driver_impl
from runtime import ml_progress as ml_progress_impl
from runtime import model_setup as model_setup_impl


def test_root_wrappers_delegate_to_packaged_main_functions() -> None:
    assert extract_compare.main is extract_compare_impl.main
    assert ml_driver.main is ml_driver_impl.main
    assert ml_progress.main is ml_progress_impl.main
    assert model_setup.main is model_setup_impl.main
    assert run_ifs.main is run_ifs_impl.main
    assert trend_analysis.main is trend_analysis_impl.main
    assert trend_dataset_options.main is trend_dataset_options_impl.main
    assert validate_ifs.main is validate_ifs_impl.main


def test_validate_ifs_wrapper_style_launch_uses_sys_argv(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        validate_ifs_impl,
        "_initialize_working_files",
        lambda: {"performed": False},
    )
    monkeypatch.setattr(sys, "argv", ["validate_ifs.py"])

    exit_code = validate_ifs.main()
    payload = json.loads(capsys.readouterr().out.strip())

    assert exit_code == 1
    assert payload == {"valid": False, "missingFiles": ["No folder path provided"]}
