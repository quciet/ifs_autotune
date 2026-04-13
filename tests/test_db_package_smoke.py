from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from analysis import trend_analysis, trend_dataset_options
from db import ifs_metadata, input_profiles, migration, schema
from ifs import extract_compare, run_ifs, validate_ifs
from runtime import ml_driver, ml_progress, model_setup


def test_db_package_exports_expected_modules() -> None:
    assert schema.MODEL_RUN_TABLE == "model_run"
    assert schema.INPUT_PROFILE_TABLE == "input_profile"
    assert callable(ifs_metadata.ensure_ifs_metadata_schema)
    assert callable(input_profiles.resolve_profile)
    assert callable(migration.migrate_bigpopa_db)


def test_runtime_ifs_and_analysis_packages_export_main_entrypoints() -> None:
    assert callable(model_setup.main)
    assert callable(ml_driver.main)
    assert callable(ml_progress.main)
    assert callable(validate_ifs.main)
    assert callable(run_ifs.main)
    assert callable(extract_compare.main)
    assert callable(trend_analysis.main)
    assert callable(trend_dataset_options.main)
