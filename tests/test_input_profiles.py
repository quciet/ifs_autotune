from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from db import input_profiles
from db.ifs_metadata import ensure_ifs_metadata_schema
from db.schema import (
    INPUT_PROFILE_COEFFICIENT_TABLE,
    INPUT_PROFILE_ML_SETTINGS_TABLE,
    INPUT_PROFILE_OUTPUT_TABLE,
    INPUT_PROFILE_PARAMETER_TABLE,
    INPUT_PROFILE_TABLE,
    ensure_current_bigpopa_schema,
)


def _seed_static_catalog(output_dir: Path) -> None:
    db_path = output_dir / "bigpopa.db"
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        ensure_current_bigpopa_schema(cursor)
        ensure_ifs_metadata_schema(cursor)
        cursor.execute(
            "INSERT INTO ifs_static (ifs_static_id, version_number, base_year) VALUES (?, ?, ?)",
            (1, "8.01", 2020),
        )
        cursor.execute(
            """
            INSERT INTO parameter (
                ifs_static_id, param_name, param_type, param_default, param_min, param_max
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (1, "A", "parameter", 0.5, 0.0, 1.0),
        )
        cursor.execute(
            """
            INSERT INTO coefficient (
                ifs_static_id, function_name, y_name, x_name, reg_seq,
                beta_name, beta_default, beta_std
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "Func", "Y", "X", 1, "Beta", 1.25, 0.1),
        )
        conn.commit()


def test_schema_creation_adds_input_profile_tables(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    db_path = output_dir / "bigpopa.db"

    with sqlite3.connect(db_path) as conn:
        ensure_current_bigpopa_schema(conn.cursor())
        table_names = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }

    assert {
        INPUT_PROFILE_TABLE,
        INPUT_PROFILE_PARAMETER_TABLE,
        INPUT_PROFILE_COEFFICIENT_TABLE,
        INPUT_PROFILE_OUTPUT_TABLE,
        INPUT_PROFILE_ML_SETTINGS_TABLE,
    }.issubset(table_names)


def test_profile_crud_and_resolution_round_trip(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    _seed_static_catalog(output_dir)

    created = input_profiles.create_profile(
        output_folder=output_dir,
        ifs_static_id=1,
        name="Baseline",
        description="Test profile",
    )
    profile_id = int(created["profile"]["profile_id"])
    assert created["profile"]["description"] == "Test profile"

    blank_detail = input_profiles.get_profile(output_folder=output_dir, profile_id=profile_id)
    assert blank_detail["validation"]["valid"] is False
    assert blank_detail["validation"]["enabled_param_count"] == 0
    assert blank_detail["validation"]["enabled_coefficient_count"] == 0
    assert blank_detail["validation"]["enabled_output_count"] == 0

    input_profiles.save_parameters(
        output_folder=output_dir,
        profile_id=profile_id,
        rows=[
            {
                "param_name": "A",
                "enabled": True,
                "minimum": 0.2,
                "maximum": 0.8,
                "step": 0.1,
                "level_count": 7,
            }
        ],
    )
    input_profiles.save_coefficients(
        output_folder=output_dir,
        profile_id=profile_id,
        rows=[
            {
                "function_name": "Func",
                "x_name": "X",
                "beta_name": "Beta",
                "enabled": True,
                "minimum": 0.5,
                "maximum": 2.0,
            }
        ],
    )
    input_profiles.save_outputs(
        output_folder=output_dir,
        profile_id=profile_id,
        rows=[{"variable": "WGDP", "table_name": "hist_wgdp", "enabled": True}],
    )
    input_profiles.save_ml_settings(
        output_folder=output_dir,
        profile_id=profile_id,
        payload={
            "ml_method": "tree",
            "fit_metric": "mse",
            "n_sample": 25,
            "n_max_iteration": 4,
            "n_convergence": 2,
            "min_convergence_pct": 0.001,
        },
    )

    profiles = input_profiles.list_profiles(output_folder=output_dir, ifs_static_id=1)
    assert [profile["name"] for profile in profiles["profiles"]] == ["Baseline"]

    detail = input_profiles.get_profile(output_folder=output_dir, profile_id=profile_id)
    assert detail["validation"]["valid"] is True
    assert detail["validation"]["enabled_param_count"] == 1
    assert detail["validation"]["enabled_coefficient_count"] == 1
    assert detail["validation"]["enabled_output_count"] == 1

    resolved = input_profiles.resolve_profile(output_folder=output_dir, profile_id=profile_id)
    assert resolved.input_param == {"A": 0.5}
    assert resolved.input_coef == {"Func": {"X": {"Beta": 1.25}}}
    assert resolved.output_set == {"WGDP": "hist_wgdp"}
    assert resolved.ml_settings.ml_method.model_type == "tree"
    assert resolved.ml_settings.n_sample == 25


def test_profile_validation_rejects_missing_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    _seed_static_catalog(output_dir)

    created = input_profiles.create_profile(
        output_folder=output_dir,
        ifs_static_id=1,
        name="Invalid",
    )
    profile_id = int(created["profile"]["profile_id"])
    input_profiles.save_parameters(
        output_folder=output_dir,
        profile_id=profile_id,
        rows=[{"param_name": "A", "enabled": True}],
    )

    validation = input_profiles.validate_profile(output_folder=output_dir, profile_id=profile_id)
    assert validation["validation"]["valid"] is False
    assert "At least one enabled output variable is required." in validation["validation"]["errors"]


def test_profile_persists_negative_parameter_and_coefficient_bounds(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    _seed_static_catalog(output_dir)

    created = input_profiles.create_profile(
        output_folder=output_dir,
        ifs_static_id=1,
        name="Negative bounds",
    )
    profile_id = int(created["profile"]["profile_id"])

    input_profiles.save_parameters(
        output_folder=output_dir,
        profile_id=profile_id,
        rows=[
            {
                "param_name": "A",
                "enabled": True,
                "minimum": -1.0,
                "maximum": 0.5,
                "step": 0.25,
            }
        ],
    )
    input_profiles.save_coefficients(
        output_folder=output_dir,
        profile_id=profile_id,
        rows=[
            {
                "function_name": "Func",
                "x_name": "X",
                "beta_name": "Beta",
                "enabled": True,
                "minimum": -0.25,
                "maximum": 0.25,
            }
        ],
    )
    input_profiles.save_outputs(
        output_folder=output_dir,
        profile_id=profile_id,
        rows=[{"variable": "WGDP", "table_name": "hist_wgdp", "enabled": True}],
    )

    detail = input_profiles.get_profile(output_folder=output_dir, profile_id=profile_id)
    parameter_row = next(row for row in detail["parameter_catalog"] if row["param_name"] == "A")
    coefficient_row = next(
        row
        for row in detail["coefficient_catalog"]
        if row["function_name"] == "Func" and row["x_name"] == "X" and row["beta_name"] == "Beta"
    )

    assert parameter_row["minimum"] == -1.0
    assert parameter_row["maximum"] == 0.5
    assert coefficient_row["minimum"] == -0.25
    assert coefficient_row["maximum"] == 0.25

    resolved = input_profiles.resolve_profile(output_folder=output_dir, profile_id=profile_id)
    assert resolved.parameter_configs["A"].minimum == -1.0
    assert resolved.parameter_configs["A"].maximum == 0.5
    assert resolved.coefficient_configs[("Func", "X", "Beta")].minimum == -0.25
    assert resolved.coefficient_configs[("Func", "X", "Beta")].maximum == 0.25


def test_profile_validation_rejects_invalid_negative_bound_order(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    _seed_static_catalog(output_dir)

    created = input_profiles.create_profile(
        output_folder=output_dir,
        ifs_static_id=1,
        name="Bad bounds",
    )
    profile_id = int(created["profile"]["profile_id"])

    try:
        input_profiles.save_parameters(
            output_folder=output_dir,
            profile_id=profile_id,
            rows=[
                {
                    "param_name": "A",
                    "enabled": True,
                    "minimum": 1.0,
                    "maximum": -1.0,
                }
            ],
        )
    except ValueError as exc:
        assert "minimum must be less than or equal to maximum" in str(exc)
    else:
        raise AssertionError("Expected invalid parameter bound order to raise ValueError.")
