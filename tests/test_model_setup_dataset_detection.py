from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import model_setup


def _parameter_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE parameter (
            ifs_static_id INTEGER,
            param_name TEXT,
            param_default REAL
        )
        """
    )
    conn.executemany(
        "INSERT INTO parameter (ifs_static_id, param_name, param_default) VALUES (?, ?, ?)",
        [
            (7, "gdprext", 1.0),
            (7, "tfrconv", 2.0),
            (7, "wmigrm", 3.0),
        ],
    )
    return conn


def test_extract_enabled_ifsv_names_accepts_equivalent_switch_representations() -> None:
    frames = [
        pd.DataFrame({"Switch": [1, "0"], "Name": ["gdprext", "ignored"]}),
        pd.DataFrame({"Switch": [1.0, "off"], "Variable": ["gdprext", "ignored"]}),
        pd.DataFrame({"Switch": ["on", "no"], "Name/Variable": ["gdprext", "ignored"]}),
        pd.DataFrame({"Switch": ["true", "0"], "Name": ["tfrconv", "ignored"]}),
    ]

    names = [
        model_setup.extract_enabled_ifsv_names(frame)
        for frame in frames
    ]

    assert names[0] == ["gdprext"]
    assert names[1] == ["gdprext"]
    assert names[2] == ["gdprext"]
    assert names[3] == ["tfrconv"]


def test_dataset_id_is_stable_for_equivalent_ifsvar_layouts() -> None:
    conn = _parameter_db()
    cursor = conn.cursor()
    input_coef = {"demo": {"x": {"a": 10.0}}}
    output_set = {"POP": "Population"}

    frame_a = pd.DataFrame(
        {
            "Switch": [1, "on"],
            "Name": ["gdprext", ""],
            "Variable": ["", "tfrconv"],
        }
    )
    frame_b = pd.DataFrame(
        {
            "Switch": ["1.0", "true"],
            "Name/Variable": ["gdprext", "tfrconv"],
        }
    )

    try:
        input_param_a = model_setup.build_input_param_from_defaults(
            cursor, 7, model_setup.extract_enabled_ifsv_names(frame_a)
        )
        input_param_b = model_setup.build_input_param_from_defaults(
            cursor, 7, model_setup.extract_enabled_ifsv_names(frame_b)
        )
    finally:
        conn.close()

    dataset_id_a = model_setup.compute_dataset_id(
        ifs_id=2,
        input_param=input_param_a,
        input_coef=input_coef,
        output_set=output_set,
    )
    dataset_id_b = model_setup.compute_dataset_id(
        ifs_id=2,
        input_param=input_param_b,
        input_coef=input_coef,
        output_set=output_set,
    )

    assert input_param_a == {"gdprext": 1.0, "tfrconv": 2.0}
    assert input_param_b == {"gdprext": 1.0, "tfrconv": 2.0}
    assert dataset_id_a == dataset_id_b


def test_dataset_id_changes_when_selected_parameter_set_changes() -> None:
    input_coef = {"demo": {"x": {"a": 10.0}}}
    output_set = {"POP": "Population"}

    dataset_id_full = model_setup.compute_dataset_id(
        ifs_id=2,
        input_param={"gdprext": 1.0, "tfrconv": 2.0},
        input_coef=input_coef,
        output_set=output_set,
    )
    dataset_id_missing = model_setup.compute_dataset_id(
        ifs_id=2,
        input_param={"tfrconv": 2.0},
        input_coef=input_coef,
        output_set=output_set,
    )

    assert dataset_id_full != dataset_id_missing


def test_diagnose_structure_drift_reports_parameter_changes() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE model_input (
            ifs_id INTEGER,
            model_id TEXT PRIMARY KEY,
            input_param TEXT,
            input_coef TEXT,
            output_set TEXT,
            dataset_id TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO model_input (ifs_id, model_id, input_param, input_coef, output_set, dataset_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            2,
            "existing-model",
            json.dumps({"gdprext": 1.0, "tfrconv": 2.0}),
            json.dumps({"demo": {"x": {"a": 10.0}}}),
            json.dumps({"POP": "Population"}),
            "existing-dataset",
        ),
    )

    try:
        diagnostics = model_setup.diagnose_structure_drift(
            conn.cursor(),
            2,
            {"tfrconv": 2.0, "wmigrm": 3.0},
            {"demo": {"x": {"a": 10.0}}},
            {"POP": "Population"},
        )
    finally:
        conn.close()

    assert diagnostics is not None
    assert diagnostics["reference_model_id"] == "existing-model"
    assert diagnostics["parameter_keys_added"] == ["wmigrm"]
    assert diagnostics["parameter_keys_removed"] == ["gdprext"]
    assert "added parameters: wmigrm" in model_setup.format_structure_drift_warning(
        diagnostics
    )
