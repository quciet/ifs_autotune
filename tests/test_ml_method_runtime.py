from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import ml_driver
import model_setup
from ml_method import load_required_ml_method
from optimization import active_learning
from optimization.ensemble_training import validate_surrogate_memory


def _write_workbook(path: Path, *, ml_method: str | None) -> None:
    rows: list[dict[str, object]] = [
        {"Method": "general", "Parameter": "n_sample", "Value": 10},
        {"Method": "general", "Parameter": "n_max_iteration", "Value": 1},
    ]
    if ml_method is not None:
        rows.append({"Method": "general", "Parameter": "ml_method", "Value": ml_method})

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, sheet_name="ML", index=False)


def _create_bigpopa_db(
    path: Path,
    *,
    initial_model_id: str = "initial-model",
    persisted_ml_method: str | None = "tree",
) -> None:
    conn = sqlite3.connect(path)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE model_input (
                ifs_id INTEGER,
                model_id TEXT PRIMARY KEY,
                dataset_id TEXT,
                input_param TEXT,
                input_coef TEXT,
                output_set TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE ifs_version (
                ifs_id INTEGER PRIMARY KEY,
                ifs_static_id INTEGER,
                base_year INTEGER,
                end_year INTEGER,
                fit_metric TEXT,
                ml_method TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE model_output (
                ifs_id INTEGER,
                model_id TEXT PRIMARY KEY,
                model_status TEXT,
                fit_var TEXT,
                fit_pooled REAL
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO model_input (ifs_id, model_id, dataset_id, input_param, input_coef, output_set)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                initial_model_id,
                "dataset-1",
                json.dumps({"a": 0.5}),
                json.dumps({}),
                json.dumps({"fit": 1}),
            ),
        )
        cursor.execute(
            """
            INSERT INTO ifs_version (ifs_id, ifs_static_id, base_year, end_year, fit_metric, ml_method)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (1, 1, 2020, 2030, "mse", persisted_ml_method),
        )
        conn.commit()
    finally:
        conn.close()


def _dimension() -> ml_driver.SearchDimension:
    return ml_driver.SearchDimension(
        key=("param", "a"),
        display_name="parameter 'a'",
        kind="param",
        default=0.5,
        minimum=0.0,
        maximum=1.0,
    )


@pytest.mark.parametrize(
    ("persisted_ml_method", "workbook_value", "expected_model_type"),
    [
        ("neural network", "tree", "nn"),
        ("poly", "neural network", "poly"),
    ],
)
def test_ml_driver_uses_db_ml_method_even_when_workbook_differs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    persisted_ml_method: str,
    workbook_value: str,
    expected_model_type: str,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    db_path = output_dir / "bigpopa.db"
    workbook_path = tmp_path / "StartingPointTable.xlsx"
    _create_bigpopa_db(db_path, persisted_ml_method=persisted_ml_method)
    _write_workbook(workbook_path, ml_method=workbook_value)

    monkeypatch.setattr(
        ml_driver.dataset_utils,
        "load_compatible_training_samples",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        ml_driver,
        "_build_search_space",
        lambda *args, **kwargs: [_dimension()],
    )

    captured: dict[str, object] = {}

    def fake_active_learning_loop(**kwargs):
        captured["model_type"] = kwargs["model_type"]
        return (
            np.asarray([[0.5]], dtype=float),
            np.asarray([1.0], dtype=float),
            np.asarray([], dtype=object),
            {},
            False,
        )

    monkeypatch.setattr(ml_driver, "active_learning_loop", fake_active_learning_loop)

    exit_code = ml_driver.main(
        [
            "--ifs-root",
            str(tmp_path),
            "--end-year",
            "2030",
            "--output-folder",
            str(output_dir),
            "--initial-model-id",
            "initial-model",
            "--starting-point-table",
            str(workbook_path),
        ]
    )

    assert exit_code == 0
    assert captured["model_type"] == expected_model_type


def test_ml_driver_works_when_workbook_omits_ml_method_but_db_has_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    db_path = output_dir / "bigpopa.db"
    workbook_path = tmp_path / "StartingPointTable.xlsx"
    _create_bigpopa_db(db_path, persisted_ml_method="tree")
    _write_workbook(workbook_path, ml_method=None)

    monkeypatch.setattr(
        ml_driver.dataset_utils,
        "load_compatible_training_samples",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        ml_driver,
        "_build_search_space",
        lambda *args, **kwargs: [_dimension()],
    )

    captured: dict[str, object] = {}

    def fake_active_learning_loop(**kwargs):
        captured["model_type"] = kwargs["model_type"]
        return (
            np.asarray([[0.5]], dtype=float),
            np.asarray([1.0], dtype=float),
            np.asarray([], dtype=object),
            {},
            False,
        )

    monkeypatch.setattr(ml_driver, "active_learning_loop", fake_active_learning_loop)

    exit_code = ml_driver.main(
        [
            "--ifs-root",
            str(tmp_path),
            "--end-year",
            "2030",
            "--output-folder",
            str(output_dir),
            "--initial-model-id",
            "initial-model",
            "--starting-point-table",
            str(workbook_path),
        ]
    )

    assert exit_code == 0
    assert captured["model_type"] == "tree"


@pytest.mark.parametrize("persisted_ml_method", [None, "svm"])
def test_ml_driver_fails_when_db_ml_method_is_missing_or_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    persisted_ml_method: str | None,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    db_path = output_dir / "bigpopa.db"
    workbook_path = tmp_path / "StartingPointTable.xlsx"
    _create_bigpopa_db(db_path, persisted_ml_method=persisted_ml_method)
    _write_workbook(workbook_path, ml_method="neural network")

    called = {"value": False}

    def fake_active_learning_loop(**kwargs):
        called["value"] = True
        raise AssertionError("active_learning_loop should not be called")

    monkeypatch.setattr(ml_driver, "active_learning_loop", fake_active_learning_loop)

    exit_code = ml_driver.main(
        [
            "--ifs-root",
            str(tmp_path),
            "--end-year",
            "2030",
            "--output-folder",
            str(output_dir),
            "--initial-model-id",
            "initial-model",
            "--starting-point-table",
            str(workbook_path),
        ]
    )

    assert exit_code == 1
    assert called["value"] is False


def test_load_required_ml_method_rejects_invalid_value(tmp_path: Path) -> None:
    workbook_path = tmp_path / "StartingPointTable.xlsx"
    _write_workbook(workbook_path, ml_method="svm")

    with pytest.raises(ValueError, match="Unsupported ML method"):
        load_required_ml_method(workbook_path)


def test_model_setup_reads_and_normalizes_workbook_ml_method(tmp_path: Path) -> None:
    workbook_path = tmp_path / "StartingPointTable.xlsx"
    _write_workbook(workbook_path, ml_method="neural network")

    fit_metric, ml_method = model_setup._load_ml_text_settings(workbook_path)

    assert fit_metric == "mse"
    assert ml_method.normalized_value == "neural network"
    assert ml_method.model_type == "nn"


def test_active_learning_requires_explicit_model_type() -> None:
    with pytest.raises(ValueError, match="model_type must be provided explicitly"):
        active_learning.active_learning_loop(
            f=lambda x: 0.0,
            X_obs=np.asarray([[0.0]], dtype=float),
            Y_obs=np.asarray([0.0], dtype=float),
            X_grid=np.asarray([[0.0]], dtype=float),
        )


def test_chunked_candidate_selection_matches_full_scan() -> None:
    class FakeModel:
        def __init__(self, scale: float) -> None:
            self.scale = scale

        def predict(self, X: np.ndarray) -> np.ndarray:
            X = np.asarray(X, dtype=float)
            return X[:, 0] * self.scale

    x_grid = np.asarray([[0.1], [0.4], [0.2], [0.3]], dtype=float)
    cache: dict[tuple[float, ...], float] = {}
    models = [FakeModel(1.0), FakeModel(1.5)]

    chunked = active_learning._select_candidate_index(
        models=models,
        X_grid=x_grid,
        results_cache=cache,
        acquisition="LCB",
        y_best=0.0,
        kappa=1.0,
        chunk_size=2,
    )
    full = active_learning._select_candidate_index(
        models=models,
        X_grid=x_grid,
        results_cache=cache,
        acquisition="LCB",
        y_best=0.0,
        kappa=1.0,
        chunk_size=len(x_grid),
    )

    assert chunked == full


def test_validate_surrogate_memory_rejects_unsafe_polynomial_budget() -> None:
    with pytest.raises(ValueError, match="exceeds the configured memory budget"):
        validate_surrogate_memory(
            n_observations=500,
            n_candidates=10,
            n_dimensions=20,
            model_type="poly",
            memory_budget_bytes=1024,
        )


@pytest.mark.parametrize(
    ("search_space", "expected_shape"),
    [
        (
            [
                ml_driver.SearchDimension(
                    key=("param", "a"),
                    display_name="parameter 'a'",
                    kind="param",
                    default=0.5,
                    minimum=0.0,
                    maximum=1.0,
                ),
            ],
            (10, 1),
        ),
        (
            [
                ml_driver.SearchDimension(
                    key=("param", "a"),
                    display_name="parameter 'a'",
                    kind="param",
                    default=0.0,
                    minimum=0.0,
                    maximum=19.0,
                    level_count=10,
                ),
            ],
            (10, 1),
        ),
        (
            [
                ml_driver.SearchDimension(
                    key=("param", "a"),
                    display_name="parameter 'a'",
                    kind="param",
                    default=0.0,
                    minimum=0.0,
                    maximum=4.0,
                    level_count=5,
                ),
                ml_driver.SearchDimension(
                    key=("param", "b"),
                    display_name="parameter 'b'",
                    kind="param",
                    default=0.5,
                    minimum=0.0,
                    maximum=1.0,
                ),
            ],
            (10, 2),
        ),
    ],
)
def test_candidate_pool_log_reports_actual_shape_and_memory(
    capsys: pytest.CaptureFixture[str],
    search_space: list[ml_driver.SearchDimension],
    expected_shape: tuple[int, int],
) -> None:
    memory_budget_bytes = 512 * 1024 * 1024

    if ml_driver._has_grid_configuration(search_space):
        explicit_dimensions, free_dimensions = ml_driver._split_search_space(search_space)
        if explicit_dimensions and free_dimensions:
            x_grid = ml_driver._generate_hybrid_candidate_grid(
                search_space,
                n_samples=10,
                run_seed=123,
                memory_budget_bytes=memory_budget_bytes,
            )
        else:
            x_grid = ml_driver._generate_candidate_grid(
                search_space,
                n_samples=10,
                memory_budget_bytes=memory_budget_bytes,
            )
    else:
        x_grid = ml_driver._sample_grid(
            [(dimension.minimum, dimension.maximum) for dimension in search_space],
            n_samples=10,
            run_seed=123,
        )

    ml_driver._log_candidate_pool_usage(x_grid, memory_budget_bytes=memory_budget_bytes)

    captured = capsys.readouterr().out.strip()

    assert "Candidate pool generated:" in captured
    assert f"shape=({expected_shape[0]}, {expected_shape[1]})" in captured
    assert f"candidate_pool_rows={expected_shape[0]}" in captured
    assert f"candidate_pool_dimensions={expected_shape[1]}" in captured
    assert f"candidate_pool_mb={x_grid.nbytes / 1024 / 1024:.6f}" in captured
    assert "memory_budget_mb=512.0" in captured
