from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from runtime import dataset_utils
from db import input_profiles
from runtime import ml_driver
from runtime import model_setup
from db.ifs_metadata import ensure_ifs_metadata_schema
from runtime.ml_method import normalize_ml_method
from runtime.model_run_store import insert_model_run
from runtime.model_status import (
    FALLBACK_FIT_POOLED,
    FIT_EVALUATED,
    IFS_RUN_COMPLETED,
    IFS_RUN_FAILED,
    MODEL_REUSED,
)
from optimization import active_learning
from optimization.ensemble_training import ensemble_predict, train_ensemble, validate_surrogate_memory
from optimization.surrogate_models import BoundsScaler, LogClippedTargetTransform
from db.schema import ensure_current_bigpopa_schema


def _create_bigpopa_db(
    path: Path,
    *,
    initial_model_id: str = "initial-model",
    persisted_ml_method: str | None = "tree",
) -> None:
    conn = sqlite3.connect(path)
    try:
        cursor = conn.cursor()
        ensure_current_bigpopa_schema(cursor)
        ensure_ifs_metadata_schema(cursor)
        insert_model_run(
            conn,
            ifs_id=1,
            model_id=initial_model_id,
            dataset_id="dataset-1",
            input_param={"a": 0.5},
            input_coef={},
            output_set={"fit": 1},
            model_status=None,
            resolution_note="model_setup_seed",
        )
        cursor.execute(
            """
            INSERT OR REPLACE INTO ifs_version (
                ifs_id, ifs_static_id, version_number, base_year, end_year, fit_metric, ml_method
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, 1, "8.01", 2020, 2030, "mse", persisted_ml_method),
        )
        cursor.execute(
            """
            INSERT INTO ifs_static (ifs_static_id, version_number, base_year)
            VALUES (?, ?, ?)
            """,
            (1, "8.01", 2020),
        )
        cursor.execute(
            """
            INSERT INTO parameter (
                ifs_static_id, param_name, param_type, param_default, param_min, param_max
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (1, "a", "parameter", 0.5, 0.0, 1.0),
        )
        conn.commit()
    finally:
        conn.close()


def _create_input_profile(
    output_dir: Path,
    *,
    ml_method: str = "tree",
    n_sample: int = 10,
    n_max_iteration: int = 1,
    n_convergence: int = 10,
    min_convergence_pct: float = 0.01 / 100.0,
) -> int:
    created = input_profiles.create_profile(
        output_folder=output_dir,
        ifs_static_id=1,
        name="Test profile",
    )
    profile_id = int(created["profile"]["profile_id"])
    input_profiles.save_parameters(
        output_folder=output_dir,
        profile_id=profile_id,
        rows=[{"param_name": "a", "enabled": True}],
    )
    input_profiles.save_outputs(
        output_folder=output_dir,
        profile_id=profile_id,
        rows=[{"variable": "fit", "table_name": "hist_fit", "enabled": True}],
    )
    input_profiles.save_ml_settings(
        output_folder=output_dir,
        profile_id=profile_id,
        payload={
            "ml_method": ml_method,
            "fit_metric": "mse",
            "n_sample": n_sample,
            "n_max_iteration": n_max_iteration,
            "n_convergence": n_convergence,
            "min_convergence_pct": min_convergence_pct,
        },
    )
    return profile_id


def _ensure_bigpopa_schema(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        model_setup.ensure_bigpopa_schema(conn.cursor())
        conn.commit()


def _latest_model_run_status(conn: sqlite3.Connection, model_id: str) -> tuple[str | None, float | None]:
    row = conn.execute(
        """
        SELECT model_status, fit_pooled
        FROM model_run
        WHERE model_id = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (model_id,),
    ).fetchone()
    if row is None:
        return None, None
    return row[0], row[1]


def _dimension() -> ml_driver.SearchDimension:
    return ml_driver.SearchDimension(
        key=("param", "a"),
        display_name="parameter 'a'",
        kind="param",
        default=0.5,
        minimum=0.0,
        maximum=1.0,
    )


def _args_namespace(tmp_path: Path, output_dir: Path) -> argparse.Namespace:
    return argparse.Namespace(
        ifs_root=str(tmp_path),
        end_year=2030,
        output_folder=str(output_dir),
        base_year=2020,
        start_token="5",
        log="jrs.txt",
        websessionid="session-1",
        artifact_retention=None,
    )


def _canonical_model_id(*, ifs_id: int, param_values: dict, coef_values: dict, output_set: dict) -> str:
    canonical = model_setup.canonical_config(ifs_id, param_values, coef_values, output_set)
    return model_setup.hash_model_id(canonical)


def test_load_compatible_training_samples_uses_exact_dataset_id_only(tmp_path: Path) -> None:
    db_path = tmp_path / "bigpopa.db"
    conn = sqlite3.connect(db_path)
    try:
        ensure_current_bigpopa_schema(conn.cursor())
        insert_model_run(
            conn,
            ifs_id=1,
            model_id="same-dataset",
            dataset_id="dataset-1",
            input_param={"a": 0.1},
            input_coef={},
            output_set={"fit": 1},
            model_status=FIT_EVALUATED,
            fit_pooled=1.5,
            trial_index=1,
            batch_index=1,
            started_at_utc="2026-03-24T00:00:00Z",
            completed_at_utc="2026-03-24T00:01:00Z",
        )
        insert_model_run(
            conn,
            ifs_id=1,
            model_id="other-dataset",
            dataset_id="dataset-2",
            input_param={"a": 0.2},
            input_coef={},
            output_set={"fit": 1},
            model_status=FIT_EVALUATED,
            fit_pooled=2.5,
            trial_index=1,
            batch_index=1,
            started_at_utc="2026-03-24T00:02:00Z",
            completed_at_utc="2026-03-24T00:03:00Z",
        )
        conn.commit()
    finally:
        conn.close()

    samples = dataset_utils.load_compatible_training_samples(str(db_path), (), "dataset-1")

    assert [sample["model_id"] for sample in samples] == ["same-dataset"]


@pytest.mark.parametrize(
    ("persisted_ml_method", "profile_value", "expected_model_type"),
    [
        ("neural network", "tree", "tree"),
        ("poly", "neural network", "nn"),
    ],
)
def test_ml_driver_uses_profile_ml_method_even_when_ifs_version_differs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    persisted_ml_method: str,
    profile_value: str,
    expected_model_type: str,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    db_path = output_dir / "bigpopa.db"
    _create_bigpopa_db(db_path, persisted_ml_method=persisted_ml_method)
    profile_id = _create_input_profile(output_dir, ml_method=profile_value)

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
            "--input-profile-id",
            str(profile_id),
        ]
    )

    assert exit_code == 0
    assert captured["model_type"] == expected_model_type


def test_ml_driver_uses_profile_ml_settings_defaults_when_profile_is_minimal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    db_path = output_dir / "bigpopa.db"
    _create_bigpopa_db(db_path, persisted_ml_method="tree")
    profile_id = _create_input_profile(output_dir, ml_method="tree")

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
            "--input-profile-id",
            str(profile_id),
        ]
    )

    assert exit_code == 0
    assert captured["model_type"] == "tree"


@pytest.mark.parametrize("invalid_profile_ml_method", ["svm"])
def test_ml_driver_fails_when_profile_ml_method_is_missing_or_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid_profile_ml_method: str | None,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    db_path = output_dir / "bigpopa.db"
    _create_bigpopa_db(db_path, persisted_ml_method="tree")
    profile_id = _create_input_profile(output_dir, ml_method="tree")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE input_profile_ml_settings SET ml_method = ? WHERE profile_id = ?",
            (invalid_profile_ml_method, profile_id),
        )
        conn.commit()

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
            "--input-profile-id",
            str(profile_id),
        ]
    )

    assert exit_code == 1
    assert called["value"] is False


def test_active_learning_requires_explicit_model_type() -> None:
    with pytest.raises(ValueError, match="model_type must be provided explicitly"):
        active_learning.active_learning_loop(
            f=lambda x: 0.0,
            X_obs=np.asarray([[0.0]], dtype=float),
            Y_obs=np.asarray([0.0], dtype=float),
            X_grid=np.asarray([[0.0]], dtype=float),
        )


def test_active_learning_uses_iteration_offset_for_candidate_generation_and_state_updates() -> None:
    seen_iterations: list[int] = []
    state_updates: list[dict[str, float]] = []

    class FakeModel:
        def predict(self, X: np.ndarray) -> np.ndarray:
            X = np.asarray(X, dtype=float)
            return np.zeros(len(X), dtype=float)

    original_train_ensemble = active_learning.train_ensemble
    active_learning.train_ensemble = lambda *args, **kwargs: [FakeModel(), FakeModel()]
    try:
        x_obs, y_obs, history, _, _ = active_learning.active_learning_loop(
            f=lambda x: float(np.atleast_1d(x)[0]) + 0.5,
            X_obs=np.asarray([[0.0]], dtype=float),
            Y_obs=np.asarray([2.5], dtype=float),
            X_grid=None,
            n_iters=2,
            model_type="tree",
            iteration_offset=7,
            initial_no_improve_counter=2,
            on_state_update=lambda state: state_updates.append(dict(state)),
            candidate_generator=lambda **kwargs: (
                seen_iterations.append(int(kwargs["iteration"])) or np.asarray(
                    [[float(kwargs["iteration"]) + 1.0]],
                    dtype=float,
                )
            ),
        )
    finally:
        active_learning.train_ensemble = original_train_ensemble

    assert seen_iterations == [7, 8]
    assert state_updates[0]["effective_iteration_count"] == 7
    assert state_updates[0]["no_improve_counter"] == 2
    assert state_updates[-1]["effective_iteration_count"] == 9
    assert history.shape[0] == 2
    assert np.array_equal(x_obs, np.asarray([[0.0], [8.0], [9.0]], dtype=float))
    assert np.array_equal(y_obs, np.asarray([2.5, 8.5, 9.5], dtype=float))


def test_ensure_bigpopa_schema_creates_ml_resume_state_table_idempotently() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        cursor = conn.cursor()
        model_setup.ensure_bigpopa_schema(cursor)
        model_setup.ensure_bigpopa_schema(cursor)
        cursor.execute("PRAGMA table_info(ml_resume_state)")
        columns = {row[1] for row in cursor.fetchall()}
    finally:
        conn.close()

    assert {
        "cohort_key",
        "dataset_id",
        "base_year",
        "end_year",
        "settings_signature",
        "settings_payload",
        "proposal_seed",
        "effective_iteration_count",
        "no_improve_counter",
        "best_y_prev",
        "updated_at_utc",
    }.issubset(columns)


def test_ensure_bigpopa_schema_upgrades_existing_db_with_ml_resume_state_table(tmp_path: Path) -> None:
    db_path = tmp_path / "bigpopa.db"
    _create_bigpopa_db(db_path)

    with sqlite3.connect(db_path) as conn:
        before = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'ml_resume_state'"
        ).fetchone()
    assert before == ("ml_resume_state",)

    _ensure_bigpopa_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        after = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'ml_resume_state'"
        ).fetchone()
    assert after == ("ml_resume_state",)


def test_ml_driver_resumes_search_state_when_settings_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    db_path = output_dir / "bigpopa.db"
    _create_bigpopa_db(db_path, persisted_ml_method="tree")
    profile_id = _create_input_profile(
        output_dir,
        ml_method="tree",
        n_sample=10,
        n_max_iteration=1,
    )
    _ensure_bigpopa_schema(db_path)

    search_space = [_dimension()]
    settings_signature, settings_payload = ml_driver._build_resume_settings_payload(
        ml_method_config=normalize_ml_method("tree"),
        n_sample=10,
        n_convergence=10,
        min_convergence_pct=0.01 / 100.0,
        proposal_mode=ml_driver.DEFAULT_PROPOSAL_MODE,
        explicit_random_seed=None,
        search_space=search_space,
    )
    with sqlite3.connect(db_path) as conn:
        ml_driver._persist_resume_state(
            conn,
            cohort_key=ml_driver._build_resume_cohort_key(
                dataset_id="dataset-1",
                base_year=2020,
                end_year=2030,
            ),
            dataset_id="dataset-1",
            base_year=2020,
            end_year=2030,
            settings_signature=settings_signature,
            settings_payload=settings_payload,
            proposal_seed=4242,
            effective_iteration_count=4,
            no_improve_counter=3,
            best_y_prev=1.5,
        )

    monkeypatch.setattr(
        ml_driver.dataset_utils,
        "load_compatible_training_samples",
        lambda *args, **kwargs: [
            {
                "model_id": "prior-model",
                "input_param": {"a": 0.25},
                "input_coef": {},
                "output_set": {"fit": 1},
                "fit_pooled": 1.5,
            }
        ],
    )
    monkeypatch.setattr(ml_driver, "_build_search_space", lambda *args, **kwargs: search_space)

    captured: dict[str, object] = {}

    def fake_active_learning_loop(**kwargs):
        captured["iteration_offset"] = kwargs["iteration_offset"]
        captured["initial_no_improve_counter"] = kwargs["initial_no_improve_counter"]
        captured["x_obs"] = np.asarray(kwargs["X_obs"], dtype=float).copy()
        kwargs["on_state_update"](
            {
                "effective_iteration_count": 5,
                "no_improve_counter": 4,
                "best_y_prev": 1.5,
            }
        )
        return (
            np.asarray([[0.25]], dtype=float),
            np.asarray([1.5], dtype=float),
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
            "--input-profile-id",
            str(profile_id),
        ]
    )

    assert exit_code == 0
    assert captured["iteration_offset"] == 4
    assert captured["initial_no_improve_counter"] == 0
    assert np.array_equal(captured["x_obs"], np.asarray([[0.25]], dtype=float))

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT proposal_seed, effective_iteration_count, no_improve_counter FROM ml_resume_state"
        ).fetchone()

    assert row == (4242, 5, 4)


def test_ml_driver_resets_search_state_but_keeps_training_data_when_settings_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    db_path = output_dir / "bigpopa.db"
    _create_bigpopa_db(db_path, persisted_ml_method="tree")
    profile_id = _create_input_profile(
        output_dir,
        ml_method="tree",
        n_sample=25,
        n_max_iteration=1,
    )
    _ensure_bigpopa_schema(db_path)

    search_space = [_dimension()]
    old_signature, old_payload = ml_driver._build_resume_settings_payload(
        ml_method_config=normalize_ml_method("tree"),
        n_sample=10,
        n_convergence=10,
        min_convergence_pct=0.01 / 100.0,
        proposal_mode=ml_driver.DEFAULT_PROPOSAL_MODE,
        explicit_random_seed=None,
        search_space=search_space,
    )
    with sqlite3.connect(db_path) as conn:
        ml_driver._persist_resume_state(
            conn,
            cohort_key=ml_driver._build_resume_cohort_key(
                dataset_id="dataset-1",
                base_year=2020,
                end_year=2030,
            ),
            dataset_id="dataset-1",
            base_year=2020,
            end_year=2030,
            settings_signature=old_signature,
            settings_payload=old_payload,
            proposal_seed=4242,
            effective_iteration_count=7,
            no_improve_counter=5,
            best_y_prev=1.25,
        )

    monkeypatch.setattr(
        ml_driver.dataset_utils,
        "load_compatible_training_samples",
        lambda *args, **kwargs: [
            {
                "model_id": "prior-model",
                "input_param": {"a": 0.75},
                "input_coef": {},
                "output_set": {"fit": 1},
                "fit_pooled": 1.25,
            }
        ],
    )
    monkeypatch.setattr(ml_driver, "_build_search_space", lambda *args, **kwargs: search_space)

    captured: dict[str, object] = {}

    def fake_active_learning_loop(**kwargs):
        captured["iteration_offset"] = kwargs["iteration_offset"]
        captured["initial_no_improve_counter"] = kwargs["initial_no_improve_counter"]
        captured["x_obs"] = np.asarray(kwargs["X_obs"], dtype=float).copy()
        kwargs["on_state_update"](
            {
                "effective_iteration_count": 1,
                "no_improve_counter": 0,
                "best_y_prev": 1.25,
            }
        )
        return (
            np.asarray([[0.75]], dtype=float),
            np.asarray([1.25], dtype=float),
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
            "--input-profile-id",
            str(profile_id),
        ]
    )

    assert exit_code == 0
    assert captured["iteration_offset"] == 7
    assert captured["initial_no_improve_counter"] == 0
    assert np.array_equal(captured["x_obs"], np.asarray([[0.75]], dtype=float))

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT proposal_seed, effective_iteration_count, no_improve_counter FROM ml_resume_state"
        ).fetchone()

    assert row == (4242, 1, 0)


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


def test_bounds_scaler_maps_configured_ranges_to_unit_interval() -> None:
    scaler = BoundsScaler(
        lower=np.asarray([0.0, 10.0], dtype=float),
        upper=np.asarray([20.0, 30.0], dtype=float),
    )

    transformed = scaler.transform(np.asarray([[0.0, 10.0], [10.0, 20.0], [20.0, 30.0]]))

    assert np.allclose(
        transformed,
        np.asarray([[-1.0, -1.0], [0.0, 0.0], [1.0, 1.0]], dtype=float),
    )


def test_log_clipped_target_transform_limits_fail_penalty_in_training_space() -> None:
    transformer = LogClippedTargetTransform(upper_quantile=95.0, absolute_cap=1e6).fit(
        np.asarray(list(range(1, 21)) + [1e6], dtype=float)
    )

    transformed = transformer.transform(np.asarray([5.0, 1e6], dtype=float))

    assert transformer.upper_clip_ is not None
    assert transformer.upper_clip_ < 1e6
    assert transformed[1] == pytest.approx(np.log1p(transformer.upper_clip_))
    assert transformed[1] > transformed[0]


def test_bootstrap_ensemble_adds_uncertainty_for_tree_surrogate() -> None:
    x_obs = np.asarray([[0.0], [1.0], [2.0], [3.0]], dtype=float)
    y_obs = np.asarray([0.0, 1.0, 4.0, 9.0], dtype=float)
    x_grid = np.asarray([[1.5], [2.5]], dtype=float)

    deterministic_models = train_ensemble(
        x_obs,
        y_obs,
        M=4,
        bootstrap=False,
        model_type="tree",
    )
    bootstrapped_models = train_ensemble(
        x_obs,
        y_obs,
        M=4,
        bootstrap=True,
        model_type="tree",
    )

    _, deterministic_sigma = ensemble_predict(deterministic_models, x_grid)
    _, bootstrapped_sigma = ensemble_predict(bootstrapped_models, x_grid)

    assert np.allclose(deterministic_sigma, 0.0)
    assert np.any(bootstrapped_sigma > 0.0)


def test_default_distance_penalty_changes_candidate_ranking_only() -> None:
    class FakeModel:
        def predict(self, X: np.ndarray) -> np.ndarray:
            X = np.asarray(X, dtype=float)
            return np.zeros(len(X), dtype=float)

    search_space = [
        ml_driver.SearchDimension(
            key=("param", "a"),
            display_name="parameter 'a'",
            kind="param",
            default=1.0,
            minimum=0.0,
            maximum=1.0,
        ),
    ]
    scaler = ml_driver._build_bounds_scaler(search_space)
    penalty = ml_driver._build_default_distance_penalty(
        search_space=search_space,
        x_scaler=scaler,
        strength=1.0,
    )
    x_grid = np.asarray([[0.0], [1.0]], dtype=float)
    cache: dict[tuple[float, ...], float] = {}
    models = [FakeModel(), FakeModel()]

    without_penalty = active_learning._select_candidate_index(
        models=models,
        X_grid=x_grid,
        results_cache=cache,
        acquisition="LCB",
        y_best=0.0,
        kappa=1.0,
        chunk_size=2,
    )
    with_penalty = active_learning._select_candidate_index(
        models=models,
        X_grid=x_grid,
        results_cache=cache,
        acquisition="LCB",
        y_best=0.0,
        kappa=1.0,
        chunk_size=2,
        proposal_penalty_fn=penalty,
    )

    assert without_penalty == 0
    assert with_penalty == 1


def test_proposal_penalty_does_not_change_training_targets() -> None:
    captured: dict[str, np.ndarray] = {}

    def fake_train_ensemble(
        X_obs,
        Y_obs,
        M=8,
        degree=5,
        bootstrap=True,
        model_type=None,
        nn_config=None,
        x_scaler=None,
        y_transformer=None,
    ):
        captured["X_obs"] = np.asarray(X_obs, dtype=float).copy()
        captured["Y_obs"] = np.asarray(Y_obs, dtype=float).copy()

        class FakeModel:
            def predict(self, X: np.ndarray) -> np.ndarray:
                X = np.asarray(X, dtype=float)
                return np.zeros(len(X), dtype=float)

        return [FakeModel(), FakeModel()]

    original_train_ensemble = active_learning.train_ensemble
    active_learning.train_ensemble = fake_train_ensemble
    try:
        x_obs, y_obs, history, _, _ = active_learning.active_learning_loop(
            f=lambda x: 3.0,
            X_obs=np.asarray([[0.0]], dtype=float),
            Y_obs=np.asarray([2.5], dtype=float),
            X_grid=np.asarray([[1.0]], dtype=float),
            n_iters=1,
            model_type="tree",
            proposal_penalty_fn=lambda X: np.asarray([5.0] * len(np.asarray(X))),
        )
    finally:
        active_learning.train_ensemble = original_train_ensemble

    assert np.array_equal(captured["X_obs"], np.asarray([[0.0]], dtype=float))
    assert np.array_equal(captured["Y_obs"], np.asarray([2.5], dtype=float))
    assert np.array_equal(x_obs, np.asarray([[0.0], [1.0]], dtype=float))
    assert np.array_equal(y_obs, np.asarray([2.5, 3.0], dtype=float))
    assert history.shape[0] == 1


def test_active_learning_regenerates_exhausted_cached_pool_and_continues() -> None:
    refresh_attempts: list[int] = []
    evaluated: list[float] = []

    class FakeModel:
        def predict(self, X: np.ndarray) -> np.ndarray:
            X = np.asarray(X, dtype=float)
            return np.zeros(len(X), dtype=float)

    original_train_ensemble = active_learning.train_ensemble
    active_learning.train_ensemble = lambda *args, **kwargs: [FakeModel(), FakeModel()]
    try:
        x_obs, y_obs, history, _, _ = active_learning.active_learning_loop(
            f=lambda x: evaluated.append(float(np.atleast_1d(x)[0])) or 3.0,
            X_obs=np.asarray([[0.0]], dtype=float),
            Y_obs=np.asarray([2.5], dtype=float),
            X_grid=None,
            n_iters=1,
            model_type="tree",
            candidate_generator=lambda **kwargs: (
                refresh_attempts.append(int(kwargs["refresh_attempt"])) or np.asarray(
                    [[0.0]] if kwargs["refresh_attempt"] == 0 else [[1.0]],
                    dtype=float,
                )
            ),
        )
    finally:
        active_learning.train_ensemble = original_train_ensemble

    assert refresh_attempts == [0, 1]
    assert evaluated == [1.0]
    assert np.array_equal(x_obs, np.asarray([[0.0], [1.0]], dtype=float))
    assert np.array_equal(y_obs, np.asarray([2.5, 3.0], dtype=float))
    assert history.shape[0] == 1


def test_active_learning_stops_after_configured_pool_regeneration_limit() -> None:
    refresh_attempts: list[int] = []

    class FakeModel:
        def predict(self, X: np.ndarray) -> np.ndarray:
            X = np.asarray(X, dtype=float)
            return np.zeros(len(X), dtype=float)

    original_train_ensemble = active_learning.train_ensemble
    active_learning.train_ensemble = lambda *args, **kwargs: [FakeModel(), FakeModel()]
    try:
        x_obs, y_obs, history, _, _ = active_learning.active_learning_loop(
            f=lambda x: pytest.fail("No new candidate should be evaluated"),
            X_obs=np.asarray([[0.0]], dtype=float),
            Y_obs=np.asarray([2.5], dtype=float),
            X_grid=None,
            n_iters=1,
            model_type="tree",
            max_pool_regenerations=2,
            candidate_generator=lambda **kwargs: (
                refresh_attempts.append(int(kwargs["refresh_attempt"])) or np.asarray([[0.0]], dtype=float)
            ),
        )
    finally:
        active_learning.train_ensemble = original_train_ensemble

    assert refresh_attempts == [0, 1, 2]
    assert np.array_equal(x_obs, np.asarray([[0.0]], dtype=float))
    assert np.array_equal(y_obs, np.asarray([2.5], dtype=float))
    assert history.size == 0


def test_active_learning_uses_later_regenerated_pool_with_fresh_point() -> None:
    refresh_attempts: list[int] = []
    evaluated: list[float] = []

    class FakeModel:
        def predict(self, X: np.ndarray) -> np.ndarray:
            X = np.asarray(X, dtype=float)
            return np.zeros(len(X), dtype=float)

    original_train_ensemble = active_learning.train_ensemble
    active_learning.train_ensemble = lambda *args, **kwargs: [FakeModel(), FakeModel()]
    try:
        x_obs, y_obs, history, _, _ = active_learning.active_learning_loop(
            f=lambda x: evaluated.append(float(np.atleast_1d(x)[0])) or 4.0,
            X_obs=np.asarray([[0.0]], dtype=float),
            Y_obs=np.asarray([2.5], dtype=float),
            X_grid=None,
            n_iters=1,
            model_type="tree",
            max_pool_regenerations=3,
            candidate_generator=lambda **kwargs: (
                refresh_attempts.append(int(kwargs["refresh_attempt"])) or np.asarray(
                    [[0.0]] if kwargs["refresh_attempt"] < 2 else [[2.0]],
                    dtype=float,
                )
            ),
        )
    finally:
        active_learning.train_ensemble = original_train_ensemble

    assert refresh_attempts == [0, 1, 2]
    assert evaluated == [2.0]
    assert np.array_equal(x_obs, np.asarray([[0.0], [2.0]], dtype=float))
    assert np.array_equal(y_obs, np.asarray([2.5, 4.0], dtype=float))
    assert history.shape[0] == 1


def test_run_model_marks_cached_result_as_reused_without_launching_ifs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    db_path = output_dir / "bigpopa.db"
    param_values = {"a": 0.5}
    coef_values: dict[str, dict[str, dict[str, float]]] = {}
    output_set = {"fit": 1}
    model_id = _canonical_model_id(
        ifs_id=1,
        param_values=param_values,
        coef_values=coef_values,
        output_set=output_set,
    )

    _create_bigpopa_db(db_path, initial_model_id=model_id)
    with sqlite3.connect(db_path) as conn:
        ml_driver._upsert_model_output_tracking(
            conn,
            ifs_id=1,
            model_id=model_id,
            trial_index=99,
            batch_index=1,
            model_status=FIT_EVALUATED,
            fit_pooled=12.5,
        )

    monkeypatch.setattr(
        ml_driver.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("run_ifs.py should not be launched for cached models"),
    )

    fit_val, returned_model_id = ml_driver._run_model(
        args=_args_namespace(tmp_path, output_dir),
        param_values=param_values,
        coef_values=coef_values,
        output_set=output_set,
        ifs_id=1,
        dataset_id="dataset-1",
        bigpopa_db=db_path,
        dataset_id_supported=True,
        trial_index=1,
        batch_index=1,
    )

    with sqlite3.connect(db_path) as conn:
        row = _latest_model_run_status(conn, model_id)
        reused_rows = conn.execute(
            """
            SELECT model_status, was_reused, fit_pooled
            FROM model_run
            WHERE model_id = ?
            ORDER BY run_id
            """,
            (model_id,),
        ).fetchall()

    assert fit_val == 12.5
    assert returned_model_id == model_id
    assert row == (MODEL_REUSED, 12.5)
    assert reused_rows == [
        (FIT_EVALUATED, 0, 12.5),
        (MODEL_REUSED, 1, 12.5),
    ]


def test_run_model_persists_fallback_fit_when_ifs_run_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    db_path = output_dir / "bigpopa.db"
    _create_bigpopa_db(db_path)

    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], returncode=1)

    monkeypatch.setattr(ml_driver.subprocess, "run", fake_run)

    fit_val, model_id = ml_driver._run_model(
        args=_args_namespace(tmp_path, output_dir),
        param_values={"a": 0.5},
        coef_values={},
        output_set={"fit": 1},
        ifs_id=1,
        dataset_id="dataset-1",
        bigpopa_db=db_path,
        dataset_id_supported=True,
        trial_index=1,
        batch_index=1,
    )

    with sqlite3.connect(db_path) as conn:
        row = _latest_model_run_status(conn, model_id)

    assert fit_val == FALLBACK_FIT_POOLED
    assert row == (IFS_RUN_FAILED, FALLBACK_FIT_POOLED)


def test_run_model_preserves_real_fit_after_successful_fit_evaluation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    db_path = output_dir / "bigpopa.db"
    _create_bigpopa_db(db_path)

    def fake_run(command, **kwargs):
        model_id = command[command.index("--model-id") + 1]
        with sqlite3.connect(db_path) as conn:
            ml_driver._upsert_model_output_tracking(
                conn,
                ifs_id=1,
                model_id=model_id,
                trial_index=1,
                batch_index=1,
                model_status=FIT_EVALUATED,
                fit_pooled=7.25,
            )
        return subprocess.CompletedProcess(command, returncode=0)

    monkeypatch.setattr(ml_driver.subprocess, "run", fake_run)

    fit_val, model_id = ml_driver._run_model(
        args=_args_namespace(tmp_path, output_dir),
        param_values={"a": 0.5},
        coef_values={},
        output_set={"fit": 1},
        ifs_id=1,
        dataset_id="dataset-1",
        bigpopa_db=db_path,
        dataset_id_supported=True,
        trial_index=1,
        batch_index=1,
    )

    with sqlite3.connect(db_path) as conn:
        row = _latest_model_run_status(conn, model_id)

    assert fit_val == 7.25
    assert row == (FIT_EVALUATED, 7.25)


def test_run_model_returns_fallback_fit_when_compare_completes_without_pooled_metric(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    db_path = output_dir / "bigpopa.db"
    _create_bigpopa_db(db_path)

    def fake_run(command, **kwargs):
        model_id = command[command.index("--model-id") + 1]
        with sqlite3.connect(db_path) as conn:
            ml_driver._upsert_model_output_tracking(
                conn,
                ifs_id=1,
                model_id=model_id,
                trial_index=1,
                batch_index=1,
                model_status=IFS_RUN_COMPLETED,
                fit_pooled=FALLBACK_FIT_POOLED,
            )
        return subprocess.CompletedProcess(command, returncode=0)

    monkeypatch.setattr(ml_driver.subprocess, "run", fake_run)

    fit_val, model_id = ml_driver._run_model(
        args=_args_namespace(tmp_path, output_dir),
        param_values={"a": 0.5},
        coef_values={},
        output_set={"fit": 1},
        ifs_id=1,
        dataset_id="dataset-1",
        bigpopa_db=db_path,
        dataset_id_supported=True,
        trial_index=1,
        batch_index=1,
    )

    with sqlite3.connect(db_path) as conn:
        row = _latest_model_run_status(conn, model_id)

    assert fit_val == FALLBACK_FIT_POOLED
    assert row == (IFS_RUN_COMPLETED, FALLBACK_FIT_POOLED)


def test_run_model_keeps_ifs_completed_status_when_fit_evaluation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    db_path = output_dir / "bigpopa.db"
    _create_bigpopa_db(db_path)

    def fake_run(command, **kwargs):
        model_id = command[command.index("--model-id") + 1]
        with sqlite3.connect(db_path) as conn:
            ml_driver._upsert_model_output_tracking(
                conn,
                ifs_id=1,
                model_id=model_id,
                trial_index=1,
                batch_index=1,
                model_status=IFS_RUN_COMPLETED,
                fit_pooled=FALLBACK_FIT_POOLED,
            )
        return subprocess.CompletedProcess(command, returncode=1)

    monkeypatch.setattr(ml_driver.subprocess, "run", fake_run)

    fit_val, model_id = ml_driver._run_model(
        args=_args_namespace(tmp_path, output_dir),
        param_values={"a": 0.5},
        coef_values={},
        output_set={"fit": 1},
        ifs_id=1,
        dataset_id="dataset-1",
        bigpopa_db=db_path,
        dataset_id_supported=True,
        trial_index=1,
        batch_index=1,
    )

    with sqlite3.connect(db_path) as conn:
        row = _latest_model_run_status(conn, model_id)

    assert fit_val == FALLBACK_FIT_POOLED
    assert row == (IFS_RUN_COMPLETED, FALLBACK_FIT_POOLED)
