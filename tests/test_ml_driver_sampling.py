from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from runtime import ml_driver
from runtime.model_run_store import insert_model_run
from db.schema import ensure_current_bigpopa_schema


def _dimension(
    name: str,
    *,
    minimum: float,
    maximum: float,
    default: float,
    step: float | None = None,
    level_count: int | None = None,
) -> ml_driver.SearchDimension:
    return ml_driver.SearchDimension(
        key=("param", name),
        display_name=f"parameter '{name}'",
        kind="param",
        default=default,
        minimum=minimum,
        maximum=maximum,
        step=step,
        level_count=level_count,
    )


def test_sample_grid_legacy_returns_requested_count_and_bounds() -> None:
    samples = ml_driver._sample_grid([(0.0, 1.0), (10.0, 11.0)], n_samples=7)

    assert samples.shape == (7, 2)
    assert np.all(samples[:, 0] >= 0.0)
    assert np.all(samples[:, 0] <= 1.0)
    assert np.all(samples[:, 1] >= 10.0)
    assert np.all(samples[:, 1] <= 11.0)


def test_sample_grid_uses_explicit_seed_for_reproducibility() -> None:
    first = ml_driver._sample_grid([(0.0, 1.0)], n_samples=5, run_seed=123)
    second = ml_driver._sample_grid([(0.0, 1.0)], n_samples=5, run_seed=123)

    assert np.array_equal(first, second)


def test_sample_grid_varies_across_runs_without_explicit_seed() -> None:
    first = ml_driver._sample_grid([(0.0, 1.0)], n_samples=5)
    second = ml_driver._sample_grid([(0.0, 1.0)], n_samples=5)

    assert not np.array_equal(first, second)


def test_generate_candidate_grid_builds_explicit_cartesian_product() -> None:
    search_space = [
        _dimension("a", minimum=0.0, maximum=19.0, default=0.0, level_count=20),
        _dimension("b", minimum=100.0, maximum=104.0, default=100.0, level_count=5),
    ]

    grid = ml_driver._generate_candidate_grid(search_space, n_samples=100)

    assert grid.shape == (100, 2)
    assert np.array_equal(grid[0], np.array([0.0, 100.0]))
    assert np.array_equal(grid[4], np.array([0.0, 104.0]))
    assert np.array_equal(grid[5], np.array([1.0, 100.0]))
    assert np.array_equal(grid[-1], np.array([19.0, 104.0]))


def test_generate_hybrid_candidate_grid_respects_explicit_levels_and_varies_free_dims() -> None:
    search_space = [
        _dimension("a", minimum=0.0, maximum=29.0, default=0.0, step=1.0),
        _dimension("b", minimum=0.0, maximum=1.0, default=0.5),
        _dimension("c", minimum=-10.0, maximum=10.0, default=0.0),
    ]

    grid = ml_driver._generate_hybrid_candidate_grid(search_space, n_samples=120)
    unique_levels, level_counts = np.unique(grid[:, 0], return_counts=True)

    assert grid.shape == (120, 3)
    assert np.array_equal(unique_levels, np.arange(30, dtype=float))
    assert np.all(level_counts == 4)
    assert len(np.unique(grid[:, 1])) > 30
    assert len(np.unique(grid[:, 2])) > 30


def test_generate_hybrid_candidate_grid_balances_non_divisible_allocations() -> None:
    search_space = [
        _dimension("a", minimum=0.0, maximum=2.0, default=1.0, level_count=3),
        _dimension("b", minimum=10.0, maximum=11.0, default=10.0, level_count=2),
        _dimension("c", minimum=-1.0, maximum=1.0, default=0.0),
    ]

    grid = ml_driver._generate_hybrid_candidate_grid(search_space, n_samples=20)
    combo_counts = []
    combo_order = []
    start = 0
    while start < len(grid):
        combo = tuple(grid[start, :2])
        count = 1
        while start + count < len(grid) and tuple(grid[start + count, :2]) == combo:
            count += 1
        combo_order.append(combo)
        combo_counts.append(count)
        start += count

    assert grid.shape == (20, 3)
    assert combo_order == [
        (0.0, 10.0),
        (0.0, 11.0),
        (1.0, 10.0),
        (1.0, 11.0),
        (2.0, 10.0),
        (2.0, 11.0),
    ]
    assert combo_counts == [4, 4, 3, 3, 3, 3]


def test_step_wins_and_uses_strict_stepping() -> None:
    dimension = _dimension(
        "a",
        minimum=0.0,
        maximum=1.0,
        default=0.5,
        step=0.4,
        level_count=10,
    )

    values = ml_driver._explicit_level_values(dimension)

    assert np.array_equal(values, np.array([0.0, 0.4, 0.8]))


def test_single_level_uses_clipped_default_value() -> None:
    dimension = _dimension("a", minimum=0.0, maximum=10.0, default=99.0, level_count=1)

    values = ml_driver._explicit_level_values(dimension)

    assert np.array_equal(values, np.array([10.0]))


def test_invalid_step_and_level_count_raise_clear_errors() -> None:
    with pytest.raises(ValueError, match="must be greater than 0"):
        ml_driver._generate_levels_for_step(
            _dimension("a", minimum=0.0, maximum=1.0, default=0.5),
            0.0,
        )

    with pytest.raises(ValueError, match="must be an integer"):
        ml_driver._parse_config_int("2.5", field_name="LevelCount", label="parameter 'a'")


def test_explicit_grid_cannot_exceed_target_sample_count() -> None:
    search_space = [
        _dimension("a", minimum=0.0, maximum=19.0, default=0.0, level_count=20),
        _dimension("b", minimum=0.0, maximum=4.0, default=0.0, level_count=5),
    ]

    with pytest.raises(ValueError, match="exceeds n_sample=99"):
        ml_driver._generate_candidate_grid(search_space, n_samples=99)


def test_hybrid_grid_cannot_exceed_target_sample_count() -> None:
    search_space = [
        _dimension("a", minimum=0.0, maximum=19.0, default=0.0, level_count=20),
        _dimension("b", minimum=0.0, maximum=4.0, default=0.0, level_count=5),
        _dimension("c", minimum=-1.0, maximum=1.0, default=0.0),
    ]

    with pytest.raises(ValueError, match="exceeds n_sample=99"):
        ml_driver._generate_hybrid_candidate_grid(search_space, n_samples=99)


def test_normalize_batch_indexes_is_a_noop_under_unified_model_run_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "bigpopa.db"

    conn_a = sqlite3.connect(db_path)
    try:
        ensure_current_bigpopa_schema(conn_a.cursor())
        insert_model_run(
            conn_a,
            ifs_id=2,
            model_id="existing-model",
            dataset_id="dataset-a",
            input_param={"a": 0.25},
            input_coef={},
            output_set={"fit": 1},
            model_status="completed",
            fit_pooled=0.25,
            trial_index=1,
            batch_index=1,
            started_at_utc="2026-03-13T00:00:00+00:00",
            completed_at_utc="2026-03-13T00:01:00+00:00",
        )
        conn_a.commit()

        repaired_rows = ml_driver._normalize_model_output_batch_indexes(conn_a)
        assert repaired_rows == 0

        conn_b = sqlite3.connect(db_path, timeout=0)
        try:
            insert_model_run(
                conn_b,
                ifs_id=2,
                model_id="new-model",
                dataset_id="dataset-a",
                input_param={"a": 0.5},
                input_coef={},
                output_set={"fit": 1},
                trial_index=2,
                batch_index=1,
                started_at_utc="2026-03-13T00:00:00+00:00",
                model_status="running",
            )
            conn_b.commit()
        finally:
            conn_b.close()

        rows = conn_a.execute(
            "SELECT model_id, batch_index FROM model_run ORDER BY run_id"
        ).fetchall()
    finally:
        conn_a.close()

    assert rows == [("existing-model", 1), ("new-model", 1)]


def test_candidate_generator_refreshes_continuous_values_and_preserves_discrete_levels() -> None:
    search_space = [
        _dimension("a", minimum=1.0, maximum=3.0, default=2.0, step=1.0),
        _dimension("b", minimum=-1.0, maximum=1.0, default=0.0, level_count=2),
        _dimension("c", minimum=10.0, maximum=20.0, default=15.0),
    ]
    generator = ml_driver._build_candidate_generator(
        search_space=search_space,
        n_samples=12,
        run_seed=123,
        memory_budget_bytes=512 * 1024 * 1024,
    )

    x_obs = np.asarray(
        [
            [1.0, -1.0, 11.0],
            [2.0, 1.0, 15.0],
            [3.0, -1.0, 19.0],
        ],
        dtype=float,
    )
    y_obs = np.asarray([3.0, 1.0, 2.0], dtype=float)

    first = generator(X_obs=x_obs, Y_obs=y_obs, iteration=0)
    second = generator(X_obs=x_obs, Y_obs=y_obs, iteration=1)

    assert first.shape == (12, 3)
    assert second.shape == (12, 3)
    assert not np.array_equal(first, second)

    expected_discrete = {
        (1.0, -1.0),
        (1.0, 1.0),
        (2.0, -1.0),
        (2.0, 1.0),
        (3.0, -1.0),
        (3.0, 1.0),
    }
    assert {tuple(row[:2]) for row in first} == expected_discrete
    assert {tuple(row[:2]) for row in second} == expected_discrete
    assert np.all(first[:, 2] >= 10.0)
    assert np.all(first[:, 2] <= 20.0)
    assert np.all(second[:, 2] >= 10.0)
    assert np.all(second[:, 2] <= 20.0)


def test_candidate_generator_refresh_attempt_produces_new_deterministic_pool() -> None:
    search_space = [
        _dimension("a", minimum=1.0, maximum=3.0, default=2.0, step=1.0),
        _dimension("b", minimum=-1.0, maximum=1.0, default=0.0, level_count=2),
        _dimension("c", minimum=10.0, maximum=20.0, default=15.0),
    ]
    generator = ml_driver._build_candidate_generator(
        search_space=search_space,
        n_samples=12,
        run_seed=123,
        memory_budget_bytes=512 * 1024 * 1024,
    )

    x_obs = np.asarray(
        [
            [1.0, -1.0, 11.0],
            [2.0, 1.0, 15.0],
            [3.0, -1.0, 19.0],
        ],
        dtype=float,
    )
    y_obs = np.asarray([3.0, 1.0, 2.0], dtype=float)

    first_attempt = generator(X_obs=x_obs, Y_obs=y_obs, iteration=4, refresh_attempt=0)
    repeated_first_attempt = generator(X_obs=x_obs, Y_obs=y_obs, iteration=4, refresh_attempt=0)
    refreshed_attempt = generator(X_obs=x_obs, Y_obs=y_obs, iteration=4, refresh_attempt=1)
    repeated_refreshed_attempt = generator(X_obs=x_obs, Y_obs=y_obs, iteration=4, refresh_attempt=1)

    assert np.array_equal(first_attempt, repeated_first_attempt)
    assert np.array_equal(refreshed_attempt, repeated_refreshed_attempt)
    assert not np.array_equal(first_attempt, refreshed_attempt)


def test_candidate_generator_samples_balanced_subset_when_discrete_space_is_large() -> None:
    search_space = [
        _dimension("a", minimum=0.0, maximum=9.0, default=0.0, level_count=10),
        _dimension("b", minimum=0.0, maximum=9.0, default=0.0, level_count=10),
        _dimension("c", minimum=0.0, maximum=1.0, default=0.5),
    ]
    generator = ml_driver._build_candidate_generator(
        search_space=search_space,
        n_samples=20,
        run_seed=321,
        memory_budget_bytes=512 * 1024 * 1024,
    )

    rows = generator(
        X_obs=np.asarray([[0.0, 0.0, 0.5]], dtype=float),
        Y_obs=np.asarray([1.0], dtype=float),
        iteration=0,
        refresh_attempt=0,
    )

    assert rows.shape == (20, 3)
    assert len({tuple(row[:2]) for row in rows}) == 5
    assert set(rows[:, 0]).issubset(set(np.arange(10, dtype=float)))
    assert set(rows[:, 1]).issubset(set(np.arange(10, dtype=float)))
    assert np.all(rows[:, 2] >= 0.0)
    assert np.all(rows[:, 2] <= 1.0)


def test_adaptive_local_radius_shrinks_refreshed_sampling_over_iterations() -> None:
    search_space = [
        _dimension("focus", minimum=0.0, maximum=100.0, default=50.0),
    ]
    generator = ml_driver._build_candidate_generator(
        search_space=search_space,
        n_samples=40,
        run_seed=777,
        memory_budget_bytes=512 * 1024 * 1024,
    )

    x_obs = np.asarray([[50.0], [40.0], [60.0]], dtype=float)
    y_obs = np.asarray([0.1, 1.0, 2.0], dtype=float)

    early = generator(X_obs=x_obs, Y_obs=y_obs, iteration=0, refresh_attempt=0)
    late = generator(X_obs=x_obs, Y_obs=y_obs, iteration=20, refresh_attempt=0)

    early_mean_abs_distance = float(np.mean(np.abs(early[:, 0] - 50.0)))
    late_mean_abs_distance = float(np.mean(np.abs(late[:, 0] - 50.0)))

    assert early.shape == (40, 1)
    assert late.shape == (40, 1)
    assert late_mean_abs_distance < early_mean_abs_distance


def test_build_proposal_generator_direct_mode_raises_clear_placeholder_error() -> None:
    generator = ml_driver._build_proposal_generator(
        search_space=[_dimension("a", minimum=0.0, maximum=1.0, default=0.5)],
        n_samples=8,
        run_seed=123,
        memory_budget_bytes=512 * 1024 * 1024,
        proposal_mode=ml_driver.PROPOSAL_MODE_DIRECT,
    )

    with pytest.raises(NotImplementedError, match="Direct acquisition proposal mode"):
        generator(
            X_obs=np.asarray([[0.5]], dtype=float),
            Y_obs=np.asarray([1.0], dtype=float),
            iteration=0,
            refresh_attempt=0,
        )


def test_default_distance_penalty_uses_scaled_distance_from_defaults() -> None:
    search_space = [
        _dimension("wide", minimum=0.0, maximum=100.0, default=50.0),
        _dimension("narrow", minimum=0.0, maximum=1.0, default=0.5),
    ]
    scaler = ml_driver._build_bounds_scaler(search_space)
    penalty = ml_driver._build_default_distance_penalty(
        search_space=search_space,
        x_scaler=scaler,
        strength=1.0,
    )

    candidates = np.asarray(
        [
            [60.0, 0.5],
            [50.0, 0.6],
            [50.0, 0.5],
        ],
        dtype=float,
    )

    penalties = penalty(candidates)

    assert penalties.shape == (3,)
    assert penalties[0] == pytest.approx(penalties[1])
    assert penalties[2] == pytest.approx(0.0)


def test_default_distance_penalty_is_enabled_by_default() -> None:
    assert ml_driver.ENABLE_DEFAULT_DISTANCE_PENALTY is True
