from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import ml_driver


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


def test_generate_candidate_grid_auto_fills_unspecified_dimensions_under_target() -> None:
    search_space = [
        _dimension("a", minimum=0.0, maximum=10.0, default=0.0, step=1.0),
        _dimension("b", minimum=0.0, maximum=9.0, default=4.5),
        _dimension("c", minimum=-4.0, maximum=4.0, default=0.0),
    ]

    counts = ml_driver._infer_grid_level_counts(search_space, n_samples=1000)
    grid = ml_driver._generate_candidate_grid(search_space, n_samples=1000)

    assert counts == [11, 10, 9]
    assert grid.shape == (990, 3)
    assert len(np.unique(grid[:, 0])) == 11
    assert len(np.unique(grid[:, 1])) == 10
    assert len(np.unique(grid[:, 2])) == 9


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
