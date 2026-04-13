"""Active-learning orchestration layer for BIGPOPA.

This module sits between the Electron shell and the legacy ``run_ifs.py``
runner. It coordinates the optimization loop and delegates all IFs execution
and extraction work to the existing scripts without modifying their behavior.
Electron should trigger this entry point instead of calling ``run_ifs.py``
directly.
"""

from __future__ import annotations

import argparse
import hashlib
import math
import json
import os
import secrets
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Tuple

import numpy as np
from runtime import dataset_utils
from runtime.artifact_retention import RETENTION_NONE, normalize_artifact_retention_mode
from db.input_profiles import resolve_profile
from runtime.ml_method import normalize_ml_method
from runtime.model_run_store import (
    count_completed_trial_runs,
    fetch_latest_result_for_model,
    find_active_run_id_for_model,
    insert_model_run,
    load_model_definition,
    update_model_run,
)
from runtime.model_status import (
    FALLBACK_FIT_POOLED,
    FIT_EVALUATED,
    IFS_RUN_STARTED,
    IFS_RUN_FAILED,
    IFS_RUN_COMPLETED,
    MODEL_REUSED,
    cached_result_status,
    visible_fit_pooled,
)
from runtime.model_setup import (
    canonical_config,
    ensure_bigpopa_schema,
    hash_model_id,
)
from optimization.active_learning import active_learning_loop
from optimization.ensemble_training import (
    estimate_prediction_chunk_size,
    validate_surrogate_memory,
)
from optimization.surrogate_models import BoundsScaler, LogClippedTargetTransform
from db.schema import ensure_current_bigpopa_schema


FAIL_Y: float = FALLBACK_FIT_POOLED
DEFAULT_ML_MEMORY_BUDGET_MB: float = float(os.getenv("BIGPOPA_ML_MEMORY_BUDGET_MB", "512"))
DEFAULT_GLOBAL_PROPOSAL_FRACTION: float = 0.25
DEFAULT_LOCAL_TOP_K: int = 5
DEFAULT_LOCAL_RADIUS_FRACTION: float = 0.15
DEFAULT_LOCAL_RADIUS_DECAY: float = 0.85
DEFAULT_LOCAL_RADIUS_MIN_FRACTION: float = 0.05
DEFAULT_CANDIDATE_REFRESH_INTERVAL: int = 1
DEFAULT_MAX_POOL_REGENERATIONS: int = 3
MAX_ENUMERATED_DISCRETE_COMBINATIONS: int = 4096
PROPOSAL_MODE_REFRESHED: str = "refreshed"
PROPOSAL_MODE_DIRECT: str = "direct"
DEFAULT_PROPOSAL_MODE: str = PROPOSAL_MODE_REFRESHED
ENABLE_DEFAULT_DISTANCE_PENALTY: bool = True
DEFAULT_DISTANCE_PENALTY_STRENGTH: float = 0.15

@dataclass(frozen=True)
class UserDimensionConfig:
    minimum: float | None = None
    maximum: float | None = None
    step: float | None = None
    level_count: int | None = None

    @property
    def has_grid_config(self) -> bool:
        return self.step is not None or self.level_count is not None


@dataclass(frozen=True)
class SearchDimension:
    key: tuple[str, ...]
    display_name: str
    kind: str
    default: float
    minimum: float
    maximum: float
    step: float | None = None
    level_count: int | None = None


@dataclass(frozen=True)
class ResumeState:
    cohort_key: str
    dataset_id: str | None
    base_year: int | None
    end_year: int
    settings_signature: str
    settings_payload: str
    proposal_seed: int
    effective_iteration_count: int
    no_improve_counter: int
    best_y_prev: float | None = None


def _profile_config_to_user_config(
    config_map: dict[object, object],
) -> dict[object, UserDimensionConfig]:
    normalized: dict[object, UserDimensionConfig] = {}
    for key, value in config_map.items():
        minimum = getattr(value, "minimum", None)
        maximum = getattr(value, "maximum", None)
        step = getattr(value, "step", None)
        level_count = getattr(value, "level_count", None)
        normalized[key] = UserDimensionConfig(
            minimum=float(minimum) if minimum is not None else None,
            maximum=float(maximum) if maximum is not None else None,
            step=float(step) if step is not None else None,
            level_count=int(level_count) if level_count is not None else None,
        )
    return normalized


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def emit_stage_response(status: str, stage: str, message: str, data: Dict[str, object]) -> None:
    """Emit a JSON payload for Electron to consume."""

    payload = {
        "status": status,
        "stage": stage,
        "message": message,
        "data": data,
    }
    print(json.dumps(payload))
    sys.stdout.flush()



def stop_requested(stop_file: Path | None) -> bool:
    if stop_file is None:
        return False

    try:
        return stop_file.exists()
    except OSError:
        return False


def _resolve_run_seed(seed_value: int | None) -> int:
    if seed_value is not None:
        return int(seed_value)
    return secrets.randbits(63)


def _memory_budget_bytes() -> int:
    budget_mb = DEFAULT_ML_MEMORY_BUDGET_MB
    if not math.isfinite(budget_mb) or budget_mb <= 0:
        raise ValueError("BIGPOPA_ML_MEMORY_BUDGET_MB must be a positive finite number.")
    return int(budget_mb * 1024 * 1024)


def _validate_candidate_pool_size(*, n_rows: int, n_dimensions: int, budget_bytes: int) -> None:
    if n_rows < 0 or n_dimensions < 0:
        raise ValueError("Candidate-pool size inputs must be non-negative.")
    estimated_bytes = int(n_rows) * int(n_dimensions) * np.dtype(float).itemsize
    if estimated_bytes > budget_bytes:
        raise ValueError(
            "Candidate pool requires approximately "
            f"{estimated_bytes / 1024 / 1024:.1f} MiB, which exceeds the configured "
            f"memory budget of {budget_bytes / 1024 / 1024:.1f} MiB."
        )


def _log_candidate_pool_usage(X_grid: np.ndarray, *, memory_budget_bytes: int) -> None:
    grid = np.asarray(X_grid, dtype=float)
    if grid.ndim == 1:
        grid = grid.reshape(-1, 1)
    elif grid.ndim == 0:
        grid = grid.reshape(1, 1)
    else:
        grid = np.atleast_2d(grid)

    rows, dims = grid.shape
    print(
        "Candidate pool generated: "
        f"shape=({rows}, {dims}), "
        f"candidate_pool_rows={rows}, "
        f"candidate_pool_dimensions={dims}, "
        f"candidate_pool_mb={grid.nbytes / 1024 / 1024:.6f}, "
        f"memory_budget_mb={memory_budget_bytes / 1024 / 1024:.1f}",
        flush=True,
    )


def _build_bounds_scaler(search_space: list[SearchDimension]) -> BoundsScaler:
    lower = np.asarray([dimension.minimum for dimension in search_space], dtype=float)
    upper = np.asarray([dimension.maximum for dimension in search_space], dtype=float)
    return BoundsScaler(lower=lower, upper=upper, clip=True)


def _build_target_transform() -> LogClippedTargetTransform:
    return LogClippedTargetTransform(upper_quantile=95.0, absolute_cap=FAIL_Y)


def _default_reference_vector(search_space: list[SearchDimension]) -> np.ndarray:
    return np.asarray(
        [
            _clip_to_bounds(dimension.default, dimension.minimum, dimension.maximum)
            for dimension in search_space
        ],
        dtype=float,
    )


# --- Flattening helpers ----------------------------------------------------


def flatten_inputs(input_param: dict, input_coef: dict) -> np.ndarray:
    """Flatten parameter and coefficient dictionaries into a numeric vector.

    Keys are traversed in sorted order to guarantee deterministic layouts.
    """

    vector: List[float] = []
    for key in sorted(input_param.keys()):
        vector.append(float(input_param[key]))

    for func in sorted(input_coef.keys()):
        x_map = input_coef[func]
        for x_name in sorted(x_map.keys()):
            beta_map = x_map[x_name]
            for beta in sorted(beta_map.keys()):
                vector.append(float(beta_map[beta]))

    return np.array(vector, dtype=float)


def _format_point_for_log(values: np.ndarray) -> str:
    """Format an input vector for deterministic, readable logging output."""

    arr = np.asarray(values, dtype=float).ravel()
    joined_values = ", ".join(f"{v:.6f}" for v in arr)
    return f"({joined_values})"


def unflatten_vector(
    vector: Iterable[float], input_param_template: dict, input_coef_template: dict
) -> Tuple[dict, dict]:
    """Reconstruct parameter and coefficient dictionaries from a vector."""

    params: dict = {}
    coefs: dict = {}
    values = list(vector)
    idx = 0

    for key in sorted(input_param_template.keys()):
        if idx >= len(values):
            raise ValueError("Vector is too short to reconstruct parameters")
        params[key] = float(values[idx])
        idx += 1

    for func in sorted(input_coef_template.keys()):
        coefs[func] = {}
        x_map = input_coef_template[func]
        for x_name in sorted(x_map.keys()):
            coefs[func][x_name] = {}
            beta_map = x_map[x_name]
            for beta in sorted(beta_map.keys()):
                if idx >= len(values):
                    raise ValueError("Vector is too short to reconstruct coefficients")
                coefs[func][x_name][beta] = float(values[idx])
                idx += 1

    if idx != len(values):
        raise ValueError("Vector length does not match template structure")

    return params, coefs


# --- Database helpers ------------------------------------------------------


def _model_input_has_dataset_id(conn: sqlite3.Connection) -> bool:
    # Unified model_run rows always carry dataset_id.
    del conn
    return True


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _build_resume_cohort_key(
    *,
    dataset_id: str | None,
    base_year: int | None,
    end_year: int,
) -> str:
    payload = {
        "dataset_id": dataset_id,
        "base_year": int(base_year) if base_year is not None else None,
        "end_year": int(end_year),
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _build_search_space_signature(search_space: list[SearchDimension]) -> str:
    payload = [
        {
            "key": list(dimension.key),
            "kind": dimension.kind,
            "default": float(dimension.default),
            "minimum": float(dimension.minimum),
            "maximum": float(dimension.maximum),
            "step": None if dimension.step is None else float(dimension.step),
            "level_count": dimension.level_count,
        }
        for dimension in search_space
    ]
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _build_resume_settings_payload(
    *,
    ml_method_config,
    n_sample: int,
    n_convergence: int,
    min_convergence_pct: float,
    proposal_mode: str,
    explicit_random_seed: int | None,
    search_space: list[SearchDimension],
) -> tuple[str, str]:
    payload = {
        "model_type": ml_method_config.model_type,
        "ml_method": ml_method_config.normalized_value,
        "n_sample": int(n_sample),
        "n_convergence": int(n_convergence),
        "min_convergence_pct": float(min_convergence_pct),
        "proposal_mode": str(proposal_mode),
        "explicit_random_seed": explicit_random_seed,
        "candidate_refresh_interval": int(DEFAULT_CANDIDATE_REFRESH_INTERVAL),
        "max_pool_regenerations": int(DEFAULT_MAX_POOL_REGENERATIONS),
        "distance_penalty_enabled": bool(ENABLE_DEFAULT_DISTANCE_PENALTY),
        "distance_penalty_strength": float(DEFAULT_DISTANCE_PENALTY_STRENGTH),
        "search_space_signature": _build_search_space_signature(search_space),
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    signature = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    return signature, serialized


def _load_resume_state(
    conn: sqlite3.Connection,
    *,
    cohort_key: str,
) -> ResumeState | None:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            cohort_key,
            dataset_id,
            base_year,
            end_year,
            settings_signature,
            settings_payload,
            proposal_seed,
            effective_iteration_count,
            no_improve_counter,
            best_y_prev
        FROM ml_resume_state
        WHERE cohort_key = ?
        LIMIT 1
        """,
        (cohort_key,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return ResumeState(
        cohort_key=str(row[0]),
        dataset_id=row[1],
        base_year=int(row[2]) if row[2] is not None else None,
        end_year=int(row[3]),
        settings_signature=str(row[4]),
        settings_payload=str(row[5]),
        proposal_seed=int(row[6]),
        effective_iteration_count=max(0, int(row[7] or 0)),
        no_improve_counter=max(0, int(row[8] or 0)),
        best_y_prev=float(row[9]) if row[9] is not None else None,
    )


def _persist_resume_state(
    conn: sqlite3.Connection,
    *,
    cohort_key: str,
    dataset_id: str | None,
    base_year: int | None,
    end_year: int,
    settings_signature: str,
    settings_payload: str,
    proposal_seed: int,
    effective_iteration_count: int,
    no_improve_counter: int,
    best_y_prev: float | None,
) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO ml_resume_state (
                cohort_key,
                dataset_id,
                base_year,
                end_year,
                settings_signature,
                settings_payload,
                proposal_seed,
                effective_iteration_count,
                no_improve_counter,
                best_y_prev,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cohort_key) DO UPDATE SET
                dataset_id=excluded.dataset_id,
                base_year=excluded.base_year,
                end_year=excluded.end_year,
                settings_signature=excluded.settings_signature,
                settings_payload=excluded.settings_payload,
                proposal_seed=excluded.proposal_seed,
                effective_iteration_count=excluded.effective_iteration_count,
                no_improve_counter=excluded.no_improve_counter,
                best_y_prev=excluded.best_y_prev,
                updated_at_utc=excluded.updated_at_utc
            """,
            (
                cohort_key,
                dataset_id,
                base_year,
                end_year,
                settings_signature,
                settings_payload,
                int(proposal_seed),
                max(0, int(effective_iteration_count)),
                max(0, int(no_improve_counter)),
                None if best_y_prev is None else float(best_y_prev),
                _utc_now_iso(),
            ),
        )


def _resolve_resume_behavior(
    conn: sqlite3.Connection,
    *,
    dataset_id: str | None,
    base_year: int | None,
    end_year: int,
    settings_signature: str,
    settings_payload: str,
    explicit_random_seed: int | None,
) -> tuple[ResumeState | None, int, int, int, str]:
    cohort_key = _build_resume_cohort_key(
        dataset_id=dataset_id,
        base_year=base_year,
        end_year=end_year,
    )
    existing_state = _load_resume_state(conn, cohort_key=cohort_key)

    if explicit_random_seed is not None:
        proposal_seed = int(explicit_random_seed)
    elif existing_state is not None:
        proposal_seed = int(existing_state.proposal_seed)
    else:
        proposal_seed = _resolve_run_seed(None)

    historical_iteration_count = count_completed_trial_runs(conn, dataset_id)
    if (
        historical_iteration_count <= 0
        and existing_state is not None
        and existing_state.effective_iteration_count > 0
    ):
        historical_iteration_count = int(existing_state.effective_iteration_count)

    if existing_state is None:
        return None, proposal_seed, historical_iteration_count, 0, "fresh_history"

    if existing_state.settings_signature == settings_signature:
        return (
            existing_state,
            proposal_seed,
            historical_iteration_count,
            0,
            "resume_training_history",
        )

    return existing_state, proposal_seed, historical_iteration_count, 0, "reset_search_state"


def _upsert_model_output_tracking(
    conn: sqlite3.Connection,
    *,
    ifs_id: int,
    model_id: str,
    trial_index: int,
    batch_index: int,
    started_at_utc: str | None = None,
    completed_at_utc: str | None = None,
    model_status: str | None = None,
    fit_pooled: float | None = None,
) -> None:
    del ifs_id, trial_index, batch_index, started_at_utc, completed_at_utc
    run_id = find_active_run_id_for_model(conn, model_id=model_id)
    if run_id is None:
        raise RuntimeError(f"No model_run row was found for model_id={model_id}.")
    update_model_run(
        conn,
        run_id=run_id,
        model_status=model_status,
        fit_pooled=fit_pooled,
    )


def _fetch_model_output_snapshot(
    conn: sqlite3.Connection,
    *,
    model_id: str,
) -> tuple[str | None, float | None]:
    status, fit_pooled, _fit_var = fetch_latest_result_for_model(conn, model_id=model_id)
    return status, fit_pooled


def _repair_model_output_batch_indexes(conn: sqlite3.Connection) -> int:
    del conn
    return 0


def _normalize_model_output_batch_indexes(conn: sqlite3.Connection) -> int:
    """Commit batch-index normalization before other connections begin writing."""

    del conn
    return 0


def _load_model_by_id(
    conn: sqlite3.Connection, has_dataset_id: bool, model_id: str
) -> Tuple[int, str, dict, dict, dict, str | None]:
    del has_dataset_id
    definition = load_model_definition(conn, model_id)
    return (
        int(definition.ifs_id),
        str(definition.model_id),
        definition.input_param,
        definition.input_coef,
        definition.output_set,
        definition.dataset_id,
    )


def _load_persisted_ml_method(conn: sqlite3.Connection, ifs_id: int):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT ml_method FROM ifs_version WHERE ifs_id = ? LIMIT 1",
        (ifs_id,),
    )
    row = cursor.fetchone()
    if not row or row[0] is None:
        raise ValueError(
            "bigpopa.db is missing ifs_version.ml_method for the selected model. "
            "Please rerun model setup."
        )
    try:
        return normalize_ml_method(row[0])
    except ValueError as exc:
        raise ValueError(
            "bigpopa.db contains an invalid ifs_version.ml_method for the selected model. "
            "Please rerun model setup."
        ) from exc


def _get_ifs_static_id(conn: sqlite3.Connection, ifs_id: int) -> Tuple[int | None, int | None]:
    cursor = conn.cursor()
    cursor.execute("SELECT ifs_static_id, base_year FROM ifs_version WHERE ifs_id = ? LIMIT 1", (ifs_id,))
    row = cursor.fetchone()
    if row:
        return int(row[0]) if row[0] is not None else None, int(row[1]) if row[1] is not None else None
    return None, None


def _build_search_space(
    conn: sqlite3.Connection,
    ifs_static_id: int,
    input_param: dict,
    input_coef: dict,
    user_param_configs: dict[str, UserDimensionConfig],
    user_coef_configs: dict[tuple[str, str, str], UserDimensionConfig],
) -> list[SearchDimension]:
    cursor = conn.cursor()
    search_space: list[SearchDimension] = []

    for param_name in sorted(input_param.keys()):
        cursor.execute(
            """
            SELECT param_min, param_max, param_default
            FROM parameter
            WHERE ifs_static_id = ? AND LOWER(param_name) = LOWER(?)
            LIMIT 1
            """,
            (ifs_static_id, param_name),
        )
        row = cursor.fetchone()
        default_val = float(input_param[param_name])
        param_min = None
        param_max = None
        if row:
            param_min, param_max, param_default = row
            if param_default is not None:
                default_val = float(param_default)

        if default_val != 0:
            fallback_min = default_val - abs(default_val)
            fallback_max = default_val + abs(default_val)
        else:
            fallback_min, fallback_max = -1.0, 1.0

        user_config = user_param_configs.get(param_name, UserDimensionConfig())
        user_min = user_config.minimum
        user_max = user_config.maximum
        min_val = float(user_min) if user_min is not None else (
            float(param_min) if param_min is not None else float(fallback_min)
        )
        max_val = float(user_max) if user_max is not None else (
            float(param_max) if param_max is not None else float(fallback_max)
        )
        if min_val is not None and max_val is not None and min_val > max_val:
            original_min, original_max = min_val, max_val
            min_val, max_val = max_val, min_val
            print(
                f"Warning: swapped bounds for parameter '{param_name}' because min ({original_min}) > max ({original_max})",
                flush=True,
            )
        search_space.append(
            SearchDimension(
                key=("param", param_name),
                display_name=f"parameter '{param_name}'",
                kind="param",
                default=default_val,
                minimum=float(min_val),
                maximum=float(max_val),
                step=user_config.step,
                level_count=user_config.level_count,
            )
        )

    for func in sorted(input_coef.keys()):
        for x_name in sorted(input_coef[func].keys()):
            for beta in sorted(input_coef[func][x_name].keys()):
                cursor.execute(
                    """
                    SELECT beta_default, beta_std
                    FROM coefficient
                    WHERE ifs_static_id = ?
                      AND LOWER(function_name) = LOWER(?)
                      AND LOWER(x_name) = LOWER(?)
                      AND LOWER(beta_name) = LOWER(?)
                    LIMIT 1
                    """,
                    (ifs_static_id, func, x_name, beta),
                )
                row = cursor.fetchone()
                default_val = float(input_coef[func][x_name][beta])
                beta_default = row[0] if row else None
                beta_std = row[1] if row else None

                center = float(beta_default) if beta_default is not None else default_val
                db_min = None
                db_max = None
                if beta_std is not None:
                    span = abs(beta_std) * 3
                    db_min = center - span
                    db_max = center + span

                if center != 0:
                    fallback_min = center - abs(center)
                    fallback_max = center + abs(center)
                else:
                    fallback_min, fallback_max = -1.0, 1.0

                user_config = user_coef_configs.get((func, x_name, beta), UserDimensionConfig())
                user_min = user_config.minimum
                user_max = user_config.maximum
                bounds_fully_specified = user_min is not None and user_max is not None

                min_val = float(user_min) if user_min is not None else (
                    float(db_min) if db_min is not None else float(fallback_min)
                )
                max_val = float(user_max) if user_max is not None else (
                    float(db_max) if db_max is not None else float(fallback_max)
                )

                if beta_default is not None and not bounds_fully_specified:
                    if beta_default > 0:
                        min_val = max(min_val, 0.0)
                    elif beta_default < 0:
                        max_val = min(max_val, 0.0)

                if min_val is not None and max_val is not None and min_val > max_val:
                    original_min, original_max = min_val, max_val
                    min_val, max_val = max_val, min_val
                    print(
                        (
                            "Warning: swapped bounds for coefficient "
                            f"(func='{func}', x_name='{x_name}', beta='{beta}') "
                            f"because min ({original_min}) > max ({original_max})"
                        ),
                        flush=True,
                    )

                search_space.append(
                    SearchDimension(
                        key=("coef", func, x_name, beta),
                        display_name=f"coefficient '{func}/{x_name}/{beta}'",
                        kind="coef",
                        default=center,
                        minimum=float(min_val),
                        maximum=float(max_val),
                        step=user_config.step,
                        level_count=user_config.level_count,
                    )
                )
    return search_space


def _sample_grid(
    ranges: List[Tuple[float, float]],
    n_samples: int = 200,
    *,
    run_seed: int | None = None,
) -> np.ndarray:
    rng = np.random.default_rng(run_seed)
    return _sample_ranges(ranges, n_samples=n_samples, rng=rng)


def _sample_ranges(
    ranges: List[Tuple[float, float]],
    *,
    n_samples: int,
    rng: np.random.Generator,
) -> np.ndarray:
    if not ranges:
        return np.empty((n_samples, 0), dtype=float)

    bounds = np.asarray(ranges, dtype=float)
    low = bounds[:, 0]
    high = bounds[:, 1]
    samples = rng.uniform(low=low, high=high, size=(n_samples, len(ranges)))
    equal_mask = np.isclose(low, high)
    if np.any(equal_mask):
        samples[:, equal_mask] = low[equal_mask]
    return samples.astype(float, copy=False)


def _has_grid_configuration(search_space: list[SearchDimension]) -> bool:
    return any(dimension.step is not None or dimension.level_count is not None for dimension in search_space)


def _clip_to_bounds(value: float, low: float, high: float) -> float:
    return min(max(float(value), float(low)), float(high))


def _adaptive_local_radius_fraction(iteration: int) -> float:
    decayed = DEFAULT_LOCAL_RADIUS_FRACTION * (DEFAULT_LOCAL_RADIUS_DECAY ** max(0, int(iteration)))
    return max(DEFAULT_LOCAL_RADIUS_MIN_FRACTION, float(decayed))


def _generate_levels_for_count(dimension: SearchDimension, level_count: int) -> np.ndarray:
    if level_count < 1:
        raise ValueError(f"Level count for {dimension.display_name} must be at least 1.")
    if math.isclose(dimension.minimum, dimension.maximum):
        return np.asarray([dimension.minimum], dtype=float)
    if level_count == 1:
        return np.asarray(
            [_clip_to_bounds(dimension.default, dimension.minimum, dimension.maximum)],
            dtype=float,
        )
    return np.linspace(dimension.minimum, dimension.maximum, num=level_count, dtype=float)


def _generate_levels_for_step(dimension: SearchDimension, step: float) -> np.ndarray:
    if step <= 0:
        raise ValueError(f"Step for {dimension.display_name} must be greater than 0.")
    if math.isclose(dimension.minimum, dimension.maximum):
        return np.asarray([dimension.minimum], dtype=float)

    values: list[float] = []
    index = 0
    tolerance = max(1e-12, abs(step) * 1e-12, abs(dimension.maximum) * 1e-12)
    while True:
        value = dimension.minimum + index * step
        if value > dimension.maximum + tolerance:
            break
        values.append(_clip_to_bounds(value, dimension.minimum, dimension.maximum))
        index += 1
    return np.asarray(values, dtype=float)


def _explicit_level_values(dimension: SearchDimension) -> np.ndarray | None:
    if dimension.step is not None:
        return _generate_levels_for_step(dimension, dimension.step)
    if dimension.level_count is not None:
        return _generate_levels_for_count(dimension, dimension.level_count)
    return None


def _infer_grid_level_counts(search_space: list[SearchDimension], n_samples: int) -> list[int]:
    if n_samples < 1:
        raise ValueError("n_sample must be at least 1 when grid mode is enabled.")

    counts: list[int] = []
    unspecified_indices: list[int] = []
    explicit_product = 1

    for index, dimension in enumerate(search_space):
        explicit_values = _explicit_level_values(dimension)
        if explicit_values is None:
            counts.append(1)
            unspecified_indices.append(index)
            continue
        explicit_count = int(len(explicit_values))
        counts.append(explicit_count)
        explicit_product *= explicit_count

    if explicit_product > n_samples:
        raise ValueError(
            "Explicit grid settings produce "
            f"{explicit_product} candidates, which exceeds n_sample={n_samples}."
        )

    total_candidates = explicit_product
    while unspecified_indices:
        next_index = min(
            unspecified_indices,
            key=lambda idx: (counts[idx], search_space[idx].key),
        )
        next_total = (total_candidates // counts[next_index]) * (counts[next_index] + 1)
        if next_total > n_samples:
            break
        counts[next_index] += 1
        total_candidates = next_total

    return counts


def _cartesian_product(level_values: list[np.ndarray]) -> np.ndarray:
    total_rows = math.prod(len(values) for values in level_values)
    total_dimensions = len(level_values)
    grid = np.empty((total_rows, total_dimensions), dtype=float)

    repeat_block = total_rows
    for column_index, values in enumerate(level_values):
        value_array = np.asarray(values, dtype=float)
        repeat_block //= len(value_array)
        pattern = np.repeat(value_array, repeat_block)
        tile_count = total_rows // len(pattern)
        grid[:, column_index] = np.tile(pattern, tile_count)

    return grid


def _generate_candidate_grid(
    search_space: list[SearchDimension],
    n_samples: int,
    *,
    memory_budget_bytes: int | None = None,
) -> np.ndarray:
    if not search_space:
        return np.empty((n_samples, 0), dtype=float)

    level_counts = _infer_grid_level_counts(search_space, n_samples)
    level_values: list[np.ndarray] = []

    for dimension, inferred_count in zip(search_space, level_counts):
        explicit_values = _explicit_level_values(dimension)
        if explicit_values is not None:
            level_values.append(explicit_values)
        else:
            level_values.append(_generate_levels_for_count(dimension, inferred_count))

    total_rows = math.prod(len(values) for values in level_values)
    if memory_budget_bytes is not None:
        _validate_candidate_pool_size(
            n_rows=total_rows,
            n_dimensions=len(search_space),
            budget_bytes=memory_budget_bytes,
        )
    return _cartesian_product(level_values)


def _split_search_space(
    search_space: list[SearchDimension],
) -> tuple[list[tuple[int, SearchDimension]], list[tuple[int, SearchDimension]]]:
    explicit_dimensions: list[tuple[int, SearchDimension]] = []
    free_dimensions: list[tuple[int, SearchDimension]] = []

    for index, dimension in enumerate(search_space):
        if _explicit_level_values(dimension) is None:
            free_dimensions.append((index, dimension))
        else:
            explicit_dimensions.append((index, dimension))

    return explicit_dimensions, free_dimensions


def _generate_hybrid_candidate_grid(
    search_space: list[SearchDimension],
    n_samples: int,
    *,
    run_seed: int | None = None,
    memory_budget_bytes: int | None = None,
) -> np.ndarray:
    if not search_space:
        return np.empty((n_samples, 0), dtype=float)

    explicit_dimensions, free_dimensions = _split_search_space(search_space)
    if not explicit_dimensions:
        return _sample_grid(
            [(dimension.minimum, dimension.maximum) for dimension in search_space],
            n_samples=n_samples,
            run_seed=run_seed,
        )
    if not free_dimensions:
        return _generate_candidate_grid(
            search_space,
            n_samples=n_samples,
            memory_budget_bytes=memory_budget_bytes,
        )

    explicit_search_space = [dimension for _, dimension in explicit_dimensions]
    explicit_grid = _generate_candidate_grid(
        explicit_search_space,
        n_samples=n_samples,
        memory_budget_bytes=memory_budget_bytes,
    )
    explicit_count = explicit_grid.shape[0]
    if explicit_count > n_samples:
        raise ValueError(
            "Explicit grid settings produce "
            f"{explicit_count} candidates, which exceeds n_sample={n_samples}."
        )

    base_count, remainder = divmod(n_samples, explicit_count)
    free_ranges = [(dimension.minimum, dimension.maximum) for _, dimension in free_dimensions]
    rng = np.random.default_rng(run_seed)
    total_dimensions = len(search_space)
    if memory_budget_bytes is not None:
        _validate_candidate_pool_size(
            n_rows=n_samples,
            n_dimensions=total_dimensions,
            budget_bytes=memory_budget_bytes,
        )
    rows = np.empty((n_samples, total_dimensions), dtype=float)
    start = 0

    for combo_index, explicit_values in enumerate(explicit_grid):
        sample_count = base_count + (1 if combo_index < remainder else 0)
        free_samples = _sample_ranges(free_ranges, n_samples=sample_count, rng=rng)
        stop = start + sample_count
        for value, (dimension_index, _) in zip(explicit_values, explicit_dimensions):
            rows[start:stop, dimension_index] = value
        for free_column, (dimension_index, _) in enumerate(free_dimensions):
            rows[start:stop, dimension_index] = free_samples[:, free_column]
        start = stop

    return rows


def _append_unique_row(
    rows: list[np.ndarray],
    seen: set[tuple[float, ...]],
    row: np.ndarray,
    *,
    precision: int = 12,
) -> bool:
    key = tuple(np.round(np.asarray(row, dtype=float), precision))
    if key in seen:
        return False
    seen.add(key)
    rows.append(np.asarray(row, dtype=float))
    return True


def _balanced_discrete_combo_sample(
    level_values: list[np.ndarray],
    *,
    n_combos: int,
    rng: np.random.Generator,
) -> np.ndarray:
    if not level_values:
        return np.zeros((1, 0), dtype=float)

    total_possible = math.prod(len(values) for values in level_values)
    target = min(max(1, n_combos), total_possible)
    rows: list[np.ndarray] = []
    seen: set[tuple[float, ...]] = set()
    sequences: list[np.ndarray] = []

    for values in level_values:
        indices = np.arange(len(values), dtype=int)
        repeats = max(1, math.ceil(target / len(indices)))
        balanced = np.tile(indices, repeats)[:target].copy()
        rng.shuffle(balanced)
        sequences.append(balanced)

    for row_index in range(target):
        combo = np.asarray(
            [values[sequence[row_index]] for values, sequence in zip(level_values, sequences)],
            dtype=float,
        )
        _append_unique_row(rows, seen, combo)

    attempts = 0
    max_attempts = max(target * 20, 100)
    while len(rows) < target and attempts < max_attempts:
        combo = np.asarray(
            [values[rng.integers(0, len(values))] for values in level_values],
            dtype=float,
        )
        if _append_unique_row(rows, seen, combo):
            attempts = 0
            continue
        attempts += 1

    if not rows:
        return np.zeros((0, len(level_values)), dtype=float)
    return np.vstack(rows)


def _select_discrete_combinations(
    explicit_dimensions: list[tuple[int, SearchDimension]],
    *,
    n_samples: int,
    rng: np.random.Generator,
) -> np.ndarray:
    if not explicit_dimensions:
        return np.zeros((1, 0), dtype=float)

    level_values = [_explicit_level_values(dimension) for _, dimension in explicit_dimensions]
    if any(values is None for values in level_values):
        raise ValueError("Discrete-combination selection requires explicit levels for all dimensions.")
    normalized_level_values = [np.asarray(values, dtype=float) for values in level_values if values is not None]
    total_possible = math.prod(len(values) for values in normalized_level_values)
    enumerate_all = total_possible <= min(MAX_ENUMERATED_DISCRETE_COMBINATIONS, max(1, n_samples))
    if enumerate_all:
        return _cartesian_product(normalized_level_values)

    min_candidates_per_combo = 4
    combo_budget = max(1, n_samples // min_candidates_per_combo)
    return _balanced_discrete_combo_sample(
        normalized_level_values,
        n_combos=min(combo_budget, total_possible),
        rng=rng,
    )


def _matching_seed_vectors(
    *,
    X_obs: np.ndarray,
    Y_obs: np.ndarray,
    explicit_dimensions: list[tuple[int, SearchDimension]],
    combo_values: np.ndarray,
    top_k: int,
) -> np.ndarray:
    if len(X_obs) == 0:
        return np.zeros((0, 0), dtype=float)

    top_indices = np.argsort(np.asarray(Y_obs, dtype=float))[: max(1, top_k)]
    top_vectors = np.asarray(X_obs[top_indices], dtype=float)
    if not explicit_dimensions:
        return top_vectors

    matching_rows = []
    for vector in top_vectors:
        is_match = True
        for value, (dimension_index, _) in zip(combo_values, explicit_dimensions):
            if not np.isclose(vector[dimension_index], value):
                is_match = False
                break
        if is_match:
            matching_rows.append(vector)

    if matching_rows:
        return np.asarray(matching_rows, dtype=float)
    return top_vectors


def _sample_local_continuous_rows(
    *,
    free_dimensions: list[tuple[int, SearchDimension]],
    seed_vectors: np.ndarray,
    n_rows: int,
    rng: np.random.Generator,
    radius_fraction: float,
) -> np.ndarray:
    if n_rows < 1:
        return np.empty((0, len(free_dimensions)), dtype=float)
    if not free_dimensions:
        return np.empty((n_rows, 0), dtype=float)
    if seed_vectors.size == 0:
        ranges = [(dimension.minimum, dimension.maximum) for _, dimension in free_dimensions]
        return _sample_ranges(ranges, n_samples=n_rows, rng=rng)

    rows = np.empty((n_rows, len(free_dimensions)), dtype=float)
    seed_indices = rng.integers(0, len(seed_vectors), size=n_rows)

    for row_index, seed_index in enumerate(seed_indices):
        seed = seed_vectors[seed_index]
        for column_index, (dimension_index, dimension) in enumerate(free_dimensions):
            span = float(dimension.maximum - dimension.minimum)
            if np.isclose(span, 0.0):
                rows[row_index, column_index] = float(dimension.minimum)
                continue
            center = float(seed[dimension_index])
            scale = max(span * radius_fraction, 1e-12)
            sampled = rng.normal(loc=center, scale=scale)
            rows[row_index, column_index] = _clip_to_bounds(
                sampled,
                dimension.minimum,
                dimension.maximum,
            )
    return rows


def _assemble_candidate_pool(
    *,
    search_space: list[SearchDimension],
    discrete_combinations: np.ndarray,
    X_obs: np.ndarray,
    Y_obs: np.ndarray,
    n_samples: int,
    rng: np.random.Generator,
    iteration: int,
) -> np.ndarray:
    total_dimensions = len(search_space)
    explicit_dimensions, free_dimensions = _split_search_space(search_space)
    if discrete_combinations.size == 0:
        discrete_combinations = np.zeros((1, 0), dtype=float)

    total_requested = max(1, n_samples)
    combo_count = len(discrete_combinations)
    base_count, remainder = divmod(total_requested, combo_count)
    rows: list[np.ndarray] = []
    seen: set[tuple[float, ...]] = set()
    free_ranges = [(dimension.minimum, dimension.maximum) for _, dimension in free_dimensions]
    local_radius_fraction = _adaptive_local_radius_fraction(iteration)

    for combo_index, combo_values in enumerate(discrete_combinations):
        candidate_count = base_count + (1 if combo_index < remainder else 0)
        if candidate_count < 1:
            continue

        global_count = int(round(candidate_count * DEFAULT_GLOBAL_PROPOSAL_FRACTION))
        global_count = min(candidate_count, max(1, global_count)) if candidate_count > 1 else candidate_count
        local_count = candidate_count - global_count

        if free_dimensions:
            global_rows = _sample_ranges(free_ranges, n_samples=global_count, rng=rng)
            seed_vectors = _matching_seed_vectors(
                X_obs=X_obs,
                Y_obs=Y_obs,
                explicit_dimensions=explicit_dimensions,
                combo_values=combo_values,
                top_k=DEFAULT_LOCAL_TOP_K,
            )
            local_rows = _sample_local_continuous_rows(
                free_dimensions=free_dimensions,
                seed_vectors=seed_vectors,
                n_rows=local_count,
                rng=rng,
                radius_fraction=local_radius_fraction,
            )
            continuous_rows = np.vstack([global_rows, local_rows]) if local_rows.size else global_rows
        else:
            continuous_rows = np.empty((candidate_count, 0), dtype=float)

        for continuous_row in continuous_rows:
            row = np.empty(total_dimensions, dtype=float)
            for value, (dimension_index, _) in zip(combo_values, explicit_dimensions):
                row[dimension_index] = float(value)
            for free_value, (dimension_index, _) in zip(continuous_row, free_dimensions):
                row[dimension_index] = float(free_value)
            _append_unique_row(rows, seen, row)

    attempts = 0
    max_attempts = max(total_requested * 10, 100)
    while len(rows) < total_requested and attempts < max_attempts:
        if explicit_dimensions:
            combo_values = discrete_combinations[rng.integers(0, combo_count)]
        else:
            combo_values = np.empty(0, dtype=float)
        if free_dimensions:
            continuous_row = _sample_ranges(free_ranges, n_samples=1, rng=rng)[0]
        else:
            continuous_row = np.empty(0, dtype=float)
        row = np.empty(total_dimensions, dtype=float)
        for value, (dimension_index, _) in zip(combo_values, explicit_dimensions):
            row[dimension_index] = float(value)
        for free_value, (dimension_index, _) in zip(continuous_row, free_dimensions):
            row[dimension_index] = float(free_value)
        if _append_unique_row(rows, seen, row):
            attempts = 0
            continue
        attempts += 1

    if not rows:
        return np.empty((0, total_dimensions), dtype=float)
    return np.vstack(rows)


def _build_candidate_generator(
    *,
    search_space: list[SearchDimension],
    n_samples: int,
    run_seed: int,
    memory_budget_bytes: int,
) -> Callable[..., np.ndarray]:
    _validate_candidate_pool_size(
        n_rows=max(1, n_samples),
        n_dimensions=len(search_space),
        budget_bytes=memory_budget_bytes,
    )

    def generator(
        *,
        X_obs: np.ndarray,
        Y_obs: np.ndarray,
        iteration: int,
        refresh_attempt: int = 0,
    ) -> np.ndarray:
        # Refresh attempts must produce a new deterministic pool for the same
        # optimization iteration when the previous pool was already exhausted.
        rng = np.random.default_rng(run_seed + int(iteration) + int(refresh_attempt) + 1)
        explicit_dimensions, _ = _split_search_space(search_space)
        discrete_combinations = _select_discrete_combinations(
            explicit_dimensions,
            n_samples=max(1, n_samples),
            rng=rng,
        )
        candidates = _assemble_candidate_pool(
            search_space=search_space,
            discrete_combinations=discrete_combinations,
            X_obs=np.asarray(X_obs, dtype=float),
            Y_obs=np.asarray(Y_obs, dtype=float),
            n_samples=max(1, n_samples),
            rng=rng,
            iteration=iteration,
        )
        _log_candidate_pool_usage(candidates, memory_budget_bytes=memory_budget_bytes)
        return candidates

    return generator


def _build_default_distance_penalty(
    *,
    search_space: list[SearchDimension],
    x_scaler: BoundsScaler,
    strength: float,
) -> Callable[[np.ndarray], np.ndarray]:
    scaled_default = np.asarray(
        x_scaler.transform(_default_reference_vector(search_space).reshape(1, -1))[0],
        dtype=float,
    )
    penalty_strength = max(0.0, float(strength))

    def penalty(X: np.ndarray) -> np.ndarray:
        X_arr = np.asarray(X, dtype=float)
        if X_arr.ndim == 1:
            X_arr = X_arr.reshape(1, -1)
        scaled = np.asarray(x_scaler.transform(X_arr), dtype=float)
        delta = scaled - scaled_default
        return penalty_strength * np.sum(delta * delta, axis=1)

    return penalty


def _propose_direct_candidate(
    *,
    search_space: list[SearchDimension],
    X_obs: np.ndarray,
    Y_obs: np.ndarray,
    iteration: int,
    run_seed: int,
) -> np.ndarray:
    """Placeholder for future direct acquisition optimization.

    The planned implementation will optimize the acquisition function directly
    over the mixed discrete-continuous search space instead of ranking a
    sampled candidate pool.

    Intended behavior:
    - Enumerate all discrete combinations when the Cartesian product is small.
    - Cap or sample discrete combinations when the product is large.
    - For each selected discrete combination, optimize the continuous
      subspace with a bounded multi-start optimizer.
    - Detect duplicates against prior observations and fall back to an
      alternate proposal when needed.
    - Surface optimizer failures clearly so the caller can decide whether to
      retry, adjust settings, or fall back to refreshed screening.
    """

    raise NotImplementedError(
        "Direct acquisition proposal mode is reserved for a future implementation. "
        "The intended design is mixed discrete-continuous acquisition optimization "
        "with enumerate-small/cap-large discrete handling, bounded multi-start "
        "continuous optimization, and duplicate-aware fallback logic."
    )


def _build_direct_candidate_generator(
    *,
    search_space: list[SearchDimension],
    run_seed: int,
) -> Callable[..., np.ndarray]:
    def generator(
        *,
        X_obs: np.ndarray,
        Y_obs: np.ndarray,
        iteration: int,
        refresh_attempt: int = 0,
    ) -> np.ndarray:
        return _propose_direct_candidate(
            search_space=search_space,
            X_obs=np.asarray(X_obs, dtype=float),
            Y_obs=np.asarray(Y_obs, dtype=float),
            iteration=iteration,
            run_seed=run_seed,
        )

    return generator


def _build_proposal_generator(
    *,
    search_space: list[SearchDimension],
    n_samples: int,
    run_seed: int,
    memory_budget_bytes: int,
    proposal_mode: str = DEFAULT_PROPOSAL_MODE,
) -> Callable[..., np.ndarray]:
    normalized_mode = str(proposal_mode).strip().lower()
    if normalized_mode == PROPOSAL_MODE_REFRESHED:
        return _build_candidate_generator(
            search_space=search_space,
            n_samples=n_samples,
            run_seed=run_seed,
            memory_budget_bytes=memory_budget_bytes,
        )
    if normalized_mode == PROPOSAL_MODE_DIRECT:
        return _build_direct_candidate_generator(
            search_space=search_space,
            run_seed=run_seed,
        )
    raise ValueError(f"Unknown proposal_mode: {proposal_mode}")


def _cell_has_value(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        value = value.strip()
        if not value or value.lower() in {"nan", "null"}:
            return False
    return True


def _parse_config_float(value: object, *, field_name: str, label: str) -> float | None:
    if not _cell_has_value(value):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} for {label} must be numeric.") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{field_name} for {label} must be finite.")
    return parsed


def _parse_config_int(value: object, *, field_name: str, label: str) -> int | None:
    parsed = _parse_config_float(value, field_name=field_name, label=label)
    if parsed is None:
        return None
    if not float(parsed).is_integer():
        raise ValueError(f"{field_name} for {label} must be an integer.")
    return int(parsed)
# --- Active learning orchestration ----------------------------------------


def _run_model(
    *,
    args: argparse.Namespace,
    param_values: dict,
    coef_values: dict,
    output_set: dict,
    ifs_id: int,
    dataset_id: str | None,
    bigpopa_db: Path,
    dataset_id_supported: bool,
    trial_index: int,
    batch_index: int,
) -> Tuple[float, str]:
    del dataset_id_supported
    canonical = canonical_config(ifs_id, param_values, coef_values, output_set)
    model_id = hash_model_id(canonical)
    started_at_utc = _utc_now_iso()
    model_run_id: int | None = None

    with sqlite3.connect(bigpopa_db) as conn:
        cursor = conn.cursor()
        ensure_current_bigpopa_schema(cursor)
        model_run_id = insert_model_run(
            conn,
            ifs_id=ifs_id,
            model_id=model_id,
            dataset_id=dataset_id,
            input_param=canonical["input_param"],
            input_coef=canonical["input_coef"],
            output_set=canonical["output_set"],
            model_status=IFS_RUN_STARTED,
            trial_index=trial_index,
            batch_index=batch_index,
            started_at_utc=started_at_utc,
            was_reused=False,
            resolution_note="pending",
        )
        # ----------------------------------------------------------------------
        # HUMAN-READABLE COMMENT (For Codex/GitHub Review)
        #
        # BEFORE RUNNING IFS, CHECK FOR CACHED RESULTS
        # ------------------------------------------------
        # This block implements caching: if this exact model_id was already
        # evaluated in prior model_run history,
        # we should *not* run IFs again. Instead, we immediately return the
        # stored fit_pooled value.
        #
        # Why?
        # - canonical configurations are hashed into model_id; so identical
        #   parameter/coef sets always produce identical model_ids.
        # - If ml_driver proposes a point we've already evaluated, this ensures
        #   we reuse previous results and avoid redundant heavy IFs simulations.
        # - Greatly improves speed & makes ML loop behave deterministically.
        #
        # ----------------------------------------------------------------------
        existing_status, existing_fit = _fetch_model_output_snapshot(conn, model_id=model_id)
        if existing_fit is not None:
            # Found previously evaluated model - reuse stored fit_pooled
            fit_val = existing_fit
            reused_status = cached_result_status(existing_status, fit_val)
            completed_at_utc = _utc_now_iso()
            update_model_run(
                conn,
                run_id=int(model_run_id),
                model_status=reused_status,
                completed_at_utc=completed_at_utc,
                fit_pooled=fit_val,
                was_reused=True,
                source_status=existing_status,
                resolution_note="cached_result_reused",
            )
            point = np.round(flatten_inputs(param_values, coef_values), 6)
            point_text = _format_point_for_log(point)
            status_text = reused_status or existing_status or "unknown"
            print(
                f"Reusing cached model {model_id} at {point_text} => fit_pooled={fit_val:.6f}, status={status_text}",
                flush=True,
            )
            return fit_val, model_id

    # --------------------------------------------------------------------------
    # If no cached value was found, proceed to insert this configuration and
    # prepare for a fresh IFs run.
    # --------------------------------------------------------------------------

    command = [
        sys.executable,
            str(Path(__file__).resolve().parents[1] / "run_ifs.py"),
        "--ifs-root",
        args.ifs_root,
        "--end-year",
        str(args.end_year),
        "--output-dir",
        str(args.output_folder),
        "--model-id",
        model_id,
        "--ifs-id",
        str(ifs_id),
    ]

    if args.base_year is not None:
        command.extend(["--base-year", str(args.base_year)])
    if args.start_token is not None:
        command.extend(["--start-token", str(args.start_token)])
    if args.log is not None:
        command.extend(["--log", str(args.log)])
    if args.websessionid is not None:
        command.extend(["--websessionid", str(args.websessionid)])
    if args.artifact_retention is not None:
        command.extend(["--artifact-retention", str(args.artifact_retention)])

    # Ensure python knows where the package root is
    env = os.environ.copy()
    project_root = str(Path(__file__).resolve().parent.parent)
    env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")

    process = subprocess.run(command, capture_output=False, text=True, env=env)
    if process.returncode != 0:
        with sqlite3.connect(bigpopa_db) as conn:
            ensure_current_bigpopa_schema(conn.cursor())
            status, fit_val = _fetch_model_output_snapshot(conn, model_id=model_id)
            if fit_val is None:
                fit_val = FAIL_Y
                status = IFS_RUN_FAILED
            completed_at_utc = _utc_now_iso()
            update_model_run(
                conn,
                run_id=int(model_run_id),
                model_status=status or IFS_RUN_FAILED,
                completed_at_utc=completed_at_utc,
                fit_pooled=fit_val,
                source_status=status,
                resolution_note="ifs_runtime_non_zero",
            )
        print(
            f"IFs runtime exited non-zero for model_id={model_id} (return_code={process.returncode}); "
            f"using recorded fit_pooled={fit_val:.6f}, status={status or IFS_RUN_FAILED}",
            flush=True,
        )
        return fit_val, model_id

    with sqlite3.connect(bigpopa_db) as conn:
        ensure_current_bigpopa_schema(conn.cursor())
        status, fit_val = _fetch_model_output_snapshot(conn, model_id=model_id)
        if fit_val is None:
            raise RuntimeError("fit_pooled not found after IFs run")
        completed_at_utc = _utc_now_iso()
        update_model_run(
            conn,
            run_id=int(model_run_id),
            model_status=status or FIT_EVALUATED,
            completed_at_utc=completed_at_utc,
            fit_pooled=fit_val,
            source_status=status,
            resolution_note="fresh_evaluation_completed",
        )

    point = np.round(flatten_inputs(param_values, coef_values), 6)
    point_text = _format_point_for_log(point)
    print(
        f"Finished model {model_id} at {point_text} => fit_pooled={fit_val:.6f}, status={status or FIT_EVALUATED}",
        flush=True,
    )
    return fit_val, model_id


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    parser = argparse.ArgumentParser(description="Active learning driver for BIGPOPA")
    parser.add_argument("--ifs-root", required=True, help="Path to IFs root")
    parser.add_argument("--end-year", required=True, type=int, help="Simulation end year")
    parser.add_argument(
        "--output-folder",
        "--output-dir",
        dest="output_folder",
        required=True,
        help="Folder containing bigpopa.db and run artifacts",
    )
    parser.add_argument("--bigpopa-db", help="Explicit path to bigpopa.db")
    parser.add_argument("--base-year", type=int, default=None)
    parser.add_argument("--start-token", default="5")
    parser.add_argument("--log", default="jrs.txt")
    parser.add_argument("--websessionid", default="qsdqsqsdqsdqsdqs")
    parser.add_argument(
        "--artifact-retention",
        dest="artifact_retention",
        default=RETENTION_NONE,
        help="Artifact retention mode: none, best_only, or all",
    )
    parser.add_argument(
        "--initial-model-id",
        required=True,
        help="Model ID of the initial seed configuration to start the ML driver",
    )
    parser.add_argument(
        "--input-profile-id",
        dest="input_profile_id",
        required=True,
        type=int,
        help="Input profile identifier stored in bigpopa.db",
    )
    parser.add_argument(
        "--stop-file",
        dest="stop_file",
        help="Path to a sentinel file requesting graceful stop after the current run",
    )
    parser.add_argument(
        "--random-seed",
        dest="random_seed",
        type=int,
        default=None,
        help="Optional seed controlling candidate-pool generation for reproducible runs",
    )

    args = parser.parse_args(argv)
    args.artifact_retention = normalize_artifact_retention_mode(args.artifact_retention)
    args.output_folder = os.path.abspath(args.output_folder)
    args.ifs_root = os.path.abspath(args.ifs_root)
    stop_file = Path(args.stop_file).expanduser().resolve() if args.stop_file else None

    bigpopa_db = Path(args.bigpopa_db) if args.bigpopa_db else Path(args.output_folder) / "bigpopa.db"

    emit_stage_response(
        "info",
        "ml_driver",
        "Starting ML driver and loading latest configuration.",
        {"bigpopa_db": str(bigpopa_db)},
    )

    try:
        conn = sqlite3.connect(str(bigpopa_db))
    except sqlite3.Error as exc:  # pragma: no cover - runtime guard
        emit_stage_response("error", "ml_driver", "Unable to open bigpopa.db", {"error": str(exc)})
        return 1

    with conn:
        ensure_bigpopa_schema(conn.cursor())
        dataset_id_supported = _model_input_has_dataset_id(conn)
        (
            ifs_id,
            initial_model_id,
            input_param,
            input_coef,
            output_set,
            dataset_id,
        ) = _load_model_by_id(conn, dataset_id_supported, args.initial_model_id)
        if dataset_id is None:
            raise RuntimeError("dataset_id is missing from the selected stored model definition")
        ifs_static_id, stored_base_year = _get_ifs_static_id(conn, ifs_id)
        if args.base_year is None:
            args.base_year = stored_base_year

    if ifs_static_id is None:
        emit_stage_response(
            "error",
            "ml_driver",
            "Unable to resolve ifs_static_id for range construction.",
            {"ifs_id": ifs_id},
        )
        return 1

    try:
        resolved_profile = resolve_profile(
            output_folder=args.output_folder,
            profile_id=args.input_profile_id,
            ifs_root=args.ifs_root,
        )
    except Exception as exc:
        emit_stage_response(
            "error",
            "ml_driver",
            str(exc),
            {"input_profile_id": args.input_profile_id},
        )
        return 1

    if int(resolved_profile.ifs_static_id) != int(ifs_static_id):
        emit_stage_response(
            "error",
            "ml_driver",
            "Selected profile does not match the active IFs static layer.",
            {
                "input_profile_id": args.input_profile_id,
                "profile_ifs_static_id": resolved_profile.ifs_static_id,
                "model_ifs_static_id": ifs_static_id,
            },
        )
        return 1

    try:
        memory_budget_bytes = _memory_budget_bytes()
        ml_method_config = resolved_profile.ml_settings.ml_method
        n_sample = resolved_profile.ml_settings.n_sample
        n_max_iteration = resolved_profile.ml_settings.n_max_iteration
        n_convergence = resolved_profile.ml_settings.n_convergence
        min_convergence_pct = resolved_profile.ml_settings.min_convergence_pct
        user_param_configs = _profile_config_to_user_config(
            resolved_profile.parameter_configs
        )
        user_coef_configs = _profile_config_to_user_config(
            resolved_profile.coefficient_configs
        )

        samples = dataset_utils.load_compatible_training_samples(
            str(bigpopa_db), (), dataset_id
        )

        param_template = input_param
        coef_template = input_coef

        X_obs: list[np.ndarray] = []
        Y_obs: list[float] = []
        vector_to_model_id: dict[Tuple[float, ...], str] = {}

        for sample in samples:
            fit_val = sample.get("fit_pooled")
            if fit_val is None:
                continue
            vec = flatten_inputs(sample.get("input_param", {}), sample.get("input_coef", {}))
            X_obs.append(vec)
            Y_obs.append(float(fit_val))
            vector_to_model_id[tuple(np.round(vec, 6))] = sample["model_id"]

        initial_vec = flatten_inputs(param_template, coef_template)
        vector_to_model_id.setdefault(tuple(np.round(initial_vec, 6)), initial_model_id)

        search_space = _build_search_space(
            conn,
            ifs_static_id,
            param_template,
            coef_template,
            user_param_configs,
            user_coef_configs,
        )
        proposal_mode = DEFAULT_PROPOSAL_MODE
        settings_signature, settings_payload = _build_resume_settings_payload(
            ml_method_config=ml_method_config,
            n_sample=n_sample,
            n_convergence=n_convergence,
            min_convergence_pct=min_convergence_pct,
            proposal_mode=proposal_mode,
            explicit_random_seed=args.random_seed,
            search_space=search_space,
        )
        (
            existing_resume_state,
            proposal_seed,
            iteration_offset,
            initial_no_improve_counter,
            resume_mode,
        ) = _resolve_resume_behavior(
            conn,
            dataset_id=dataset_id,
            base_year=args.base_year,
            end_year=args.end_year,
            settings_signature=settings_signature,
            settings_payload=settings_payload,
            explicit_random_seed=args.random_seed,
        )
        print(
            (
                "Using ML method from input profile: "
                f"profile='{ml_method_config.raw_value}', "
                f"normalized='{ml_method_config.normalized_value}', "
                f"runtime_model='{ml_method_config.model_type}', "
                f"proposal_seed={proposal_seed}, "
                f"resume_mode='{resume_mode}', "
                f"iteration_offset={iteration_offset}, "
                f"memory_budget_mb={memory_budget_bytes / 1024 / 1024:.1f}"
            ),
            flush=True,
        )
        candidate_generator = _build_proposal_generator(
            search_space=search_space,
            n_samples=n_sample,
            run_seed=proposal_seed,
            memory_budget_bytes=memory_budget_bytes,
            proposal_mode=proposal_mode,
        )
        x_scaler = _build_bounds_scaler(search_space)
        y_transformer = _build_target_transform()
        proposal_penalty_fn = None
        if ENABLE_DEFAULT_DISTANCE_PENALTY:
            proposal_penalty_fn = _build_default_distance_penalty(
                search_space=search_space,
                x_scaler=x_scaler,
                strength=DEFAULT_DISTANCE_PENALTY_STRENGTH,
            )
        validate_surrogate_memory(
            n_observations=max(len(X_obs), 1),
            n_candidates=max(n_sample, 1),
            n_dimensions=len(search_space),
            model_type=ml_method_config.model_type,
            memory_budget_bytes=memory_budget_bytes,
        )
        prediction_chunk_size = estimate_prediction_chunk_size(
            n_dimensions=len(search_space),
            model_type=ml_method_config.model_type,
            memory_budget_bytes=memory_budget_bytes,
        )

        resume_state_snapshot = {
            "effective_iteration_count": iteration_offset,
            "no_improve_counter": initial_no_improve_counter,
            "best_y_prev": float(np.min(np.asarray(Y_obs, dtype=float)))
            if Y_obs
            else (existing_resume_state.best_y_prev if existing_resume_state is not None else None),
        }

        trial_counter = {"value": 0}

        def callback(x_vector: np.ndarray | float):
            trial_counter["value"] += 1
            trial_index = trial_counter["value"]
            param_values, coef_values = unflatten_vector(x_vector, param_template, coef_template)
            fit_val, model_id = _run_model(
                args=args,
                param_values=param_values,
                coef_values=coef_values,
                output_set=output_set,
                ifs_id=ifs_id,
                dataset_id=dataset_id,
                bigpopa_db=bigpopa_db,
                dataset_id_supported=dataset_id_supported,
                trial_index=trial_index,
                batch_index=1,
            )
            vector_to_model_id[tuple(np.round(np.atleast_1d(x_vector), 6))] = model_id
            return fit_val

        X_obs_arr, Y_obs_arr, history, results_cache, stop_honored = active_learning_loop(
            f=callback,
            X_obs=np.asarray(X_obs),
            Y_obs=np.asarray(Y_obs),
            X_grid=None,
            n_iters=n_max_iteration,
            model_type=ml_method_config.model_type,
            bootstrap=True,
            patience=n_convergence,
            min_improve_pct=min_convergence_pct,
            prediction_chunk_size=prediction_chunk_size,
            memory_budget_bytes=memory_budget_bytes,
            iteration_offset=iteration_offset,
            initial_no_improve_counter=initial_no_improve_counter,
            on_state_update=resume_state_snapshot.update,
            should_stop=lambda: stop_requested(stop_file),
            candidate_generator=candidate_generator,
            candidate_refresh_interval=DEFAULT_CANDIDATE_REFRESH_INTERVAL,
            max_pool_regenerations=DEFAULT_MAX_POOL_REGENERATIONS,
            x_scaler=x_scaler,
            y_transformer=y_transformer,
            proposal_penalty_fn=proposal_penalty_fn,
        )

        _persist_resume_state(
            conn,
            cohort_key=_build_resume_cohort_key(
                dataset_id=dataset_id,
                base_year=args.base_year,
                end_year=args.end_year,
            ),
            dataset_id=dataset_id,
            base_year=args.base_year,
            end_year=args.end_year,
            settings_signature=settings_signature,
            settings_payload=settings_payload,
            proposal_seed=proposal_seed,
            effective_iteration_count=resume_state_snapshot["effective_iteration_count"],
            no_improve_counter=resume_state_snapshot["no_improve_counter"],
            best_y_prev=resume_state_snapshot["best_y_prev"],
        )

        best_index = int(np.argmin(Y_obs_arr))
        best_vector = tuple(np.round(np.atleast_1d(X_obs_arr[best_index]), 6))
        best_fit = float(Y_obs_arr[best_index])
        best_model_id = vector_to_model_id.get(best_vector)
        termination_reason = "stopped_gracefully" if stop_honored else "completed"
        completion_message = (
            "Active learning stopped after the current run."
            if stop_honored
            else "Active learning complete."
        )

        emit_stage_response(
            "success",
            "ml_driver",
            completion_message,
            {
                "best_model_id": best_model_id,
                "best_fit_pooled": best_fit,
                "iterations": len(history),
                "terminationReason": termination_reason,
                "dataset_id": dataset_id,
                "base_year": args.base_year,
                "end_year": args.end_year,
            },
        )
        return 0
    except Exception as exc:  # noqa: BLE001
        emit_stage_response("error", "ml_driver", str(exc), {})
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
