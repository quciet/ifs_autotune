"""Utilities for training and evaluating surrogate model ensembles."""

from __future__ import annotations

import math

import numpy as np

from .surrogate_models import NNSurrogate, PolynomialSurrogate, TreeSurrogate


BYTES_PER_FLOAT64 = np.dtype(np.float64).itemsize


def expanded_feature_count(n_dimensions: int, model_type: str, degree: int = 5) -> int:
    if n_dimensions < 1:
        return 1
    if model_type == "poly":
        return math.comb(n_dimensions + degree, degree)
    return n_dimensions


def estimate_training_memory_bytes(
    *,
    n_observations: int,
    n_dimensions: int,
    model_type: str,
    degree: int = 5,
) -> int:
    if model_type != "poly":
        return int(max(n_observations, 1) * max(n_dimensions, 1) * BYTES_PER_FLOAT64)
    return int(
        max(n_observations, 1)
        * expanded_feature_count(max(n_dimensions, 1), model_type, degree)
        * BYTES_PER_FLOAT64
    )


def estimate_prediction_chunk_size(
    *,
    n_dimensions: int,
    model_type: str,
    memory_budget_bytes: int,
    degree: int = 5,
) -> int:
    per_row_bytes = expanded_feature_count(
        max(n_dimensions, 1), model_type, degree
    ) * BYTES_PER_FLOAT64
    if per_row_bytes <= 0:
        return 1
    return max(1, memory_budget_bytes // per_row_bytes)


def validate_surrogate_memory(
    *,
    n_observations: int,
    n_candidates: int,
    n_dimensions: int,
    model_type: str,
    memory_budget_bytes: int,
    degree: int = 5,
) -> None:
    training_bytes = estimate_training_memory_bytes(
        n_observations=n_observations,
        n_dimensions=n_dimensions,
        model_type=model_type,
        degree=degree,
    )
    if training_bytes > memory_budget_bytes:
        raise ValueError(
            f"{model_type} surrogate training requires approximately "
            f"{training_bytes / 1024 / 1024:.1f} MiB for {n_observations} observations and "
            f"{n_dimensions} dimensions, which exceeds the configured memory budget of "
            f"{memory_budget_bytes / 1024 / 1024:.1f} MiB."
        )

    if model_type == "poly":
        feature_count = expanded_feature_count(n_dimensions, model_type, degree)
        if feature_count > 250_000:
            raise ValueError(
                f"Polynomial surrogate expands to {feature_count} features for "
                f"{n_dimensions} dimensions at degree {degree}, which is not supported safely."
            )

    full_prediction_bytes = int(
        max(n_candidates, 1)
        * expanded_feature_count(max(n_dimensions, 1), model_type, degree)
        * BYTES_PER_FLOAT64
    )
    if full_prediction_bytes > memory_budget_bytes and n_candidates > 1:
        # Chunked scoring is allowed, so this only validates that the chunk size will be usable.
        chunk_size = estimate_prediction_chunk_size(
            n_dimensions=n_dimensions,
            model_type=model_type,
            memory_budget_bytes=memory_budget_bytes,
            degree=degree,
        )
        if chunk_size < 1:
            raise ValueError(
                f"{model_type} surrogate prediction cannot be chunked safely within the "
                f"configured memory budget of {memory_budget_bytes / 1024 / 1024:.1f} MiB."
            )


def _fit_polynomial(X: np.ndarray, Y: np.ndarray, degree: int) -> PolynomialSurrogate:
    X = np.atleast_2d(np.asarray(X, dtype=float))
    Y = np.asarray(Y, dtype=float)
    if len(X) < 2:
        return PolynomialSurrogate.fit(X, Y, degree=0)
    return PolynomialSurrogate.fit(X, Y, degree=degree)


def train_ensemble(
    X_obs,
    Y_obs,
    M: int = 8,
    degree: int = 5,
    bootstrap: bool = False,
    model_type: str | None = None,
    nn_config: dict | None = None,
):
    """Train an ensemble of surrogate models.

    Parameters
    ----------
    X_obs, Y_obs : array-like
        Observed coordinates and responses.
    M : int
        Number of models in the ensemble.
    degree : int
        Polynomial degree used when ``model_type='poly'``.
    bootstrap : bool
        Whether to resample observations with replacement per model.
    model_type : {'poly', 'tree', 'nn'}
        Surrogate model family to fit.
    """
    X_obs = np.asarray(X_obs, dtype=float)
    Y_obs = np.asarray(Y_obs, dtype=float)
    if not model_type:
        raise ValueError("model_type must be provided explicitly for ensemble training.")

    n = len(X_obs)
    models = []
    for _ in range(M):
        if bootstrap and n > 1:
            idx = np.random.randint(0, n, size=n)
        else:
            idx = np.arange(n)
        Xb, Yb = X_obs[idx], Y_obs[idx]

        if model_type == "poly":
            model = _fit_polynomial(Xb, Yb, degree)
        elif model_type == "tree":
            model = TreeSurrogate.fit(Xb, Yb)
        elif model_type == "nn":
            model = NNSurrogate.fit(Xb, Yb, **(nn_config or {}))
        else:
            raise ValueError(f"Unknown model_type: {model_type}")

        models.append(model)
    return models


def ensemble_predict(models, X_grid: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Evaluate each surrogate in the ensemble and return mean and standard deviation."""
    X_grid = np.asarray(X_grid, dtype=float)
    preds = np.stack([model.predict(X_grid) for model in models], axis=0)
    mu = preds.mean(axis=0)
    sigma = preds.std(axis=0, ddof=1) if len(models) > 1 else np.zeros_like(mu)
    return mu, sigma
