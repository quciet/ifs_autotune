"""Utilities for training and evaluating surrogate model ensembles."""

from __future__ import annotations

import numpy as np

from .surrogate_models import NNSurrogate, PolynomialSurrogate, TreeSurrogate


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
    model_type: str = "poly",
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
