"""Active learning loop orchestrating candidate selection and evaluation."""

import numpy as np

from .acquisition_functions import expected_improvement, lcb
from .ensemble_training import ensemble_predict, train_ensemble


def _format_candidate(x: np.ndarray) -> str:
    arr = np.atleast_1d(np.asarray(x, dtype=float))
    if arr.size == 1:
        return f"{arr.item():.4f}"
    return np.array2string(arr, precision=4, separator=", ")


def active_learning_loop(
    f,
    X_obs,
    Y_obs,
    X_grid,
    n_iters: int = 30,
    M: int = 8,
    degree: int = 5,
    model_type: str = "poly",
    bootstrap: bool = False,
    kappa_start: float = 1.6,
    kappa_end: float = 0.8,
    acquisition: str = "LCB",
    patience: int = 10,
    min_improve_pct: float | None = 0.01,
    nn_config: dict | None = None,
):
    """Active-learning optimization loop using ensemble-based surrogates."""

    X_obs = np.array(X_obs, dtype=float, copy=True)
    if X_obs.ndim == 1:
        X_obs = X_obs.reshape(-1, 1)
    else:
        X_obs = np.atleast_2d(X_obs)

    Y_obs = np.asarray(Y_obs, dtype=float).reshape(-1)

    X_grid = np.asarray(X_grid, dtype=float)
    if X_grid.ndim == 1:
        X_grid = X_grid.reshape(-1, 1)
    else:
        X_grid = np.atleast_2d(X_grid)

    results_cache = {
        tuple(np.round(np.atleast_1d(x), 6)): float(y) for x, y in zip(X_obs, Y_obs)
    }
    kappas = np.linspace(kappa_start, kappa_end, n_iters)
    history = []
    no_improve_counter = 0

    best_y_prev = float(np.min(Y_obs))
    print(f"Starting active learning run — initial best_y = {best_y_prev:.4f}")

    for t in range(n_iters):
        models = train_ensemble(
            X_obs,
            Y_obs,
            M=M,
            degree=degree,
            bootstrap=bootstrap,
            model_type=model_type,
            nn_config=nn_config,
        )
        mu, sigma = ensemble_predict(models, X_grid)
        y_best = np.min(Y_obs)

        if acquisition.upper() == "LCB":
            acq = lcb(mu, sigma, kappa=kappas[t])
            sort_order = np.argsort(acq)
        elif acquisition.upper() == "EI":
            acq = expected_improvement(mu, sigma, y_best)
            sort_order = np.argsort(-acq)
        else:
            raise ValueError(f"Unknown acquisition: {acquisition}")

        x_next = None
        x_next_array = None
        for idx in sort_order:
            candidate = np.atleast_1d(np.asarray(X_grid[idx], dtype=float))
            key = tuple(np.round(candidate, 6))
            if key not in results_cache:
                x_next = candidate.item() if candidate.size == 1 else candidate
                x_next_array = candidate
                break
        if x_next_array is None:
            print("All candidates evaluated. Stopping early.")
            break

        key = tuple(np.round(x_next_array, 6))
        if key in results_cache:
            y_next = results_cache[key]
            reused = True
        else:
            eval_point = x_next
            y_next = f(eval_point)
            results_cache[key] = y_next
            reused = False

        X_obs = np.vstack([X_obs, x_next_array])
        Y_obs = np.append(Y_obs, y_next)
        best_y_curr = float(np.min(Y_obs))
        history.append((t, x_next, y_next, best_y_curr))

        print(
            f"[{t+1:03d}/{n_iters}] acq={acquisition}, x_next={_format_candidate(x_next_array)}, "
            f"y_next={y_next:.4f} ({'reused' if reused else 'new'}), best_y={best_y_curr:.4f}"
        )

        if min_improve_pct is not None:
            rel_change = abs(best_y_curr - best_y_prev) / (abs(best_y_prev) + 1e-8)
            if rel_change < min_improve_pct:
                no_improve_counter += 1
            else:
                no_improve_counter = 0
            if no_improve_counter >= patience:
                print(
                    "Stopping early: no improvement > "
                    f"{min_improve_pct*100:.2f}% for {patience} consecutive iterations."
                )
                break
        best_y_prev = best_y_curr

    return X_obs, Y_obs, np.array(history, dtype=object), results_cache
