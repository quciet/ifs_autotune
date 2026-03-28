"""Active learning loop orchestrating candidate selection and evaluation."""

import numpy as np

from .acquisition_functions import expected_improvement, lcb
from .ensemble_training import ensemble_predict, train_ensemble
from .surrogate_models import BoundsScaler, LogClippedTargetTransform


def _format_candidate(x: np.ndarray) -> str:
    arr = np.atleast_1d(np.asarray(x, dtype=float))
    if arr.size == 1:
        return f"{arr.item():.4f}"
    return np.array2string(arr, precision=4, separator=", ")


def format_percent_adaptive(pct: float | None) -> str:
    """
    Format percent values adaptively to avoid misleading rounding like 0.00%.
    pct is already in percent units (e.g., 0.001 means 0.001%).
    """
    if pct is None:
        return "N/A"
    if pct == 0:
        return "0%"

    a = abs(pct)

    if a >= 1:
        return f"{pct:.2f}%"
    if a >= 0.01:
        return f"{pct:.3f}%"
    if a >= 0.0001:
        return f"{pct:.4f}%"
    return f"{pct:.2e}%"


def active_learning_loop(
    f,
    X_obs,
    Y_obs,
    X_grid=None,
    n_iters: int = 30,
    M: int = 8,
    degree: int = 5,
    model_type: str | None = None,
    bootstrap: bool = True,
    kappa_start: float = 1.6,
    kappa_end: float = 0.8,
    acquisition: str = "LCB",
    patience: int = 10,
    min_improve_pct: float | None = 0.01,
    nn_config: dict | None = None,
    prediction_chunk_size: int = 2048,
    memory_budget_bytes: int | None = None,
    should_stop=None,
    candidate_generator=None,
    candidate_refresh_interval: int = 1,
    max_pool_regenerations: int = 3,
    x_scaler: BoundsScaler | None = None,
    y_transformer: LogClippedTargetTransform | None = None,
    proposal_penalty_fn=None,
):
    """Active-learning optimization loop using ensemble-based surrogates."""
    if not model_type:
        raise ValueError("model_type must be provided explicitly for active learning.")

    X_obs = np.array(X_obs, dtype=float, copy=True)
    if X_obs.ndim == 1:
        X_obs = X_obs.reshape(-1, 1)
    else:
        X_obs = np.atleast_2d(X_obs)

    Y_obs = np.asarray(Y_obs, dtype=float).reshape(-1)

    if X_grid is None and candidate_generator is None:
        raise ValueError("Either X_grid or candidate_generator must be provided.")
    X_grid = _coerce_candidate_pool(X_grid) if X_grid is not None else None

    results_cache = {
        tuple(np.round(np.atleast_1d(x), 6)): float(y) for x, y in zip(X_obs, Y_obs)
    }
    kappas = np.linspace(kappa_start, kappa_end, n_iters)
    history = []
    no_improve_counter = 0
    stop_honored = False

    best_y_prev = float(np.min(Y_obs))
    ml_prefix = "[ML-STATUS] "
    print(
        f"{ml_prefix}Starting active learning run - initial best_y = {best_y_prev:.4f}",
        flush=True,
    )
    details = [
        f"{ml_prefix}Surrogate='{model_type}'",
        f"prediction_chunk_size={prediction_chunk_size}",
    ]
    if memory_budget_bytes is not None:
        details.append(f"memory_budget_mb={memory_budget_bytes / 1024 / 1024:.1f}")
    print(", ".join(details), flush=True)

    for t in range(n_iters):
        if should_stop is not None and should_stop():
            print(
                f"{ml_prefix}Graceful stop acknowledged; stopping before the next candidate.",
                flush=True,
            )
            stop_honored = True
            break

        models = train_ensemble(
            X_obs,
            Y_obs,
            M=M,
            degree=degree,
            bootstrap=bootstrap,
            model_type=model_type,
            nn_config=nn_config,
            x_scaler=x_scaler,
            y_transformer=y_transformer,
        )
        y_best = np.min(Y_obs)

        if candidate_generator is not None and (
            X_grid is None or candidate_refresh_interval <= 1 or t % candidate_refresh_interval == 0
        ):
            X_grid = _coerce_candidate_pool(
                candidate_generator(
                    X_obs=X_obs,
                    Y_obs=Y_obs,
                    iteration=t,
                    refresh_attempt=0,
                )
            )
        if X_grid is None:
            raise RuntimeError("Candidate generator returned no proposal pool.")

        best_index = _select_candidate_index(
            models=models,
            X_grid=X_grid,
            results_cache=results_cache,
            acquisition=acquisition,
            y_best=float(y_best),
            kappa=float(kappas[t]),
            chunk_size=max(1, prediction_chunk_size),
            proposal_penalty_fn=proposal_penalty_fn,
        )
        if best_index is None and candidate_generator is not None:
            for regeneration_attempt in range(1, max_pool_regenerations + 1):
                print(
                    f"{ml_prefix}Candidate pool exhausted; regenerating random pool "
                    f"({regeneration_attempt}/{max_pool_regenerations}).",
                    flush=True,
                )
                X_grid = _coerce_candidate_pool(
                    candidate_generator(
                        X_obs=X_obs,
                        Y_obs=Y_obs,
                        iteration=t,
                        refresh_attempt=regeneration_attempt,
                    )
                )
                best_index = _select_candidate_index(
                    models=models,
                    X_grid=X_grid,
                    results_cache=results_cache,
                    acquisition=acquisition,
                    y_best=float(y_best),
                    kappa=float(kappas[t]),
                    chunk_size=max(1, prediction_chunk_size),
                    proposal_penalty_fn=proposal_penalty_fn,
                )
                if best_index is not None:
                    break
        x_next = None
        x_next_array = None
        if best_index is not None:
            candidate = np.atleast_1d(np.asarray(X_grid[best_index], dtype=float))
            x_next = candidate.item() if candidate.size == 1 else candidate
            x_next_array = candidate
        if x_next_array is None:
            print(
                f"{ml_prefix}All candidates evaluated after pool regeneration attempts. "
                "Stopping early.",
                flush=True,
            )
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
            f"{ml_prefix}[{t+1:03d}/{n_iters}] "
            f"y_next={y_next:.4f} ({'reused' if reused else 'new'}), "
            f"best_y={best_y_curr:.4f}",
            flush=True,
        )

        if should_stop is not None and should_stop():
            print(
                f"{ml_prefix}Graceful stop acknowledged; stopping after the current evaluation.",
                flush=True,
            )
            stop_honored = True
            break

        if min_improve_pct is not None:
            rel_change = abs(best_y_curr - best_y_prev) / (abs(best_y_prev) + 1e-8)
            if rel_change < min_improve_pct:
                no_improve_counter += 1
            else:
                no_improve_counter = 0
            if no_improve_counter >= patience:
                print(
                    f"{ml_prefix}Stopping early: no improvement > "
                    f"{format_percent_adaptive(min_improve_pct * 100)} for "
                    f"{patience} consecutive iterations.",
                    flush=True,
                )
                break
        best_y_prev = best_y_curr

    return X_obs, Y_obs, np.array(history, dtype=object), results_cache, stop_honored


def _coerce_candidate_pool(X_grid) -> np.ndarray:
    grid = np.asarray(X_grid, dtype=float)
    if grid.ndim == 1:
        return grid.reshape(-1, 1)
    return np.atleast_2d(grid)


def _select_candidate_index(
    *,
    models,
    X_grid: np.ndarray,
    results_cache: dict[tuple[float, ...], float],
    acquisition: str,
    y_best: float,
    kappa: float,
    chunk_size: int,
    proposal_penalty_fn=None,
) -> int | None:
    best_index: int | None = None
    best_score: float | None = None
    acquisition_name = acquisition.upper()

    for start in range(0, len(X_grid), chunk_size):
        stop = min(start + chunk_size, len(X_grid))
        chunk = X_grid[start:stop]
        mu, sigma = ensemble_predict(models, chunk)
        penalties = None
        if proposal_penalty_fn is not None:
            penalties = np.asarray(proposal_penalty_fn(chunk), dtype=float).reshape(-1)
            if len(penalties) != len(chunk):
                raise ValueError("proposal_penalty_fn must return one penalty per candidate.")
        if acquisition_name == "LCB":
            acq = lcb(mu, sigma, kappa=kappa)
            adjusted = acq if penalties is None else acq + penalties
            order = np.argsort(adjusted)
        elif acquisition_name == "EI":
            acq = expected_improvement(mu, sigma, y_best)
            adjusted = acq if penalties is None else acq - penalties
            order = np.argsort(-adjusted)
        else:
            raise ValueError(f"Unknown acquisition: {acquisition}")

        for local_index in order:
            candidate = np.atleast_1d(np.asarray(chunk[local_index], dtype=float))
            key = tuple(np.round(candidate, 6))
            if key in results_cache:
                continue

            global_index = start + int(local_index)
            score = float(adjusted[local_index])
            if best_index is None:
                best_index = global_index
                best_score = score
                break

            if acquisition_name == "LCB":
                if score < float(best_score) or (
                    np.isclose(score, float(best_score)) and global_index < best_index
                ):
                    best_index = global_index
                    best_score = score
            else:
                if score > float(best_score) or (
                    np.isclose(score, float(best_score)) and global_index < best_index
                ):
                    best_index = global_index
                    best_score = score
            break

    return best_index
