"""Active-learning orchestration layer for BIGPOPA.

This module sits between the Electron shell and the legacy ``run_ifs.py``
runner. It coordinates the optimization loop and delegates all IFs execution
and extraction work to the existing scripts without modifying their behavior.
Electron should trigger this entry point instead of calling ``run_ifs.py``
directly.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent))
sys.path.append(str(Path(__file__).resolve().parent.parent))

from backend import dataset_utils
from backend.model_setup import canonical_config, hash_model_id
from backend.optimization.active_learning import active_learning_loop


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
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(model_input)")
    return any(row[1] == "dataset_id" for row in cursor.fetchall())


def _load_model_by_id(
    conn: sqlite3.Connection, has_dataset_id: bool, model_id: str
) -> Tuple[int, str, dict, dict, dict, str | None]:
    cursor = conn.cursor()
    select_clause = "ifs_id, model_id, input_param, input_coef, output_set"
    if has_dataset_id:
        select_clause += ", dataset_id"
    cursor.execute(
        f"SELECT {select_clause} FROM model_input WHERE model_id = ? LIMIT 1", (model_id,)
    )
    row = cursor.fetchone()
    if not row:
        raise RuntimeError(
            "No model_input rows found for the provided model_id; cannot start ML driver."
        )

    if has_dataset_id:
        ifs_id, model_id, ip_raw, ic_raw, os_raw, dataset_id = row
    else:
        ifs_id, model_id, ip_raw, ic_raw, os_raw = row
        dataset_id = None
    return (
        int(ifs_id),
        str(model_id),
        json.loads(ip_raw),
        json.loads(ic_raw),
        json.loads(os_raw),
        dataset_id,   # keep dataset_id as string
    )


def _get_ifs_static_id(conn: sqlite3.Connection, ifs_id: int) -> Tuple[int | None, int | None]:
    cursor = conn.cursor()
    cursor.execute("SELECT ifs_static_id, base_year FROM ifs_version WHERE ifs_id = ? LIMIT 1", (ifs_id,))
    row = cursor.fetchone()
    if row:
        return int(row[0]) if row[0] is not None else None, int(row[1]) if row[1] is not None else None
    return None, None


def _merge_with_template(template_param: dict, template_coef: dict, sample_param: dict, sample_coef: dict):
    params = {key: sample_param.get(key, template_param[key]) for key in template_param.keys()}

    coef_result: dict = {}
    for func, x_map in template_coef.items():
        coef_result[func] = {}
        sample_func = sample_coef.get(func, {})
        for x_name, beta_map in x_map.items():
            coef_result[func][x_name] = {}
            sample_beta = sample_func.get(x_name, {})
            for beta, default_val in beta_map.items():
                coef_result[func][x_name][beta] = sample_beta.get(beta, default_val)
    return params, coef_result


def _build_search_ranges(
    conn: sqlite3.Connection,
    ifs_static_id: int,
    input_param: dict,
    input_coef: dict,
    user_param_bounds: dict[str, tuple[float | None, float | None]],
    user_coef_bounds: dict[tuple[str, str, str], tuple[float | None, float | None]],
) -> List[Tuple[float, float]]:
    cursor = conn.cursor()
    ranges: List[Tuple[float, float]] = []

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

        user_min, user_max = user_param_bounds.get(param_name, (None, None))
        min_val = float(user_min) if user_min is not None else (
            float(param_min) if param_min is not None else float(fallback_min)
        )
        max_val = float(user_max) if user_max is not None else (
            float(param_max) if param_max is not None else float(fallback_max)
        )
        ranges.append((min_val, max_val))

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

                user_min, user_max = user_coef_bounds.get((func, x_name, beta), (None, None))
                excel_fully_specified = user_min is not None and user_max is not None

                min_val = float(user_min) if user_min is not None else (
                    float(db_min) if db_min is not None else float(fallback_min)
                )
                max_val = float(user_max) if user_max is not None else (
                    float(db_max) if db_max is not None else float(fallback_max)
                )

                if beta_default is not None and not excel_fully_specified:
                    if beta_default > 0:
                        min_val = max(min_val, 0.0)
                    elif beta_default < 0:
                        max_val = min(max_val, 0.0)

                ranges.append((float(min_val), float(max_val)))
    return ranges


def _sample_grid(ranges: List[Tuple[float, float]], n_samples: int = 200) -> np.ndarray:
    rng = np.random.default_rng(0)
    samples = []
    for _ in range(n_samples):
        point = [rng.uniform(low, high) if low != high else low for low, high in ranges]
        samples.append(point)
    return np.asarray(samples, dtype=float)


def _switch_is_on(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() == "on"
    try:
        return float(value) == 1.0
    except (TypeError, ValueError):
        return False


def _to_optional_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _load_user_bounds(
    starting_point_table: Path,
) -> tuple[dict[str, tuple[float | None, float | None]], dict[tuple[str, str, str], tuple[float | None, float | None]]]:
    param_bounds: dict[str, tuple[float | None, float | None]] = {}
    coef_bounds: dict[tuple[str, str, str], tuple[float | None, float | None]] = {}

    if not starting_point_table.exists():
        return param_bounds, coef_bounds

    def _read_sheet(sheet_name: str) -> pd.DataFrame:
        try:
            return pd.read_excel(starting_point_table, sheet_name=sheet_name, engine="openpyxl")
        except Exception:
            return pd.DataFrame()

    for _, row in _read_sheet("IfsVar").iterrows():
        if not _switch_is_on(row.get("Switch")):
            continue
        name = str(row.get("Name") or "").strip()
        if not name:
            continue
        param_bounds[name] = (
            _to_optional_float(row.get("Minimum")),
            _to_optional_float(row.get("Maximum")),
        )

    for sheet_name in ("TablFunc", "AnalFunc"):
        for _, row in _read_sheet(sheet_name).iterrows():
            if not _switch_is_on(row.get("Switch")):
                continue
            function_name = str(row.get("Function Name") or "").strip()
            x_name = str(row.get("XVariable") or "").strip()
            beta_name = str(row.get("Coefficient") or "").strip()
            if not function_name or not x_name or not beta_name:
                continue
            coef_bounds[(function_name, x_name, beta_name)] = (
                _to_optional_float(row.get("Minimum")),
                _to_optional_float(row.get("Maximum")),
            )

    return param_bounds, coef_bounds


def _load_ml_settings(starting_point_table: Path):
    """
    Read ML tab from StartingPointTable.xlsx.

    Supported Excel parameters:
      n_sample = integer
      n_max_iteration = integer
      n_convergence = integer (patience)
      min_convergence_pct = percent number (0.01 = 0.01%)

    min_convergence_pct is converted to a fraction for active_learning_loop:
      Example: Excel 0.01 → 0.01% → 0.0001
    """
    default_n_sample = 200
    default_n_max_iteration = 30
    default_n_convergence = 10
    default_min_convergence_pct = 0.01 / 100.0  # default = 0.01% = 0.0001

    n_sample = default_n_sample
    n_max_iteration = default_n_max_iteration
    n_convergence = default_n_convergence
    min_convergence_pct = default_min_convergence_pct

    if not starting_point_table.exists():
        return (
            n_sample,
            n_max_iteration,
            n_convergence,
            min_convergence_pct,
        )

    try:
        df = pd.read_excel(starting_point_table, sheet_name="ML", engine="openpyxl")
    except Exception:
        return (
            n_sample,
            n_max_iteration,
            n_convergence,
            min_convergence_pct,
        )

    for _, row in df.iterrows():
        method = str(row.get("Method") or "").strip().lower()
        if method != "general":
            continue

        parameter = str(row.get("Parameter") or "").strip().lower()
        value = row.get("Value")

        # Cast as float first
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            continue

        if parameter == "n_sample":
            n_sample = int(numeric_value)

        elif parameter == "n_max_iteration":
            n_max_iteration = int(numeric_value)

        elif parameter == "n_convergence":
            n_convergence = int(numeric_value)

        elif parameter == "min_convergence_pct":
            # User enters percentages (0.01 = 0.01%).
            # Convert percent → fraction for ML loop.
            min_convergence_pct = float(numeric_value) / 100.0

    return (
        n_sample,
        n_max_iteration,
        n_convergence,
        min_convergence_pct,
    )


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
) -> Tuple[float, str]:
    canonical = canonical_config(ifs_id, param_values, coef_values, output_set)
    model_id = hash_model_id(canonical)

    with sqlite3.connect(bigpopa_db) as conn:
        # ----------------------------------------------------------------------
        # HUMAN-READABLE COMMENT (For Codex/GitHub Review)
        #
        # BEFORE RUNNING IFS, CHECK FOR CACHED RESULTS
        # ------------------------------------------------
        # This block implements caching: if this exact model_id was already
        # evaluated in the past (i.e., model_output contains fit_pooled),
        # we should *not* run IFs again. Instead, we immediately return the
        # stored fit_pooled value.
        #
        # Why?
        # - model_input entries are hashed by canonical_config; so identical
        #   parameter/coef sets always produce identical model_ids.
        # - If ml_driver proposes a point we've already evaluated, this ensures
        #   we reuse previous results and avoid redundant heavy IFs simulations.
        # - Greatly improves speed & makes ML loop behave deterministically.
        #
        # ----------------------------------------------------------------------
        cur = conn.cursor()
        cur.execute(
            "SELECT fit_pooled FROM model_output WHERE model_id = ? LIMIT 1",
            (model_id,),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            # Found previously evaluated model — reuse stored fit_pooled
            fit_val = float(row[0])
            point = np.round(flatten_inputs(param_values, coef_values), 6)
            point_text = _format_point_for_log(point)
            print(
                f"Reusing evaluated model {model_id} at {point_text} => fit_pooled={fit_val:.6f}",
                flush=True,
            )
            return fit_val, model_id

    # --------------------------------------------------------------------------
    # If no cached value was found, proceed to insert this configuration and
    # prepare for a fresh IFs run.
    # --------------------------------------------------------------------------

    with sqlite3.connect(bigpopa_db) as conn:
        cursor = conn.cursor()
        if not dataset_id_supported:
            raise RuntimeError("model_input.dataset_id column is required for ML runs")
        cursor.execute(
            """
            INSERT INTO model_input (ifs_id, model_id, dataset_id, input_param, input_coef, output_set)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(model_id) DO NOTHING
            """,
            (
                ifs_id,
                model_id,
                dataset_id,
                json.dumps(canonical["input_param"]),
                json.dumps(canonical["input_coef"]),
                json.dumps(canonical["output_set"]),
            ),
        )

    command = [
        sys.executable,
        str(Path(__file__).resolve().parent / "run_ifs.py"),
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

    # Ensure python knows where the package root is
    env = os.environ.copy()
    project_root = str(Path(__file__).resolve().parent.parent)
    env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")

    process = subprocess.run(command, capture_output=False, text=True, env=env)
    if process.returncode != 0:
        raise RuntimeError(f"run_ifs.py failed with exit code {process.returncode}")

    with sqlite3.connect(bigpopa_db) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT fit_pooled FROM model_output WHERE model_id = ? LIMIT 1", (model_id,)
        )
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("fit_pooled not found after IFs run")
        if row[0] is None:
            raise RuntimeError("fit_pooled is NULL after IFs run")
        fit_val = float(row[0])

    point = np.round(flatten_inputs(param_values, coef_values), 6)
    point_text = _format_point_for_log(point)
    print(
        f"Evaluated model {model_id} at {point_text} => fit_pooled={fit_val:.6f}",
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
        "--initial-model-id",
        required=True,
        help="Model ID of the initial model_input row to seed the ML driver",
    )
    parser.add_argument(
        "--starting-point-table",
        dest="starting_point_table",
        help="Path to the user-provided StartingPointTable.xlsx",
    )

    args = parser.parse_args(argv)
    args.output_folder = os.path.abspath(args.output_folder)
    args.ifs_root = os.path.abspath(args.ifs_root)

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
        dataset_id_supported = _model_input_has_dataset_id(conn)
        (
            ifs_id,
            initial_model_id,
            input_param,
            input_coef,
            output_set,
            dataset_id,
        ) = _load_model_by_id(conn, dataset_id_supported, args.initial_model_id)
        if dataset_id_supported and dataset_id is None:
            raise RuntimeError("dataset_id is missing from the selected model_input entry")
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

    output_starting_point_table = (Path(args.output_folder) / "StartingPointTable.xlsx").resolve()
    starting_point_table = output_starting_point_table
    if args.starting_point_table:
        provided_starting_point = Path(args.starting_point_table).expanduser().resolve()
        if provided_starting_point.is_file():
            starting_point_table = provided_starting_point
        else:
            emit_stage_response(
                "error",
                "ml_driver",
                "Provided StartingPointTable.xlsx could not be read; falling back to output folder copy.",
                {
                    "provided_path": str(provided_starting_point),
                    "fallback_path": str(output_starting_point_table),
                },
            )

    (
        n_sample,
        n_max_iteration,
        n_convergence,
        min_convergence_pct,
    ) = _load_ml_settings(starting_point_table)
    user_param_bounds, user_coef_bounds = _load_user_bounds(starting_point_table)

    try:
        structure = dataset_utils.extract_structure_keys(input_param, input_coef, output_set)
        samples = dataset_utils.load_compatible_training_samples(
            str(bigpopa_db), structure, dataset_id
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
            merged_param, merged_coef = _merge_with_template(
                param_template, coef_template, sample.get("input_param", {}), sample.get("input_coef", {})
            )
            vec = flatten_inputs(merged_param, merged_coef)
            X_obs.append(vec)
            Y_obs.append(float(fit_val))
            vector_to_model_id[tuple(np.round(vec, 6))] = sample["model_id"]

        initial_vec = flatten_inputs(param_template, coef_template)
        vector_to_model_id.setdefault(tuple(np.round(initial_vec, 6)), initial_model_id)

        ranges = _build_search_ranges(
            conn,
            ifs_static_id,
            param_template,
            coef_template,
            user_param_bounds,
            user_coef_bounds,
        )
        X_grid = _sample_grid(ranges, n_samples=n_sample)

        def callback(x_vector: np.ndarray | float):
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
            )
            vector_to_model_id[tuple(np.round(np.atleast_1d(x_vector), 6))] = model_id
            return fit_val

        X_obs_arr, Y_obs_arr, history, results_cache = active_learning_loop(
            f=callback,
            X_obs=np.asarray(X_obs),
            Y_obs=np.asarray(Y_obs),
            X_grid=X_grid,
            n_iters=n_max_iteration,
            patience=n_convergence,              # new
            min_improve_pct=min_convergence_pct, # new (fraction, not percent)
        )

        best_index = int(np.argmin(Y_obs_arr))
        best_vector = tuple(np.round(np.atleast_1d(X_obs_arr[best_index]), 6))
        best_fit = float(Y_obs_arr[best_index])
        best_model_id = vector_to_model_id.get(best_vector)

        emit_stage_response(
            "success",
            "ml_driver",
            "Active learning complete.",
            {
                "best_model_id": best_model_id,
                "best_fit_pooled": best_fit,
                "iterations": len(history),
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
