from __future__ import annotations
import json, hashlib, sqlite3

from runtime.model_run_store import (
    is_visible_training_sample,
    normalize_run_row,
)
from db.schema import ensure_current_bigpopa_schema


def compute_dataset_id(ifs_id: int, input_param: dict, input_coef: dict, output_set: dict) -> str:
    param_keys = sorted(input_param.keys())
    coef_keys = sorted(
        f"{func}.{x}.{beta}"
        for func, xmap in input_coef.items()
        for x, betamap in xmap.items()
        for beta in betamap.keys()
    )
    output_keys = sorted(output_set.keys())

    structure = {
        "ifs_id": int(ifs_id),
        "param_keys": param_keys,
        "coef_keys": coef_keys,
        "output_keys": output_keys,
    }
    return hashlib.sha256(json.dumps(structure, sort_keys=True).encode("utf-8")).hexdigest()


def extract_structure_keys(input_param: dict, input_coef: dict, output_set: dict):
    param = set(input_param.keys())
    coef = set(
        f"{func}.{x}.{beta}"
        for func, xmap in input_coef.items()
        for x, betamap in xmap.items()
        for beta in betamap.keys()
    )
    out = set(output_set.keys())
    return (param, coef, out)


def load_compatible_training_samples(
    db_path: str, current_structure: tuple, dataset_id: str | None
):
    del current_structure
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        ensure_current_bigpopa_schema(cur)
        if dataset_id is None:
            rows = cur.execute(
                """
                SELECT
                    run_id,
                    ifs_id,
                    model_id,
                    dataset_id,
                    input_param,
                    input_coef,
                    output_set,
                    model_status,
                    fit_var,
                    fit_pooled,
                    trial_index,
                    batch_index,
                    started_at_utc,
                    completed_at_utc,
                    was_reused,
                    source_status,
                    resolution_note
                FROM model_run
                WHERE dataset_id IS NULL
                ORDER BY
                    CASE WHEN completed_at_utc IS NULL THEN 1 ELSE 0 END,
                    completed_at_utc DESC,
                    run_id DESC
                """
            ).fetchall()
        else:
            rows = cur.execute(
                """
                SELECT
                    run_id,
                    ifs_id,
                    model_id,
                    dataset_id,
                    input_param,
                    input_coef,
                    output_set,
                    model_status,
                    fit_var,
                    fit_pooled,
                    trial_index,
                    batch_index,
                    started_at_utc,
                    completed_at_utc,
                    was_reused,
                    source_status,
                    resolution_note
                FROM model_run
                WHERE dataset_id = ?
                ORDER BY
                    CASE WHEN completed_at_utc IS NULL THEN 1 ELSE 0 END,
                    completed_at_utc DESC,
                    run_id DESC
                """,
                (dataset_id,),
            ).fetchall()

        deduped: dict[str, dict] = {}
        for raw_row in rows:
            row = normalize_run_row(raw_row)
            if not is_visible_training_sample(row):
                continue
            if row.model_id in deduped:
                continue
            deduped[row.model_id] = {
                "model_id": row.model_id,
                "input_param": row.input_param,
                "input_coef": row.input_coef,
                "output_set": row.output_set,
                "fit_pooled": row.fit_pooled,
            }
        return list(deduped.values())
    finally:
        conn.close()
