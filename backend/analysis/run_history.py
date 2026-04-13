from __future__ import annotations

from dataclasses import dataclass
import sqlite3

from runtime.model_run_store import (
    load_model_run_rows,
    normalize_run_row,
    resolve_latest_dataset_id,
)
from runtime.model_status import fit_is_missing, visible_fit_pooled
from db.schema import ensure_current_bigpopa_schema


@dataclass(frozen=True)
class RunRecord:
    run_id: int
    model_id: str
    dataset_id: str | None
    model_status: str | None
    fit_pooled: float | None
    fit_missing: bool
    trial_index: int | None
    batch_index: int | None
    started_at_utc: str | None
    completed_at_utc: str | None
    sequence_index: int
    derived_round_index: int
    input_param: dict[str, float]
    input_coef: dict[str, dict[str, dict[str, float]]]
    output_set: dict[str, object]


def normalize_requested_dataset_id(dataset_id: str | None) -> str | None:
    if dataset_id is None:
        return None
    normalized = dataset_id.strip()
    return normalized or None


def parameter_column_names(rows: list[RunRecord]) -> list[str]:
    return sorted({key for row in rows for key in row.input_param.keys()})


def coefficient_column_names(rows: list[RunRecord]) -> list[str]:
    return sorted(
        {
            f"{func_name}.{x_name}.{beta_name}"
            for row in rows
            for func_name, x_map in row.input_coef.items()
            for x_name, beta_map in x_map.items()
            for beta_name in beta_map.keys()
        }
    )


def output_variable_names(rows: list[RunRecord]) -> list[str]:
    return sorted({key for row in rows for key in row.output_set.keys()})


def flatten_run_inputs(row: RunRecord) -> dict[str, float]:
    flattened: dict[str, float] = {}
    for key in sorted(row.input_param.keys()):
        flattened[key] = float(row.input_param[key])
    for func_name in sorted(row.input_coef.keys()):
        x_map = row.input_coef[func_name]
        for x_name in sorted(x_map.keys()):
            beta_map = x_map[x_name]
            for beta_name in sorted(beta_map.keys()):
                flattened[f"{func_name}.{x_name}.{beta_name}"] = float(beta_map[beta_name])
    return flattened


def normalize_rows(rows: list[tuple[object, ...]]) -> list[RunRecord]:
    normalized: list[RunRecord] = []
    derived_round_index = 0
    visible_sequence_index = 0
    for raw_row in rows:
        row = normalize_run_row(raw_row)
        trial_index = row.trial_index
        if trial_index is None:
            continue
        visible_sequence_index += 1
        if visible_sequence_index == 1:
            derived_round_index = 1
        elif trial_index == 1:
            derived_round_index += 1
        fit_missing = fit_is_missing(row.model_status, row.fit_pooled)
        fit_value = visible_fit_pooled(row.model_status, row.fit_pooled)
        normalized.append(
            RunRecord(
                run_id=row.run_id,
                model_id=row.model_id,
                dataset_id=row.dataset_id,
                model_status=row.model_status,
                fit_pooled=fit_value,
                fit_missing=fit_missing,
                trial_index=trial_index,
                batch_index=row.batch_index,
                started_at_utc=row.started_at_utc,
                completed_at_utc=row.completed_at_utc,
                sequence_index=visible_sequence_index,
                derived_round_index=derived_round_index,
                input_param={key: float(value) for key, value in row.input_param.items()},
                input_coef={
                    func_name: {
                        x_name: {beta_name: float(beta_value) for beta_name, beta_value in beta_map.items()}
                        for x_name, beta_map in x_map.items()
                    }
                    for func_name, x_map in row.input_coef.items()
                },
                output_set=dict(row.output_set),
            )
        )
    return normalized


def select_latest_slice(rows: list[RunRecord], limit: int) -> list[RunRecord]:
    if limit <= 0:
        raise ValueError("limit must be greater than 0")
    return rows[-limit:]


def load_run_history(
    conn: sqlite3.Connection,
    dataset_id: str | None = None,
) -> tuple[str | None, list[RunRecord]]:
    cursor = conn.cursor()
    ensure_current_bigpopa_schema(cursor)
    requested_dataset_id = normalize_requested_dataset_id(dataset_id)
    selected_dataset_id = (
        requested_dataset_id if requested_dataset_id is not None else resolve_latest_dataset_id(conn)
    )
    rows = normalize_rows(load_model_run_rows(conn, dataset_id=selected_dataset_id))
    if not rows:
        if requested_dataset_id is not None:
            raise RuntimeError(
                f"No tracked runs were found for requested dataset_id={requested_dataset_id!r}. "
                "Leave the dataset override blank to use the latest dataset."
            )
        raise RuntimeError(f"No tracked runs were found for dataset_id={selected_dataset_id!r}.")
    return selected_dataset_id, rows
