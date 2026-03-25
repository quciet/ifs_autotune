from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from .run_history import (
    RunRecord,
    coefficient_column_names,
    flatten_run_inputs,
    parameter_column_names,
)


def _best_so_far(values: Sequence[float | None]) -> list[float | None]:
    best: float | None = None
    result: list[float | None] = []
    for value in values:
        if value is not None:
            best = value if best is None else min(best, value)
        result.append(best)
    return result


def build_metrics_frame(rows: Sequence[RunRecord], window: int) -> pd.DataFrame:
    if window <= 0:
        raise ValueError("window must be greater than 0")

    frame = pd.DataFrame(
        [
            {
                "model_id": row.model_id,
                "dataset_id": row.dataset_id,
                "model_status": row.model_status,
                "fit_pooled": row.fit_pooled,
                "fit_missing": row.fit_missing,
                "trial_index": row.trial_index,
                "batch_index": row.batch_index,
                "sequence_index": row.sequence_index,
                "derived_round_index": row.derived_round_index,
                "started_at_utc": row.started_at_utc,
                "completed_at_utc": row.completed_at_utc,
            }
            for row in rows
        ]
    )

    if frame.empty:
        return frame

    flattened_input_rows = [flatten_run_inputs(row) for row in rows]
    flat_frame = pd.DataFrame(flattened_input_rows)
    ordered_input_columns = parameter_column_names(list(rows)) + coefficient_column_names(list(rows))
    if ordered_input_columns:
        for column in ordered_input_columns:
            if column not in flat_frame.columns:
                flat_frame[column] = pd.NA
        frame = pd.concat([frame, flat_frame[ordered_input_columns]], axis=1)

    frame["best_so_far"] = _best_so_far(frame["fit_pooled"].tolist())

    valid_fit = frame["fit_pooled"].dropna()
    rolling_mean = valid_fit.rolling(window=window, min_periods=window).mean()
    rolling_median = valid_fit.rolling(window=window, min_periods=window).median()
    rolling_q1 = valid_fit.rolling(window=window, min_periods=window).quantile(0.25)
    rolling_q3 = valid_fit.rolling(window=window, min_periods=window).quantile(0.75)
    rolling_std = valid_fit.rolling(window=window, min_periods=window).std()

    frame[f"rolling_mean_{window}"] = rolling_mean.reindex(frame.index)
    frame[f"rolling_median_{window}"] = rolling_median.reindex(frame.index)
    frame[f"rolling_q1_{window}"] = rolling_q1.reindex(frame.index)
    frame[f"rolling_q3_{window}"] = rolling_q3.reindex(frame.index)
    frame[f"rolling_iqr_{window}"] = frame[f"rolling_q3_{window}"] - frame[f"rolling_q1_{window}"]
    frame[f"rolling_std_{window}"] = rolling_std.reindex(frame.index)

    return frame
