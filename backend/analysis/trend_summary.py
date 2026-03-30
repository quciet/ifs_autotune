from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .run_history import RunRecord


@dataclass(frozen=True)
class TrendSummary:
    dataset_id: str | None
    current_round_index: int
    latest_slice_count: int
    latest_slice_run_start: int
    latest_slice_run_end: int
    latest_slice_round_start: int
    latest_slice_round_end: int
    latest_slice_trial_start: int | None
    latest_slice_trial_end: int | None
    latest_slice_started_at_utc: str | None
    latest_slice_last_timestamp_utc: str | None
    best_fit: float | None
    best_run_index: int | None
    best_trial_index: int | None
    best_round_index: int | None
    best_model_id: str | None
    latest_fit: float | None
    rows_since_last_best_improvement: int | None
    last_best_improvement_run_index: int | None
    last_best_improvement_trial_index: int | None
    last_best_improvement_round_index: int | None
    last_best_improvement_timestamp_utc: str | None
    rolling_center_interpretation: str
    rolling_spread_interpretation: str
    practical_trend_interpretation: str
    early_median_average: float | None
    late_median_average: float | None
    early_iqr_average: float | None
    late_iqr_average: float | None


def _segment_average(frame: pd.DataFrame, column: str) -> float | None:
    values = frame[column].dropna()
    if values.empty:
        return None
    return float(values.mean())


def compare_rolling_segments(metrics_frame: pd.DataFrame, window: int) -> dict[str, Any]:
    median_column = f"rolling_median_{window}"
    iqr_column = f"rolling_iqr_{window}"
    valid = metrics_frame.dropna(subset=[median_column, iqr_column])
    if valid.empty:
        return {
            "early_median_average": None,
            "late_median_average": None,
            "early_iqr_average": None,
            "late_iqr_average": None,
            "rolling_center_interpretation": "insufficient rolling data",
            "rolling_spread_interpretation": "insufficient rolling data",
            "practical_trend_interpretation": "insufficient rolling data",
        }

    segment_size = max(len(valid) // 3, 1)
    early = valid.head(segment_size)
    late = valid.tail(segment_size)

    early_median_average = _segment_average(early, median_column)
    late_median_average = _segment_average(late, median_column)
    early_iqr_average = _segment_average(early, iqr_column)
    late_iqr_average = _segment_average(late, iqr_column)

    center_ratio = 0.0
    if (
        early_median_average is not None
        and late_median_average is not None
        and abs(early_median_average) >= 1e-8
    ):
        center_ratio = (late_median_average - early_median_average) / abs(early_median_average)

    spread_ratio = 0.0
    if (
        early_iqr_average is not None
        and late_iqr_average is not None
        and abs(early_iqr_average) >= 1e-8
    ):
        spread_ratio = (late_iqr_average - early_iqr_average) / abs(early_iqr_average)

    if center_ratio <= -0.05:
        center_interpretation = "rolling median is improving"
    elif center_ratio >= 0.05:
        center_interpretation = "rolling median is worsening"
    else:
        center_interpretation = "rolling median is flat"

    if spread_ratio <= -0.15:
        spread_interpretation = "rolling IQR is shrinking"
    elif spread_ratio >= 0.15:
        spread_interpretation = "rolling IQR is widening"
    else:
        spread_interpretation = "rolling IQR is flat"

    if center_interpretation == "rolling median is flat" and spread_interpretation in {
        "rolling IQR is flat",
        "rolling IQR is shrinking",
    }:
        practical = "plateau"
    elif center_interpretation == "rolling median is worsening" and spread_interpretation == "rolling IQR is widening":
        practical = "unstable"
    else:
        practical = "still moving"

    return {
        "early_median_average": early_median_average,
        "late_median_average": late_median_average,
        "early_iqr_average": early_iqr_average,
        "late_iqr_average": late_iqr_average,
        "rolling_center_interpretation": center_interpretation,
        "rolling_spread_interpretation": spread_interpretation,
        "practical_trend_interpretation": practical,
    }


def build_trend_summary(
    *,
    dataset_id: str | None,
    latest_slice: list[RunRecord],
    current_round_rows: list[RunRecord],
    metrics_frame: pd.DataFrame,
    window: int,
) -> TrendSummary:
    if not latest_slice:
        raise RuntimeError("latest_slice cannot be empty")
    if not current_round_rows:
        raise RuntimeError("current_round_rows cannot be empty")

    latest_row = latest_slice[-1]
    current_round_index = latest_row.derived_round_index
    best_fit: float | None = None
    best_run_index: int | None = None
    best_trial_index: int | None = None
    best_round_index: int | None = None
    best_model_id: str | None = None
    best_sequence_index: int | None = None
    last_best_improvement_run_index: int | None = None
    last_best_improvement_trial_index: int | None = None
    last_best_improvement_round_index: int | None = None
    last_best_improvement_timestamp_utc: str | None = None

    for row in latest_slice:
        if row.fit_pooled is None:
            continue
        if best_fit is None or row.fit_pooled < best_fit:
            best_fit = row.fit_pooled
            best_run_index = row.sequence_index
            best_trial_index = row.trial_index
            best_round_index = row.derived_round_index
            best_model_id = row.model_id
            best_sequence_index = row.sequence_index
            last_best_improvement_run_index = row.sequence_index
            last_best_improvement_trial_index = row.trial_index
            last_best_improvement_round_index = row.derived_round_index
            last_best_improvement_timestamp_utc = (
                row.completed_at_utc if row.completed_at_utc else row.started_at_utc
            )

    rows_since_last_best_improvement = None
    if best_sequence_index is not None:
        rows_since_last_best_improvement = latest_row.sequence_index - best_sequence_index

    rolling_comparison = compare_rolling_segments(metrics_frame, window)

    return TrendSummary(
        dataset_id=dataset_id,
        current_round_index=current_round_index,
        latest_slice_count=len(latest_slice),
        latest_slice_run_start=latest_slice[0].sequence_index,
        latest_slice_run_end=latest_row.sequence_index,
        latest_slice_round_start=latest_slice[0].derived_round_index,
        latest_slice_round_end=latest_row.derived_round_index,
        latest_slice_trial_start=latest_slice[0].trial_index,
        latest_slice_trial_end=latest_row.trial_index,
        latest_slice_started_at_utc=latest_slice[0].started_at_utc,
        latest_slice_last_timestamp_utc=(
            latest_row.completed_at_utc if latest_row.completed_at_utc else latest_row.started_at_utc
        ),
        best_fit=best_fit,
        best_run_index=best_run_index,
        best_trial_index=best_trial_index,
        best_round_index=best_round_index,
        best_model_id=best_model_id,
        latest_fit=latest_row.fit_pooled,
        rows_since_last_best_improvement=rows_since_last_best_improvement,
        last_best_improvement_run_index=last_best_improvement_run_index,
        last_best_improvement_trial_index=last_best_improvement_trial_index,
        last_best_improvement_round_index=last_best_improvement_round_index,
        last_best_improvement_timestamp_utc=last_best_improvement_timestamp_utc,
        rolling_center_interpretation=rolling_comparison["rolling_center_interpretation"],
        rolling_spread_interpretation=rolling_comparison["rolling_spread_interpretation"],
        practical_trend_interpretation=rolling_comparison["practical_trend_interpretation"],
        early_median_average=rolling_comparison["early_median_average"],
        late_median_average=rolling_comparison["late_median_average"],
        early_iqr_average=rolling_comparison["early_iqr_average"],
        late_iqr_average=rolling_comparison["late_iqr_average"],
    )
