from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3

from .plotting import render_input_trend_plots, render_trend_plot
from .rolling_metrics import build_metrics_frame
from .run_history import (
    RunRecord,
    coefficient_column_names,
    load_run_history,
    output_variable_names,
    parameter_column_names,
    select_latest_slice,
)
from .trend_summary import TrendSummary, build_trend_summary


DEFAULT_LIMIT = 400
DEFAULT_WINDOW = 25


@dataclass(frozen=True)
class AnalysisArtifacts:
    dataset_id: str | None
    output_dir: Path
    summary_path: Path
    metrics_path: Path
    plot_path: Path
    parameter_plot_paths: tuple[Path, ...]
    coefficient_plot_paths: tuple[Path, ...]
    parameter_count: int
    coefficient_count: int
    output_variable_count: int
    summary: TrendSummary


def dataset_output_name(dataset_id: str | None) -> str:
    return dataset_id if dataset_id is not None else "__null_dataset__"


def output_root_from_db(bigpopa_db: Path) -> Path:
    return bigpopa_db.resolve().parent / "analysis"


def _format_optional_float(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.6f}"


def _format_round_trial(round_index: int | None, trial_index: int | None) -> str:
    if round_index is None and trial_index is None:
        return "n/a"
    if round_index is None:
        return f"trial {trial_index}"
    if trial_index is None:
        return f"round {round_index}"
    return f"round {round_index}, trial {trial_index}"


def _format_run_reference(
    run_index: int | None,
    round_index: int | None,
    trial_index: int | None,
) -> str:
    if run_index is None:
        return _format_round_trial(round_index, trial_index)

    round_trial = _format_round_trial(round_index, trial_index)
    if round_trial == "n/a":
        return f"run {run_index}"
    return f"run {run_index} ({round_trial})"


def _format_slice_span(summary: TrendSummary) -> str:
    return f"run range {summary.latest_slice_run_start}-{summary.latest_slice_run_end}"


def _write_summary(summary: TrendSummary, path: Path) -> None:
    lines = [
        "Model-fit trend analysis",
        "",
        f"Dataset id: {summary.dataset_id}",
        f"Current round index: {summary.current_round_index}",
        f"Analyzed runs: {summary.latest_slice_count}, {_format_slice_span(summary)}",
        f"Latest slice timestamp: {summary.latest_slice_last_timestamp_utc}",
        "",
        (
            "Best fit: "
            f"{_format_optional_float(summary.best_fit)} at "
            f"{_format_run_reference(summary.best_run_index, summary.best_round_index, summary.best_trial_index)}"
        ),
        f"Best model id: {summary.best_model_id}",
        f"Latest run fit: {_format_optional_float(summary.latest_fit)}",
        f"Rows since last best improvement: {summary.rows_since_last_best_improvement}",
        (
            "Last best improvement: "
            f"{_format_run_reference(summary.last_best_improvement_run_index, summary.last_best_improvement_round_index, summary.last_best_improvement_trial_index)}"
        ),
        f"Last best improvement timestamp: {summary.last_best_improvement_timestamp_utc}",
        "",
        f"Rolling center: {summary.rolling_center_interpretation}",
        f"Rolling spread: {summary.rolling_spread_interpretation}",
        f"Practical interpretation: {summary.practical_trend_interpretation}",
        f"Early rolling median average: {_format_optional_float(summary.early_median_average)}",
        f"Late rolling median average: {_format_optional_float(summary.late_median_average)}",
        f"Early rolling IQR average: {_format_optional_float(summary.early_iqr_average)}",
        f"Late rolling IQR average: {_format_optional_float(summary.late_iqr_average)}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def analyze_latest_runs(
    *,
    bigpopa_db: Path,
    output_root: Path | None = None,
    limit: int = DEFAULT_LIMIT,
    window: int = DEFAULT_WINDOW,
    dataset_id: str | None = None,
) -> AnalysisArtifacts:
    db_path = bigpopa_db.expanduser().resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"bigpopa.db was not found at {db_path}")

    resolved_output_root = (
        output_root.expanduser().resolve()
        if output_root is not None
        else output_root_from_db(db_path)
    )

    with sqlite3.connect(str(db_path)) as conn:
        selected_dataset_id, dataset_rows = load_run_history(conn, dataset_id=dataset_id)

    latest_slice = select_latest_slice(dataset_rows, limit)
    current_round_index = latest_slice[-1].derived_round_index
    current_round_rows: list[RunRecord] = [
        row for row in dataset_rows if row.derived_round_index == current_round_index
    ]

    metrics_frame = build_metrics_frame(latest_slice, window)
    summary = build_trend_summary(
        dataset_id=selected_dataset_id,
        latest_slice=latest_slice,
        current_round_rows=current_round_rows,
        metrics_frame=metrics_frame,
        window=window,
    )

    dataset_dir = resolved_output_root / dataset_output_name(selected_dataset_id)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    file_prefix = f"latest_{limit}_window_{window}"
    summary_path = dataset_dir / f"{file_prefix}_summary.txt"
    metrics_path = dataset_dir / f"{file_prefix}_metrics.csv"
    plot_path = dataset_dir / f"{file_prefix}_trend.png"
    parameter_plot_base = dataset_dir / f"{file_prefix}_parameters_trend.png"
    coefficient_plot_base = dataset_dir / f"{file_prefix}_coefficients_trend.png"

    metrics_frame.to_csv(metrics_path, index=False)
    _write_summary(summary, summary_path)
    render_trend_plot(metrics_frame, plot_path, window, selected_dataset_id)
    parameter_plot_paths = render_input_trend_plots(
        metrics_frame,
        parameter_plot_base,
        window=window,
        title_prefix=f"Parameter trends for dataset {selected_dataset_id or '<null>'}",
        value_columns=parameter_column_names(latest_slice),
    )
    coefficient_plot_paths = render_input_trend_plots(
        metrics_frame,
        coefficient_plot_base,
        window=window,
        title_prefix=f"Coefficient trends for dataset {selected_dataset_id or '<null>'}",
        value_columns=coefficient_column_names(latest_slice),
    )
    parameter_count = len(parameter_column_names(dataset_rows))
    coefficient_count = len(coefficient_column_names(dataset_rows))
    output_variable_count = len(output_variable_names(dataset_rows))

    return AnalysisArtifacts(
        dataset_id=selected_dataset_id,
        output_dir=dataset_dir,
        summary_path=summary_path,
        metrics_path=metrics_path,
        plot_path=plot_path,
        parameter_plot_paths=tuple(parameter_plot_paths),
        coefficient_plot_paths=tuple(coefficient_plot_paths),
        parameter_count=parameter_count,
        coefficient_count=coefficient_count,
        output_variable_count=output_variable_count,
        summary=summary,
    )
