from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3

from .plotting import render_trend_plot
from .rolling_metrics import build_metrics_frame
from .run_history import RunRecord, load_run_history, select_latest_slice
from .trend_summary import TrendSummary, build_trend_summary


DEFAULT_LIMIT = 300
DEFAULT_WINDOW = 20


@dataclass(frozen=True)
class AnalysisArtifacts:
    dataset_id: str | None
    output_dir: Path
    summary_path: Path
    metrics_path: Path
    plot_path: Path
    summary: TrendSummary


def dataset_output_name(dataset_id: str | None) -> str:
    return dataset_id if dataset_id is not None else "__null_dataset__"


def output_root_from_db(bigpopa_db: Path) -> Path:
    return bigpopa_db.resolve().parent / "analysis"


def _format_optional_float(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.6f}"


def _write_summary(summary: TrendSummary, path: Path) -> None:
    lines = [
        "Model-fit trend analysis",
        "",
        f"Dataset id: {summary.dataset_id}",
        f"Current round index: {summary.current_round_index}",
        (
            f"Analyzed runs: {summary.latest_slice_count}, "
            f"trial range {summary.latest_slice_trial_start}-{summary.latest_slice_trial_end}"
        ),
        f"Latest slice completed at: {summary.latest_slice_completed_at_utc}",
        "",
        f"Best fit: {_format_optional_float(summary.best_fit)} at trial {summary.best_trial_index}",
        f"Best model id: {summary.best_model_id}",
        f"Latest run fit: {_format_optional_float(summary.latest_fit)}",
        f"Rows since last best improvement: {summary.rows_since_last_best_improvement}",
        f"Last best improvement trial: {summary.last_best_improvement_trial_index}",
        f"Last best improvement completed at: {summary.last_best_improvement_completed_at_utc}",
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

    metrics_frame.to_csv(metrics_path, index=False)
    _write_summary(summary, summary_path)
    render_trend_plot(metrics_frame, plot_path, window, selected_dataset_id)

    return AnalysisArtifacts(
        dataset_id=selected_dataset_id,
        output_dir=dataset_dir,
        summary_path=summary_path,
        metrics_path=metrics_path,
        plot_path=plot_path,
        summary=summary,
    )
