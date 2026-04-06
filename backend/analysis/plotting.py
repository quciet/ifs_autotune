from __future__ import annotations

from pathlib import Path
import math
import textwrap

import pandas as pd

X_AXIS_COLUMN = "run_index"
X_AXIS_LABEL = "Run index"


def _clipped_fit_range(fit_rows: pd.DataFrame) -> tuple[float, float, int]:
    fit_values = fit_rows["fit_pooled"].astype(float)
    lower_bound = float(fit_values.min())
    actual_max = float(fit_values.max())
    q1 = float(fit_values.quantile(0.25))
    q3 = float(fit_values.quantile(0.75))
    iqr = q3 - q1
    robust_upper = q3 + (1.5 * iqr)

    if iqr <= 0:
        robust_upper = actual_max

    clipped_upper = min(actual_max, robust_upper)
    if clipped_upper <= lower_bound:
        clipped_upper = actual_max

    outlier_count = int((fit_values > clipped_upper).sum())
    return lower_bound, clipped_upper, outlier_count


def _rolling_median(series: pd.Series, window: int) -> pd.Series:
    valid = series.dropna()
    return valid.rolling(window=window, min_periods=window).median().reindex(series.index)


def _wrap_label(label: str, width: int = 36) -> str:
    return "\n".join(textwrap.wrap(label, width=width)) or label


def _page_paths(output_path: Path, page_count: int) -> list[Path]:
    if page_count <= 1:
        return [output_path]
    return [
        output_path.with_name(f"{output_path.stem}_part_{index:02d}{output_path.suffix}")
        for index in range(1, page_count + 1)
    ]


def render_trend_plot(
    metrics_frame: pd.DataFrame,
    output_path: Path,
    window: int,
    dataset_id: str | None,
) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError(
            "matplotlib is required to render trend analysis PNG outputs."
        ) from exc

    fit_rows = metrics_frame.dropna(subset=["fit_pooled"])
    if fit_rows.empty:
        raise RuntimeError("No successful fit values are available to plot.")

    metric_rows = metrics_frame.dropna(
        subset=[
            f"rolling_mean_{window}",
            f"rolling_median_{window}",
            f"rolling_q1_{window}",
            f"rolling_q3_{window}",
            f"rolling_iqr_{window}",
            f"rolling_std_{window}",
        ]
    )

    lower_bound, clipped_upper, outlier_count = _clipped_fit_range(fit_rows)
    clipped_fit_rows = fit_rows[fit_rows["fit_pooled"] <= clipped_upper]
    outlier_rows = fit_rows[fit_rows["fit_pooled"] > clipped_upper]
    display_span = max(clipped_upper - lower_bound, 1e-6)
    top_padding = max(display_span * 0.06, clipped_upper * 0.02, 0.002)
    clipped_marker_y = clipped_upper + (top_padding * 0.45)
    displayed_top = clipped_upper + top_padding

    figure, axes = plt.subplots(
        2,
        1,
        figsize=(14, 8),
        dpi=140,
        sharex=True,
        gridspec_kw={"height_ratios": [3, 1.2]},
    )
    figure.patch.set_facecolor("white")

    top_axis, bottom_axis = axes
    top_axis.scatter(
        clipped_fit_rows[X_AXIS_COLUMN],
        clipped_fit_rows["fit_pooled"],
        s=18,
        alpha=0.35,
        color="#205493",
        label="Raw fit",
    )
    if not outlier_rows.empty:
        top_axis.scatter(
            outlier_rows[X_AXIS_COLUMN],
            [clipped_marker_y] * len(outlier_rows),
            s=42,
            marker="^",
            alpha=0.85,
            color="#8c2d04",
            label=f"Outliers clipped above {clipped_upper:.3f} (n={outlier_count})",
        )
    top_axis.plot(
        fit_rows[X_AXIS_COLUMN],
        fit_rows["best_so_far"],
        linestyle="--",
        linewidth=1.6,
        color="#6b6b6b",
        label="Best so far",
    )

    if not metric_rows.empty:
        top_axis.fill_between(
            metric_rows[X_AXIS_COLUMN],
            metric_rows[f"rolling_q1_{window}"],
            metric_rows[f"rolling_q3_{window}"],
            color="#7fc97f",
            alpha=0.3,
            label=f"Rolling IQR ({window})",
        )
        top_axis.plot(
            metric_rows[X_AXIS_COLUMN],
            metric_rows[f"rolling_mean_{window}"],
            linewidth=1.8,
            color="#d95f02",
            alpha=0.8,
            label=f"Rolling mean ({window})",
        )
        top_axis.plot(
            metric_rows[X_AXIS_COLUMN],
            metric_rows[f"rolling_median_{window}"],
            linewidth=2.9,
            color="#1b7837",
            label=f"Rolling median ({window})",
        )
        bottom_axis.plot(
            metric_rows[X_AXIS_COLUMN],
            metric_rows[f"rolling_iqr_{window}"],
            linewidth=2.2,
            color="#b22222",
            label=f"Rolling IQR ({window}, middle 50%)",
        )
        bottom_axis.plot(
            metric_rows[X_AXIS_COLUMN],
            metric_rows[f"rolling_std_{window}"],
            linewidth=2.0,
            linestyle="--",
            color="#205493",
            label=f"Rolling std dev ({window})",
        )

    title_dataset = dataset_id if dataset_id is not None else "<null>"
    top_axis.set_title(
        f"Latest fit trend for dataset {title_dataset}",
        fontweight="bold",
    )
    top_axis.set_ylabel("fit_pooled")
    top_axis.set_ylim(lower_bound, displayed_top)
    top_axis.grid(True, alpha=0.2)
    top_axis.legend(loc="upper right")

    bottom_axis.set_xlabel(X_AXIS_LABEL)
    bottom_axis.set_ylabel("Rolling spread")
    bottom_axis.grid(True, alpha=0.2)
    if not metric_rows.empty:
        bottom_axis.legend(loc="upper right")

    figure.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, bbox_inches="tight")
    plt.close(figure)


def render_input_trend_plots(
    metrics_frame: pd.DataFrame,
    output_path: Path,
    *,
    window: int,
    title_prefix: str,
    value_columns: list[str],
    max_subplots_per_page: int = 4,
) -> list[Path]:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError(
            "matplotlib is required to render trend analysis PNG outputs."
        ) from exc

    available_columns = [column for column in value_columns if column in metrics_frame.columns]
    if not available_columns:
        return []

    page_count = math.ceil(len(available_columns) / max_subplots_per_page)
    output_paths = _page_paths(output_path, page_count)

    for page_index, page_path in enumerate(output_paths):
        start = page_index * max_subplots_per_page
        end = start + max_subplots_per_page
        page_columns = available_columns[start:end]
        ncols = 2
        nrows = math.ceil(len(page_columns) / ncols)

        figure, axes = plt.subplots(
            nrows,
            ncols,
            figsize=(14, max(3.2 * nrows, 4.8)),
            dpi=140,
            sharex=True,
        )
        figure.patch.set_facecolor("white")
        axes_list = list(axes.ravel() if hasattr(axes, "ravel") else [axes])

        for axis, column in zip(axes_list, page_columns):
            series = pd.to_numeric(metrics_frame[column], errors="coerce")
            valid_rows = metrics_frame[series.notna()]
            if valid_rows.empty:
                axis.set_visible(False)
                continue

            rolling_median = _rolling_median(series, window)
            median_rows = metrics_frame[rolling_median.notna()]

            axis.scatter(
                valid_rows[X_AXIS_COLUMN],
                pd.to_numeric(valid_rows[column], errors="coerce"),
                s=12,
                alpha=0.35,
                color="#205493",
                label="Raw value",
            )
            if not median_rows.empty:
                axis.plot(
                    median_rows[X_AXIS_COLUMN],
                    rolling_median[rolling_median.notna()],
                    linewidth=2.4,
                    color="#1b7837",
                    label=f"Rolling median ({window})",
                )

            axis.set_title(_wrap_label(column), fontsize=9, fontweight="bold")
            axis.grid(True, alpha=0.2)

        for axis in axes_list[len(page_columns):]:
            axis.set_visible(False)

        if axes_list:
            axes_list[0].legend(loc="upper right")
        for axis in axes_list[-ncols:]:
            if axis.get_visible():
                axis.set_xlabel(X_AXIS_LABEL)
        for row_start in range(0, len(axes_list), ncols):
            axis = axes_list[row_start]
            if axis.get_visible():
                axis.set_ylabel("Input value")

        title = title_prefix
        if page_count > 1:
            title = f"{title_prefix} (page {page_index + 1} of {page_count})"
        figure.suptitle(title, fontweight="bold", y=0.995)
        figure.tight_layout(rect=(0, 0, 1, 0.98))
        page_path.parent.mkdir(parents=True, exist_ok=True)
        figure.savefig(page_path, bbox_inches="tight")
        plt.close(figure)

    return output_paths
