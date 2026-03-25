from __future__ import annotations

from pathlib import Path

import pandas as pd


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
        clipped_fit_rows["trial_index"],
        clipped_fit_rows["fit_pooled"],
        s=18,
        alpha=0.35,
        color="#205493",
        label="Raw fit",
    )
    if not outlier_rows.empty:
        top_axis.scatter(
            outlier_rows["trial_index"],
            [clipped_marker_y] * len(outlier_rows),
            s=42,
            marker="^",
            alpha=0.85,
            color="#8c2d04",
            label=f"Outliers clipped above {clipped_upper:.3f} (n={outlier_count})",
        )
    top_axis.plot(
        fit_rows["trial_index"],
        fit_rows["best_so_far"],
        linestyle="--",
        linewidth=1.6,
        color="#6b6b6b",
        label="Best so far",
    )

    if not metric_rows.empty:
        top_axis.fill_between(
            metric_rows["trial_index"],
            metric_rows[f"rolling_q1_{window}"],
            metric_rows[f"rolling_q3_{window}"],
            color="#7fc97f",
            alpha=0.3,
            label=f"Rolling IQR ({window})",
        )
        top_axis.plot(
            metric_rows["trial_index"],
            metric_rows[f"rolling_mean_{window}"],
            linewidth=1.8,
            color="#d95f02",
            alpha=0.8,
            label=f"Rolling mean ({window})",
        )
        top_axis.plot(
            metric_rows["trial_index"],
            metric_rows[f"rolling_median_{window}"],
            linewidth=2.9,
            color="#1b7837",
            label=f"Rolling median ({window})",
        )
        bottom_axis.plot(
            metric_rows["trial_index"],
            metric_rows[f"rolling_iqr_{window}"],
            linewidth=2.2,
            color="#b22222",
            label=f"Rolling IQR ({window}, middle 50%)",
        )
        bottom_axis.plot(
            metric_rows["trial_index"],
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

    bottom_axis.set_xlabel("Trial index")
    bottom_axis.set_ylabel("Rolling spread")
    bottom_axis.grid(True, alpha=0.2)
    if not metric_rows.empty:
        bottom_axis.legend(loc="upper right")

    figure.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, bbox_inches="tight")
    plt.close(figure)
