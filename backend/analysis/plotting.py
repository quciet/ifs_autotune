from __future__ import annotations

from pathlib import Path

import pandas as pd


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
        ]
    )

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
        fit_rows["trial_index"],
        fit_rows["fit_pooled"],
        s=18,
        alpha=0.35,
        color="#205493",
        label="Raw fit",
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
            linewidth=2.4,
            color="#d95f02",
            label=f"Rolling mean ({window})",
        )
        top_axis.plot(
            metric_rows["trial_index"],
            metric_rows[f"rolling_median_{window}"],
            linewidth=2.4,
            color="#1b7837",
            label=f"Rolling median ({window})",
        )
        bottom_axis.plot(
            metric_rows["trial_index"],
            metric_rows[f"rolling_iqr_{window}"],
            linewidth=2.2,
            color="#b22222",
            label=f"Rolling IQR width ({window})",
        )

    title_dataset = dataset_id if dataset_id is not None else "<null>"
    top_axis.set_title(
        f"Latest fit trend for dataset {title_dataset}",
        fontweight="bold",
    )
    top_axis.set_ylabel("fit_pooled")
    top_axis.grid(True, alpha=0.2)
    top_axis.legend(loc="upper right")

    bottom_axis.set_xlabel("Trial index")
    bottom_axis.set_ylabel("Rolling IQR")
    bottom_axis.grid(True, alpha=0.2)
    if not metric_rows.empty:
        bottom_axis.legend(loc="upper right")

    figure.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, bbox_inches="tight")
    plt.close(figure)
