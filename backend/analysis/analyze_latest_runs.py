from __future__ import annotations

import argparse
from pathlib import Path

from .latest_runs import DEFAULT_LIMIT, DEFAULT_WINDOW, analyze_latest_runs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze model-fit trends for the latest BIGPOPA runs."
    )
    parser.add_argument("--bigpopa-db", required=True, help="Path to bigpopa.db")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=(
            "Number of newest runs to analyze within the dataset cohort "
            f"(default: {DEFAULT_LIMIT})"
        ),
    )
    parser.add_argument(
        "--window",
        type=int,
        default=DEFAULT_WINDOW,
        help=f"Rolling window size over fit values (default: {DEFAULT_WINDOW})",
    )
    parser.add_argument(
        "--output-root",
        help="Root directory for analysis outputs. Defaults to <bigpopa.db parent>/analysis",
    )
    parser.add_argument(
        "--dataset-id",
        help="Optional dataset id override. Defaults to the dataset of the newest tracked run.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    artifacts = analyze_latest_runs(
        bigpopa_db=Path(args.bigpopa_db),
        output_root=Path(args.output_root) if args.output_root else None,
        limit=args.limit,
        window=args.window,
        dataset_id=args.dataset_id,
    )

    summary = artifacts.summary
    print("Analysis complete.")
    print(f"Dataset id: {artifacts.dataset_id}")
    print(f"Current round index: {summary.current_round_index}")
    print(
        f"Best fit: {summary.best_fit:.6f}"
        if summary.best_fit is not None
        else "Best fit: n/a"
    )
    print(f"Best trial index: {summary.best_trial_index}")
    print(
        f"Latest fit: {summary.latest_fit:.6f}"
        if summary.latest_fit is not None
        else "Latest fit: n/a"
    )
    print(f"Rows since last best improvement: {summary.rows_since_last_best_improvement}")
    print(f"Rolling center: {summary.rolling_center_interpretation}")
    print(f"Rolling spread: {summary.rolling_spread_interpretation}")
    print(f"Practical interpretation: {summary.practical_trend_interpretation}")
    print(f"Summary: {artifacts.summary_path}")
    print(f"Metrics CSV: {artifacts.metrics_path}")
    print(f"Trend PNG: {artifacts.plot_path}")
    for path in artifacts.parameter_plot_paths:
        print(f"Parameter trend PNG: {path}")
    for path in artifacts.coefficient_plot_paths:
        print(f"Coefficient trend PNG: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
