from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from analysis.latest_runs import AnalysisArtifacts, analyze_latest_runs


def emit_response(status: str, stage: str, message: str, data: dict[str, Any]) -> None:
    print(
        json.dumps(
            {
                "status": status,
                "stage": stage,
                "message": message,
                "data": data,
            }
        )
    )
    sys.stdout.flush()


def _serialize_artifacts(artifacts: AnalysisArtifacts) -> dict[str, Any]:
    summary = asdict(artifacts.summary)
    return {
        "dataset_id": artifacts.dataset_id,
        "output_dir": str(artifacts.output_dir),
        "summary_path": str(artifacts.summary_path),
        "metrics_path": str(artifacts.metrics_path),
        "plot_path": str(artifacts.plot_path),
        "parameter_plot_paths": [str(path) for path in artifacts.parameter_plot_paths],
        "coefficient_plot_paths": [str(path) for path in artifacts.coefficient_plot_paths],
        "parameter_plot_count": len(artifacts.parameter_plot_paths),
        "coefficient_plot_count": len(artifacts.coefficient_plot_paths),
        "parameter_count": artifacts.parameter_count,
        "coefficient_count": artifacts.coefficient_count,
        "output_variable_count": artifacts.output_variable_count,
        "summary": summary,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run BIGPOPA trend analysis and emit structured JSON.")
    parser.add_argument("--bigpopa-db", required=True, help="Path to bigpopa.db")
    parser.add_argument("--limit", type=int, default=400, help="Number of latest runs to analyze")
    parser.add_argument("--window", type=int, default=25, help="Rolling window size")
    parser.add_argument("--output-root", required=False, help="Optional output root")
    parser.add_argument("--dataset-id", required=False, help="Optional dataset id override")
    args = parser.parse_args(argv)

    if args.limit <= 0:
        emit_response(
            "error",
            "trend_analysis",
            "Trend analysis limit must be greater than 0.",
            {"limit": args.limit},
        )
        return 1

    if args.window <= 0:
        emit_response(
            "error",
            "trend_analysis",
            "Trend analysis rolling window must be greater than 0.",
            {"window": args.window},
        )
        return 1

    bigpopa_db = Path(args.bigpopa_db).expanduser().resolve()
    try:
        artifacts = analyze_latest_runs(
            bigpopa_db=bigpopa_db,
            output_root=Path(args.output_root).expanduser().resolve()
            if args.output_root
            else None,
            limit=args.limit,
            window=args.window,
            dataset_id=args.dataset_id,
        )
    except Exception as exc:
        emit_response(
            "error",
            "trend_analysis",
            "Trend analysis failed.",
            {
                "bigpopa_db": str(bigpopa_db),
                "dataset_id": args.dataset_id,
                "limit": args.limit,
                "window": args.window,
                "error": str(exc),
            },
        )
        return 1

    emit_response(
        "success",
        "trend_analysis",
        "Trend analysis completed successfully.",
        _serialize_artifacts(artifacts),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
