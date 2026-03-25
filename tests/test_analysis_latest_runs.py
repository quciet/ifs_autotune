from __future__ import annotations

import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from analysis.analyze_latest_runs import build_parser
from analysis.latest_runs import analyze_latest_runs, dataset_output_name
from analysis.plotting import render_trend_plot
from analysis.rolling_metrics import build_metrics_frame
from analysis.run_history import load_run_history
from analysis.trend_summary import compare_rolling_segments


def build_history_db(
    db_path: Path,
    rows: list[
        tuple[
            str,
            str | None,
            str,
            float | None,
            int,
            int | None,
            str,
            str,
        ]
    ],
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE model_input (
                model_id TEXT PRIMARY KEY,
                dataset_id TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE model_output (
                model_id TEXT PRIMARY KEY,
                model_status TEXT,
                fit_pooled REAL,
                trial_index INTEGER,
                batch_index INTEGER,
                started_at_utc TEXT,
                completed_at_utc TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO model_input (model_id, dataset_id) VALUES (?, ?)",
            [(row[0], row[1]) for row in rows],
        )
        conn.executemany(
            """
            INSERT INTO model_output (
                model_id,
                model_status,
                fit_pooled,
                trial_index,
                batch_index,
                started_at_utc,
                completed_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [(row[0], row[2], row[3], row[4], row[5], row[6], row[7]) for row in rows],
        )
        conn.commit()
    finally:
        conn.close()


@dataclass(frozen=True)
class StubRunRecord:
    model_id: str
    dataset_id: str | None
    model_status: str | None
    fit_pooled: float | None
    fit_missing: bool
    trial_index: int | None
    batch_index: int | None
    sequence_index: int
    derived_round_index: int
    started_at_utc: str | None = None
    completed_at_utc: str | None = None


def stub_rows(values: list[float]) -> list[StubRunRecord]:
    return [
        StubRunRecord(
            model_id=f"m{index}",
            dataset_id="dataset-a",
            model_status="completed",
            fit_pooled=value,
            fit_missing=False,
            trial_index=index,
            batch_index=1,
            sequence_index=index,
            derived_round_index=1,
        )
        for index, value in enumerate(values, start=1)
    ]


def test_load_run_history_defaults_to_dataset_of_newest_tracked_run(tmp_path: Path) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_history_db(
        db_path,
        [
            (
                "older-a",
                "dataset-a",
                "completed",
                0.9,
                1,
                1,
                "2026-03-24T00:00:00Z",
                "2026-03-24T00:01:00Z",
            ),
            (
                "newer-b",
                "dataset-b",
                "completed",
                0.8,
                1,
                1,
                "2026-03-24T01:00:00Z",
                "2026-03-24T01:01:00Z",
            ),
        ],
    )

    with sqlite3.connect(db_path) as conn:
        dataset_id, rows = load_run_history(conn)

    assert dataset_id == "dataset-b"
    assert [row.model_id for row in rows] == ["newer-b"]


def test_round_derivation_matches_trial_reset_ordering(tmp_path: Path) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_history_db(
        db_path,
        [
            (
                "round-1-last",
                "dataset-a",
                "completed",
                0.9,
                250,
                1,
                "2026-03-24T00:00:00Z",
                "2026-03-24T00:05:00Z",
            ),
            (
                "round-2-first",
                "dataset-a",
                "completed",
                0.8,
                1,
                1,
                "2026-03-24T01:00:00Z",
                "2026-03-24T01:05:00Z",
            ),
            (
                "round-3-first",
                "dataset-a",
                "completed",
                0.7,
                1,
                1,
                "2026-03-24T02:00:00Z",
                "2026-03-24T02:05:00Z",
            ),
        ],
    )

    with sqlite3.connect(db_path) as conn:
        _, rows = load_run_history(conn, dataset_id="dataset-a")

    assert [row.model_id for row in rows] == [
        "round-1-last",
        "round-2-first",
        "round-3-first",
    ]
    assert [row.derived_round_index for row in rows] == [1, 2, 3]


def test_build_metrics_frame_computes_rolling_statistics() -> None:
    frame = build_metrics_frame(stub_rows([1.0, 2.0, 3.0, 4.0]), window=3)

    assert pytest.approx(frame.loc[2, "rolling_mean_3"]) == 2.0
    assert pytest.approx(frame.loc[2, "rolling_median_3"]) == 2.0
    assert pytest.approx(frame.loc[2, "rolling_q1_3"]) == 1.5
    assert pytest.approx(frame.loc[2, "rolling_q3_3"]) == 2.5
    assert pytest.approx(frame.loc[2, "rolling_iqr_3"]) == 1.0
    assert pytest.approx(frame.loc[2, "rolling_std_3"]) == 1.0
    assert pytest.approx(frame.loc[3, "rolling_median_3"]) == 3.0


def test_compare_rolling_segments_detects_shrinking_spread() -> None:
    frame = build_metrics_frame(
        stub_rows([9.0, 1.0, 8.0, 2.0, 5.2, 4.8, 5.1, 4.9, 5.0]),
        window=3,
    )

    comparison = compare_rolling_segments(frame, window=3)

    assert comparison["rolling_spread_interpretation"] == "rolling IQR is shrinking"


def test_cli_defaults_use_latest_400_and_window_25() -> None:
    parser = build_parser()
    args = parser.parse_args(["--bigpopa-db", "C:\\temp\\bigpopa.db"])

    assert args.limit == 400
    assert args.window == 25


def test_render_trend_plot_handles_high_outliers(tmp_path: Path) -> None:
    pytest.importorskip("matplotlib")

    frame = build_metrics_frame(
        stub_rows([0.05, 0.06, 0.055, 0.058, 0.4, 0.052, 0.051, 0.39, 0.054]),
        window=3,
    )
    output_path = tmp_path / "trend.png"

    render_trend_plot(frame, output_path, window=3, dataset_id="dataset-a")

    assert output_path.exists()
    assert output_path.stat().st_size > 0


def test_analyze_latest_runs_writes_artifacts_under_dataset_folder(tmp_path: Path) -> None:
    pytest.importorskip("matplotlib")

    db_path = tmp_path / "bigpopa.db"
    output_root = tmp_path / "analysis"
    build_history_db(
        db_path,
        [
            (
                "m1",
                "dataset-z",
                "completed",
                0.90,
                1,
                1,
                "2026-03-24T00:00:00Z",
                "2026-03-24T00:01:00Z",
            ),
            (
                "m2",
                "dataset-z",
                "completed",
                0.70,
                2,
                1,
                "2026-03-24T00:02:00Z",
                "2026-03-24T00:03:00Z",
            ),
            (
                "m3",
                "dataset-z",
                "completed",
                0.60,
                3,
                1,
                "2026-03-24T00:04:00Z",
                "2026-03-24T00:05:00Z",
            ),
            (
                "m4",
                "dataset-z",
                "completed",
                0.55,
                4,
                1,
                "2026-03-24T00:06:00Z",
                "2026-03-24T00:07:00Z",
            ),
            (
                "m5",
                "dataset-z",
                "completed",
                0.54,
                5,
                1,
                "2026-03-24T00:08:00Z",
                "2026-03-24T00:09:00Z",
            ),
            (
                "m6",
                "dataset-z",
                "completed",
                0.54,
                6,
                1,
                "2026-03-24T00:10:00Z",
                "2026-03-24T00:11:00Z",
            ),
        ],
    )

    artifacts = analyze_latest_runs(
        bigpopa_db=db_path,
        output_root=output_root,
        limit=6,
        window=3,
    )

    expected_dir = output_root / dataset_output_name("dataset-z")
    assert artifacts.output_dir == expected_dir
    assert artifacts.summary_path.exists()
    assert artifacts.metrics_path.exists()
    assert artifacts.plot_path.exists()
    summary_text = artifacts.summary_path.read_text(encoding="utf-8").lower()
    metrics_header = artifacts.metrics_path.read_text(encoding="utf-8").splitlines()[0]
    assert "rolling center:" in summary_text
    assert "rolling spread:" in summary_text
    assert "rolling_std_3" in metrics_header
