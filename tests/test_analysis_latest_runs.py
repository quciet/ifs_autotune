from __future__ import annotations

import csv
import json
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from analysis.analyze_latest_runs import build_parser
from analysis.latest_runs import analyze_latest_runs, dataset_output_name
from analysis.plotting import render_input_trend_plots, render_trend_plot
from analysis.rolling_metrics import build_metrics_frame
from analysis.run_history import (
    coefficient_column_names,
    load_run_history,
    normalize_requested_dataset_id,
    parameter_column_names,
)
from analysis.trend_summary import compare_rolling_segments
from model_status import FIT_EVALUATED, IFS_RUN_COMPLETED, IFS_RUN_FAILED


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
            dict[str, float] | None,
            dict[str, dict[str, dict[str, float]]] | None,
        ]
    ],
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE model_input (
                model_id TEXT PRIMARY KEY,
                dataset_id TEXT,
                input_param TEXT,
                input_coef TEXT
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
            """
            INSERT INTO model_input (model_id, dataset_id, input_param, input_coef)
            VALUES (?, ?, ?, ?)
            """,
            [
                (
                    row[0],
                    row[1],
                    json.dumps(row[8] if len(row) > 8 and row[8] is not None else {"param_a": float(index + 1)}),
                    json.dumps(
                        row[9]
                        if len(row) > 9 and row[9] is not None
                        else {"func_a": {"x_a": {"beta_a": float(index + 1)}}}
                    ),
                )
                for index, row in enumerate(rows)
            ],
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
    input_param: dict[str, float] | None = None
    input_coef: dict[str, dict[str, dict[str, float]]] | None = None


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
            input_param={},
            input_coef={},
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
                None,
                None,
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
                None,
                None,
            ),
        ],
    )

    with sqlite3.connect(db_path) as conn:
        dataset_id, rows = load_run_history(conn)

    assert dataset_id == "dataset-b"
    assert [row.model_id for row in rows] == ["newer-b"]


def test_normalize_requested_dataset_id_treats_whitespace_as_blank() -> None:
    assert normalize_requested_dataset_id(None) is None
    assert normalize_requested_dataset_id("") is None
    assert normalize_requested_dataset_id("   ") is None
    assert normalize_requested_dataset_id("  dataset-a  ") == "dataset-a"


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
                None,
                None,
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
                None,
                None,
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
                None,
                None,
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

    assert frame["run_index"].tolist() == [1, 2, 3, 4]
    assert pytest.approx(frame.loc[2, "rolling_mean_3"]) == 2.0
    assert pytest.approx(frame.loc[2, "rolling_median_3"]) == 2.0
    assert pytest.approx(frame.loc[2, "rolling_q1_3"]) == 1.5
    assert pytest.approx(frame.loc[2, "rolling_q3_3"]) == 2.5
    assert pytest.approx(frame.loc[2, "rolling_iqr_3"]) == 1.0
    assert pytest.approx(frame.loc[2, "rolling_std_3"]) == 1.0
    assert pytest.approx(frame.loc[3, "rolling_median_3"]) == 3.0


def test_load_run_history_parses_input_json_and_metrics_frame_flattens_columns(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_history_db(
        db_path,
        [
            (
                "m1",
                "dataset-a",
                "completed",
                0.5,
                1,
                1,
                "2026-03-24T00:00:00Z",
                "2026-03-24T00:01:00Z",
                {"alpha": 1.0, "beta": 2.0},
                {"func_b": {"x_b": {"b2": 4.0}}, "func_a": {"x_a": {"a1": 3.0}}},
            ),
            (
                "m2",
                "dataset-a",
                "completed",
                0.4,
                2,
                1,
                "2026-03-24T00:02:00Z",
                "2026-03-24T00:03:00Z",
                {"alpha": 1.5, "beta": 2.5},
                {"func_b": {"x_b": {"b2": 4.5}}, "func_a": {"x_a": {"a1": 3.5}}},
            ),
        ],
    )

    with sqlite3.connect(db_path) as conn:
        _, rows = load_run_history(conn, dataset_id="dataset-a")

    assert rows[0].input_param == {"alpha": 1.0, "beta": 2.0}
    assert rows[0].input_coef == {
        "func_b": {"x_b": {"b2": 4.0}},
        "func_a": {"x_a": {"a1": 3.0}},
    }
    assert parameter_column_names(rows) == ["alpha", "beta"]
    assert coefficient_column_names(rows) == ["func_a.x_a.a1", "func_b.x_b.b2"]

    frame = build_metrics_frame(rows, window=2)

    assert frame["alpha"].tolist() == [1.0, 1.5]
    assert frame["beta"].tolist() == [2.0, 2.5]
    assert frame["func_a.x_a.a1"].tolist() == [3.0, 3.5]
    assert frame["func_b.x_b.b2"].tolist() == [4.0, 4.5]


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


def test_render_input_trend_plots_paginates(tmp_path: Path) -> None:
    pytest.importorskip("matplotlib")

    frame = build_metrics_frame(stub_rows([0.1, 0.2, 0.15, 0.12, 0.18]), window=3)
    value_columns: list[str] = []
    for index in range(13):
        column = f"param_{index:02d}"
        frame[column] = [index + value for value in (0.0, 0.1, 0.2, 0.15, 0.18)]
        value_columns.append(column)

    output_path = tmp_path / "parameters_trend.png"
    plot_paths = render_input_trend_plots(
        frame,
        output_path,
        window=3,
        title_prefix="Parameter trends",
        value_columns=value_columns,
        max_subplots_per_page=12,
    )

    assert len(plot_paths) == 2
    for path in plot_paths:
        assert path.exists()
        assert path.stat().st_size > 0


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
                {"alpha": 1.0},
                {"func_a": {"x_a": {"beta_a": 0.1}}},
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
                {"alpha": 1.1},
                {"func_a": {"x_a": {"beta_a": 0.2}}},
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
                {"alpha": 1.2},
                {"func_a": {"x_a": {"beta_a": 0.3}}},
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
                {"alpha": 1.3},
                {"func_a": {"x_a": {"beta_a": 0.4}}},
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
                {"alpha": 1.4},
                {"func_a": {"x_a": {"beta_a": 0.5}}},
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
                {"alpha": 1.5},
                {"func_a": {"x_a": {"beta_a": 0.6}}},
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
    assert artifacts.parameter_plot_paths
    assert artifacts.coefficient_plot_paths
    summary_text = artifacts.summary_path.read_text(encoding="utf-8").lower()
    metrics_header = artifacts.metrics_path.read_text(encoding="utf-8").splitlines()[0]
    assert "rolling center:" in summary_text
    assert "rolling spread:" in summary_text
    assert "run range 1-6" in summary_text
    assert "best fit: 0.540000 at run 5 (round 1, trial 5)" in summary_text
    assert "rolling_std_3" in metrics_header
    assert "run_index" in metrics_header
    assert "alpha" in metrics_header
    assert "func_a.x_a.beta_a" in metrics_header


def test_analyze_latest_runs_uses_consecutive_run_index_across_round_resets(tmp_path: Path) -> None:
    pytest.importorskip("matplotlib")

    db_path = tmp_path / "bigpopa.db"
    output_root = tmp_path / "analysis"
    build_history_db(
        db_path,
        [
            (
                "m1",
                "dataset-r",
                "completed",
                0.90,
                3,
                1,
                "2026-03-24T00:00:00Z",
                "2026-03-24T00:01:00Z",
                {"alpha": 1.0},
                {"func_a": {"x_a": {"beta_a": 0.1}}},
            ),
            (
                "m2",
                "dataset-r",
                "completed",
                0.80,
                4,
                1,
                "2026-03-24T00:02:00Z",
                "2026-03-24T00:03:00Z",
                {"alpha": 1.1},
                {"func_a": {"x_a": {"beta_a": 0.2}}},
            ),
            (
                "m3",
                "dataset-r",
                "completed",
                0.70,
                1,
                1,
                "2026-03-24T00:04:00Z",
                "2026-03-24T00:05:00Z",
                {"alpha": 1.2},
                {"func_a": {"x_a": {"beta_a": 0.3}}},
            ),
            (
                "m4",
                "dataset-r",
                "completed",
                0.60,
                2,
                1,
                "2026-03-24T00:06:00Z",
                "2026-03-24T00:07:00Z",
                {"alpha": 1.3},
                {"func_a": {"x_a": {"beta_a": 0.4}}},
            ),
        ],
    )

    artifacts = analyze_latest_runs(
        bigpopa_db=db_path,
        output_root=output_root,
        limit=4,
        window=2,
    )

    with artifacts.metrics_path.open("r", encoding="utf-8", newline="") as handle:
        parsed_rows = list(csv.DictReader(handle))

    assert [int(row["run_index"]) for row in parsed_rows] == [1, 2, 3, 4]
    assert [int(row["trial_index"]) for row in parsed_rows] == [3, 4, 1, 2]

    summary_text = artifacts.summary_path.read_text(encoding="utf-8").lower()
    assert "run range 1-4" in summary_text
    assert "trial span" not in summary_text


def test_load_run_history_hides_fallback_fit_for_missing_fit_statuses(tmp_path: Path) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_history_db(
        db_path,
        [
            (
                "failed-run",
                "dataset-a",
                IFS_RUN_FAILED,
                1e6,
                1,
                1,
                "2026-03-24T00:00:00Z",
                "2026-03-24T00:01:00Z",
                None,
                None,
            ),
            (
                "fit-missing",
                "dataset-a",
                IFS_RUN_COMPLETED,
                1e6,
                2,
                1,
                "2026-03-24T00:02:00Z",
                "2026-03-24T00:03:00Z",
                None,
                None,
            ),
            (
                "fit-ok",
                "dataset-a",
                FIT_EVALUATED,
                0.4,
                3,
                1,
                "2026-03-24T00:04:00Z",
                "2026-03-24T00:05:00Z",
                None,
                None,
            ),
        ],
    )

    with sqlite3.connect(db_path) as conn:
        _, rows = load_run_history(conn, dataset_id="dataset-a")

    assert [(row.model_id, row.fit_missing, row.fit_pooled) for row in rows] == [
        ("failed-run", True, None),
        ("fit-missing", True, None),
        ("fit-ok", False, 0.4),
    ]


def test_load_run_history_whitespace_dataset_override_uses_latest_dataset(tmp_path: Path) -> None:
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
                None,
                None,
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
                None,
                None,
            ),
        ],
    )

    with sqlite3.connect(db_path) as conn:
        dataset_id, rows = load_run_history(conn, dataset_id="   ")

    assert dataset_id == "dataset-b"
    assert [row.model_id for row in rows] == ["newer-b"]


def test_load_run_history_invalid_dataset_override_has_clear_error(tmp_path: Path) -> None:
    db_path = tmp_path / "bigpopa.db"
    build_history_db(
        db_path,
        [
            (
                "m1",
                "dataset-a",
                "completed",
                0.9,
                1,
                1,
                "2026-03-24T00:00:00Z",
                "2026-03-24T00:01:00Z",
                None,
                None,
            ),
        ],
    )

    with sqlite3.connect(db_path) as conn:
        with pytest.raises(RuntimeError) as exc_info:
            load_run_history(conn, dataset_id="missing-dataset")

    assert "requested dataset_id='missing-dataset'" in str(exc_info.value)
    assert "Leave the dataset override blank to use the latest dataset." in str(exc_info.value)
