from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from runtime.artifact_retention import (
    RETENTION_ALL,
    RETENTION_BEST_ONLY,
    RETENTION_NONE,
    finalize_model_artifacts,
    reset_directory,
    retained_all_dir,
    retained_best_dir,
    staging_dir,
)
from runtime.model_run_store import insert_model_run
from runtime.model_status import FIT_EVALUATED
from db.schema import ensure_current_bigpopa_schema


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    ensure_current_bigpopa_schema(conn.cursor())
    return conn


def _insert_completed_run(
    conn: sqlite3.Connection,
    *,
    dataset_id: str,
    model_id: str,
    fit_pooled: float,
) -> None:
    insert_model_run(
        conn,
        ifs_id=1,
        model_id=model_id,
        dataset_id=dataset_id,
        input_param={"a": fit_pooled},
        input_coef={},
        output_set={"fit": 1},
        model_status=FIT_EVALUATED,
        fit_pooled=fit_pooled,
        trial_index=1,
        batch_index=1,
        started_at_utc="2026-04-06T00:00:00Z",
        completed_at_utc="2026-04-06T00:01:00Z",
    )


def test_finalize_model_artifacts_deletes_staged_dir_for_none(tmp_path: Path) -> None:
    conn = _conn()
    try:
        _insert_completed_run(conn, dataset_id="dataset-1", model_id="model-1", fit_pooled=5.0)
        output_dir = tmp_path / "output"
        staged = reset_directory(staging_dir(output_dir, "model-1"))
        (staged / "artifact.txt").write_text("temp", encoding="utf-8")

        retained = finalize_model_artifacts(
            conn=conn,
            output_dir=output_dir,
            model_id="model-1",
            dataset_id="dataset-1",
            mode=RETENTION_NONE,
            staged_dir=staged,
        )

        assert retained is None
        assert not staged.exists()
    finally:
        conn.close()


def test_finalize_model_artifacts_moves_to_all_dir(tmp_path: Path) -> None:
    conn = _conn()
    try:
        _insert_completed_run(conn, dataset_id="dataset-1", model_id="model-1", fit_pooled=5.0)
        output_dir = tmp_path / "output"
        staged = reset_directory(staging_dir(output_dir, "model-1"))
        (staged / "artifact.txt").write_text("all", encoding="utf-8")

        retained = finalize_model_artifacts(
            conn=conn,
            output_dir=output_dir,
            model_id="model-1",
            dataset_id="dataset-1",
            mode=RETENTION_ALL,
            staged_dir=staged,
        )

        assert retained == retained_all_dir(output_dir, "model-1")
        assert retained is not None and retained.exists()
        assert (retained / "artifact.txt").read_text(encoding="utf-8") == "all"
        assert not staged.exists()
    finally:
        conn.close()


def test_finalize_model_artifacts_replaces_best_only_dir(tmp_path: Path) -> None:
    conn = _conn()
    try:
        output_dir = tmp_path / "output"
        _insert_completed_run(conn, dataset_id="dataset-1", model_id="model-1", fit_pooled=5.0)
        staged_first = reset_directory(staging_dir(output_dir, "model-1"))
        (staged_first / "artifact.txt").write_text("first best", encoding="utf-8")

        first_retained = finalize_model_artifacts(
            conn=conn,
            output_dir=output_dir,
            model_id="model-1",
            dataset_id="dataset-1",
            mode=RETENTION_BEST_ONLY,
            staged_dir=staged_first,
        )

        assert first_retained == retained_best_dir(output_dir, "dataset-1")
        assert first_retained is not None
        assert (first_retained / "artifact.txt").read_text(encoding="utf-8") == "first best"

        _insert_completed_run(conn, dataset_id="dataset-1", model_id="model-2", fit_pooled=4.0)
        staged_second = reset_directory(staging_dir(output_dir, "model-2"))
        (staged_second / "artifact.txt").write_text("second best", encoding="utf-8")

        second_retained = finalize_model_artifacts(
            conn=conn,
            output_dir=output_dir,
            model_id="model-2",
            dataset_id="dataset-1",
            mode=RETENTION_BEST_ONLY,
            staged_dir=staged_second,
        )

        assert second_retained == retained_best_dir(output_dir, "dataset-1")
        assert second_retained is not None
        assert (second_retained / "artifact.txt").read_text(encoding="utf-8") == "second best"
        assert not staged_second.exists()
    finally:
        conn.close()
