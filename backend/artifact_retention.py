from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

from model_status import visible_fit_pooled
from tools.db.bigpopa_schema import MODEL_RUN_TABLE, ensure_current_bigpopa_schema

RETENTION_NONE = "none"
RETENTION_BEST_ONLY = "best_only"
RETENTION_ALL = "all"
RETENTION_MODES = frozenset({RETENTION_NONE, RETENTION_BEST_ONLY, RETENTION_ALL})


def normalize_artifact_retention_mode(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    if normalized in RETENTION_MODES:
        return normalized
    return RETENTION_NONE


def artifact_root(output_dir: Path) -> Path:
    return output_dir / "model_artifacts"


def staging_dir(output_dir: Path, model_id: str) -> Path:
    return artifact_root(output_dir) / "tmp" / model_id


def retained_all_dir(output_dir: Path, model_id: str) -> Path:
    return artifact_root(output_dir) / "all" / model_id


def retained_best_dir(output_dir: Path, dataset_id: str) -> Path:
    return artifact_root(output_dir) / "best" / dataset_id


def reset_directory(path: Path) -> Path:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def delete_directory(path: Path | None) -> None:
    if path is None or not path.exists():
        return
    shutil.rmtree(path)


def _best_model_id_for_dataset(conn: sqlite3.Connection, dataset_id: str | None) -> str | None:
    cursor = conn.cursor()
    ensure_current_bigpopa_schema(cursor)
    if dataset_id is None:
        rows = cursor.execute(
            f"""
            SELECT model_id, model_status, fit_pooled, run_id
            FROM {MODEL_RUN_TABLE}
            WHERE dataset_id IS NULL
            ORDER BY run_id ASC
            """
        ).fetchall()
    else:
        rows = cursor.execute(
            f"""
            SELECT model_id, model_status, fit_pooled, run_id
            FROM {MODEL_RUN_TABLE}
            WHERE dataset_id = ?
            ORDER BY run_id ASC
            """,
            (dataset_id,),
        ).fetchall()
    best_model_id: str | None = None
    best_fit: float | None = None
    for row in rows:
        candidate_model_id = str(row[0]) if row and row[0] is not None else None
        candidate_fit = visible_fit_pooled(
            row[1] if isinstance(row[1], str) or row[1] is None else str(row[1]),
            float(row[2]) if row[2] is not None else None,
        )
        if candidate_model_id is None or candidate_fit is None:
            continue
        if best_fit is None or candidate_fit < best_fit:
            best_fit = candidate_fit
            best_model_id = candidate_model_id
    return best_model_id


def finalize_model_artifacts(
    *,
    conn: sqlite3.Connection,
    output_dir: Path,
    model_id: str,
    dataset_id: str | None,
    mode: str,
    staged_dir: Path | None,
) -> Path | None:
    normalized_mode = normalize_artifact_retention_mode(mode)
    if staged_dir is None or not staged_dir.exists():
        return None

    if normalized_mode == RETENTION_NONE:
        delete_directory(staged_dir)
        return None

    if normalized_mode == RETENTION_ALL:
        destination = retained_all_dir(output_dir, model_id)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            shutil.rmtree(destination)
        staged_dir.replace(destination)
        return destination

    if dataset_id is None:
        delete_directory(staged_dir)
        return None

    best_model_id = _best_model_id_for_dataset(conn, dataset_id)
    if best_model_id != model_id:
        delete_directory(staged_dir)
        return None

    destination = retained_best_dir(output_dir, dataset_id)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        shutil.rmtree(destination)
    staged_dir.replace(destination)
    return destination
