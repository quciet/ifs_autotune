from __future__ import annotations

import os

FALLBACK_FIT_POOLED: float = float(os.getenv("BIGPOPA_FAIL_Y", "1e6"))

MODEL_REUSED = "model_reused"
IFS_CONFIG_APPLIED = "ifs_config_applied"
IFS_RUN_STARTED = "ifs_run_started"
IFS_RUN_FAILED = "ifs_run_failed"
IFS_RUN_COMPLETED = "ifs_run_completed"
FIT_EVALUATED = "fit_evaluated"

LEGACY_REUSED = "reused"
LEGACY_COMPLETED = "completed"
LEGACY_FAILED = "failed"
LEGACY_EVALUATED = "evaluated"
LEGACY_ERROR = "error"

_MISSING_FIT_STATUSES = frozenset(
    {
        IFS_RUN_FAILED,
        IFS_RUN_COMPLETED,
        LEGACY_FAILED,
        LEGACY_ERROR,
    }
)


def fit_is_missing(status: str | None, fit_pooled: float | None) -> bool:
    if status in _MISSING_FIT_STATUSES:
        return True
    return fit_pooled is None


def visible_fit_pooled(status: str | None, fit_pooled: float | None) -> float | None:
    if fit_is_missing(status, fit_pooled):
        return None
    return float(fit_pooled)


def cached_result_status(existing_status: str | None, fit_pooled: float | None) -> str | None:
    if fit_pooled is None:
        return None
    if existing_status in {IFS_RUN_FAILED, LEGACY_FAILED, LEGACY_ERROR}:
        return IFS_RUN_FAILED
    if existing_status == IFS_RUN_COMPLETED:
        return IFS_RUN_COMPLETED
    return MODEL_REUSED
