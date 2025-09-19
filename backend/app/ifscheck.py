import os
import sqlite3
from typing import Optional

from fastapi import APIRouter

REQUIRED_PATHS = [
    "IFsInit.db",
    "DATA/SAMBase.db",
    "DATA/DataDict.db",
    "DATA/IFsHistSeries.db",
    "net8/ifs.exe",
    "RUNFILES/",
    "Scenario/",
]


router = APIRouter()



def _extract_year(raw_value: object) -> Optional[int]:
    if raw_value is None:
        return None
    try:
        if isinstance(raw_value, str):
            raw_value = raw_value.strip()
            if not raw_value:
                return None
        return int(float(raw_value))
    except (TypeError, ValueError):
        return None


def _fetch_year(cur: sqlite3.Cursor, like_pattern: str) -> Optional[int]:
    cur.execute(
        "SELECT Value FROM IFsInit WHERE Variable LIKE ? ORDER BY Variable LIMIT 1",
        (like_pattern,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return _extract_year(row[0])


def _path_exists(base_path: str, required_path: str) -> bool:
    normalized = required_path[:-1] if required_path.endswith("/") else required_path
    absolute = os.path.join(base_path, normalized)
    if required_path.endswith("/"):
        return os.path.isdir(absolute)
    return os.path.isfile(absolute)


def validate_ifs_folder(path: str) -> dict:
    requirements = []
    for required in REQUIRED_PATHS:
        exists = _path_exists(path, required)
        requirements.append({"file": required, "exists": exists})

    base_year: Optional[int] = None

    init_db = os.path.join(path, "IFsInit.db")
    if os.path.isfile(init_db):
        try:
            with sqlite3.connect(init_db) as con:
                cur = con.cursor()
                history_year = _fetch_year(cur, "LastYearHistory%")
                forecast_year = _fetch_year(cur, "FirstYearForecast%")

            if history_year and forecast_year:
                # Prefer the last historical year when available; fall back to
                # the forecast start if it's the only consistent option.
                if history_year <= forecast_year:
                    base_year = history_year
                else:
                    base_year = forecast_year
            else:
                base_year = history_year or forecast_year
        except sqlite3.Error:
            base_year = None

    return {
        "valid": all(item["exists"] for item in requirements),
        "requirements": requirements,
        "base_year": base_year,
    }


@router.post("/check")
def check_folder(payload: dict) -> dict:
    return validate_ifs_folder(payload["path"])
