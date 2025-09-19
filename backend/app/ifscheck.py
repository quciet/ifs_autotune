import os
import sqlite3
from typing import Optional

from fastapi import APIRouter

REQUIRED_ROOT_FILES = ["ifs.exe", "IFsInit.db"]
REQUIRED_DATA_FILES = ["SAMBase.db", "DataDict.db", "IFsHistSeries.db"]
REQUIRED_FOLDERS = ["net8", "RUNFILES", "Scenario", "DATA"]


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


def validate_ifs_folder(path: str) -> dict:
    missing = []

    # 1. Root files
    for f in REQUIRED_ROOT_FILES:
        if not os.path.exists(os.path.join(path, f)):
            missing.append(f)

    # 2. Required folders
    for folder in REQUIRED_FOLDERS:
        if not os.path.isdir(os.path.join(path, folder)):
            missing.append(folder)

    # 3. Data folder files
    data_folder = os.path.join(path, "DATA")
    for f in REQUIRED_DATA_FILES:
        if not os.path.exists(os.path.join(data_folder, f)):
            missing.append(f"DATA/{f}")

    base_year: Optional[int] = None

    # 4. Extract base year from IFsInit.db
    init_db = os.path.join(path, "IFsInit.db")
    if os.path.exists(init_db):
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
        "valid": len(missing) == 0,
        "missing": missing,
        "base_year": base_year,
    }


@router.post("/check")
def check_folder(payload: dict) -> dict:
    return validate_ifs_folder(payload["path"])
