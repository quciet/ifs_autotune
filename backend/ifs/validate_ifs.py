from __future__ import annotations
import argparse
import json
import os
import sys
import shutil
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from db.ifs_metadata import ensure_static_metadata
from db.input_profiles import validate_profile
from db.migration import migrate_bigpopa_db_if_needed

####################################################
# 1. WORKING FILE INITIALIZERS (from db_init.py)
####################################################

def ensure_working_db() -> Dict[str, object] | None:
    """
    Ensure desktop/input/bigpopa.db exists. If missing, clone template.
    """
    template = Path("desktop/input/template/bigpopa_clean.db")
    working = Path("desktop/output/bigpopa.db")

    created = False
    if not working.exists():
        if not template.exists():
            raise FileNotFoundError("Missing template: desktop/input/template/bigpopa_clean.db")

        working.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(template, working)
        created = True
        print("[BIGPOPA] Created working bigpopa.db in desktop/output/ from clean template.")

    with sqlite3.connect(working) as conn:
        migration_summary = migrate_bigpopa_db_if_needed(
            conn,
            db_path=working,
            create_backup=True,
        )

    if migration_summary.get("performed"):
        print("[BIGPOPA] Upgraded working bigpopa.db to unified model_run schema.")

    if created and not migration_summary.get("performed"):
        return {
            "performed": False,
            "message": "Working BIGPOPA database is already on the unified schema.",
        }
    return migration_summary


def _initialize_working_files() -> Dict[str, object] | None:
    return ensure_working_db()

####################################################
# 2. IMPORT validate_ifs_folder() CONTENTS HERE
####################################################

# Validation logic now lives directly in this module.


REQUIRED_PATHS = [
    "IFsInit.db",
    "DATA/SAMBase.db",
    "RUNFILES/DataDict.db",
    "RUNFILES/IFsHistSeries.db",
    "net8/ifs.exe",
    "RUNFILES/",
    "Scenario/",
]

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



def _path_exists(base_path: Optional[str], required_path: str) -> bool:
    if not base_path:
        return False

    normalized = required_path[:-1] if required_path.endswith("/") else required_path
    absolute = os.path.join(base_path, normalized)
    if required_path.endswith("/"):
        return os.path.isdir(absolute)
    return os.path.isfile(absolute)



def _check_directory(
    raw_path: Optional[str], *, require_writable: bool = False
) -> Dict[str, object]:
    provided = (raw_path or "").strip()
    absolute = os.path.abspath(provided) if provided else None
    display_path = absolute or (provided or None)
    exists = False
    readable = False
    writable: Optional[bool] = None
    message: Optional[str] = None

    if not absolute:
        message = "No path provided."
    elif not os.path.exists(absolute):
        message = "Path does not exist."
    elif not os.path.isdir(absolute):
        message = "Path is not a directory."
    else:
        exists = True
        readable = os.access(absolute, os.R_OK)
        if not readable:
            message = "Directory is not readable."

        if require_writable:
            writable = os.access(absolute, os.W_OK)
            if writable is False:
                message = "Directory is not writable."
        else:
            writable = None

    return {
        "displayPath": display_path,
        "exists": exists,
        "readable": readable,
        "writable": writable,
        "message": message,
    }



def _check_input_profile(profile_id: Optional[int], *, message: Optional[str] = None) -> Dict[str, object]:
    has_profile = profile_id is not None
    return {
        "displayPath": str(profile_id) if profile_id is not None else None,
        "exists": has_profile,
        "readable": has_profile,
        "writable": None,
        "message": message or ("No input profile selected." if not has_profile else None),
        "profileId": profile_id,
        "valid": False,
        "errors": [],
        "profile": None,
    }



def validate_ifs_folder(
    path: str,
    output_path: Optional[str] = None,
    input_profile_id: Optional[int] = None,
    *,
    migration_summary: Optional[Dict[str, object]] = None,
) -> dict:
    if migration_summary is None:
        migration_summary = _initialize_working_files()

    sanitized_path = (path or "").strip()
    absolute_path = os.path.abspath(sanitized_path) if sanitized_path else None

    requirements = []
    for required in REQUIRED_PATHS:
        exists = _path_exists(absolute_path, required)
        requirements.append({"file": required, "exists": exists})

    base_year: Optional[int] = None

    if absolute_path:
        init_db = os.path.join(absolute_path, "IFsInit.db")
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

    ifs_folder_check = _check_directory(sanitized_path)
    if absolute_path:
        ifs_folder_check["displayPath"] = absolute_path

    output_folder_check = _check_directory(output_path, require_writable=True)
    input_profile_check = _check_input_profile(input_profile_id)

    all_requirements_met = all(item["exists"] for item in requirements)
    output_ready = output_folder_check["exists"] and bool(output_folder_check["writable"])
    ifs_static_id: Optional[int] = None
    static_metadata_error: Optional[str] = None

    if all_requirements_met and output_ready and base_year is not None and output_path:
        try:
            static_payload = ensure_static_metadata(
                ifs_root=Path(absolute_path),
                output_folder=Path(output_path).expanduser().resolve(),
                base_year=base_year,
            )
            ifs_static_id = int(static_payload["ifs_static_id"])
        except Exception as exc:
            static_metadata_error = str(exc)
            output_folder_check["message"] = static_metadata_error

    profile_ready = False
    if input_profile_id is not None and output_ready:
        try:
            validation_payload = validate_profile(
                output_folder=Path(output_path).expanduser().resolve(),
                profile_id=input_profile_id,
                ifs_root=absolute_path,
            )
            profile_summary = validation_payload["profile"]
            profile_validation = validation_payload["validation"]
            profile_ifs_static_id = int(profile_summary["ifs_static_id"])
            profile_errors = list(profile_validation.get("errors") or [])
            if ifs_static_id is not None and profile_ifs_static_id != ifs_static_id:
                profile_errors.append(
                    "Selected profile does not match the current IFs static layer."
                )
            profile_ready = len(profile_errors) == 0 and bool(profile_validation.get("valid"))
            input_profile_check = {
                "displayPath": profile_summary["name"],
                "exists": True,
                "readable": True,
                "writable": None,
                "message": None if profile_ready else "Profile validation failed.",
                "profileId": int(profile_summary["profile_id"]),
                "valid": profile_ready,
                "errors": profile_errors,
                "profile": profile_summary,
            }
        except Exception as exc:
            input_profile_check = _check_input_profile(
                input_profile_id,
                message=str(exc),
            )

    overall_valid = all_requirements_met and output_ready and static_metadata_error is None

    result = {
        "valid": overall_valid,
        "requirements": requirements,
        "base_year": base_year,
        "ifs_static_id": ifs_static_id,
        "profileReady": profile_ready,
        "pathChecks": {
            "ifsFolder": ifs_folder_check,
            "outputFolder": output_folder_check,
            "inputProfile": input_profile_check,
        },
    }
    info_messages: list[str] = []
    if migration_summary and migration_summary.get("performed"):
        backup_path = migration_summary.get("backup_path")
        backup_text = f" Backup: {backup_path}." if isinstance(backup_path, str) and backup_path else ""
        info_messages.append(
            "BIGPOPA database upgraded to the unified model_run schema." + backup_text
        )
    if info_messages:
        result["infoMessages"] = info_messages
    if migration_summary:
        result["dbMigration"] = migration_summary
    return result


def check_folder(payload: dict) -> dict:
    return validate_ifs_folder(
        payload["path"],
        output_path=payload.get("outputPath"),
        input_profile_id=payload.get("inputProfileId"),
    )

####################################################
# 3. CLI ENTRY POINT (existing validate_ifs.py logic)
####################################################

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate an IFs installation folder")
    parser.add_argument("path", nargs="?")
    parser.add_argument("--output-path", dest="output_path")
    parser.add_argument("--input-profile-id", dest="input_profile_id", type=int)

    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    # Initialize working copies
    migration_summary = _initialize_working_files()

    if not args.path:
        print(json.dumps({"valid": False, "missingFiles": ["No folder path provided"]}))
        return 1

    try:
        result = validate_ifs_folder(
            args.path,
            output_path=args.output_path,
            input_profile_id=args.input_profile_id,
            migration_summary=migration_summary,
        )
    except Exception:
        print(json.dumps({"valid": False, "missingFiles": ["Python error"]}))
        return 1

    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

