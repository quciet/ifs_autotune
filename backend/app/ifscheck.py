import os
import sqlite3
import zipfile
from typing import Dict, Optional
from xml.etree import ElementTree

from fastapi import APIRouter

REQUIRED_PATHS = [
    "IFsInit.db",
    "DATA/SAMBase.db",
    "RUNFILES/DataDict.db",
    "RUNFILES/IFsHistSeries.db",
    "net8/ifs.exe",
    "RUNFILES/",
    "Scenario/",
]

REQUIRED_INPUT_SHEETS = ["AnalFunc", "TablFunc", "IFsVar", "DataDict"]


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


def _check_input_file(raw_path: Optional[str]) -> Dict[str, object]:
    provided = (raw_path or "").strip()
    absolute = os.path.abspath(provided) if provided else None
    display_path = absolute or (provided or None)
    sheets: Dict[str, bool] = {name: False for name in REQUIRED_INPUT_SHEETS}
    missing_sheets = list(REQUIRED_INPUT_SHEETS)
    exists = False
    readable = False
    message: Optional[str] = None

    if not absolute:
        message = "No path provided."
    elif not os.path.exists(absolute):
        message = "File does not exist."
    elif not os.path.isfile(absolute):
        message = "Path is not a file."
    elif not os.access(absolute, os.R_OK):
        exists = True
        message = "File is not readable."
    else:
        exists = True
        readable = True
        try:
            with zipfile.ZipFile(absolute) as archive:
                with archive.open("xl/workbook.xml") as workbook_xml:
                    content = workbook_xml.read()
        except KeyError:
            readable = False
            message = "Workbook metadata is missing."
        except (OSError, zipfile.BadZipFile) as exc:
            readable = False
            message = f"Unable to read workbook: {exc}"
        else:
            try:
                tree = ElementTree.fromstring(content)
            except ElementTree.ParseError as exc:
                readable = False
                message = f"Unable to parse workbook metadata: {exc}"
            else:
                namespace = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
                for sheet_node in tree.findall(f".//{namespace}sheet"):
                    name = sheet_node.attrib.get("name")
                    if name in sheets:
                        sheets[name] = True

                missing_sheets = [name for name, present in sheets.items() if not present]
                if missing_sheets:
                    message = "Missing sheets: " + ", ".join(missing_sheets)

    return {
        "displayPath": display_path,
        "exists": exists,
        "readable": readable,
        "message": message,
        "sheets": sheets,
        "missingSheets": missing_sheets,
    }


def validate_ifs_folder(
    path: str,
    output_path: Optional[str] = None,
    input_file: Optional[str] = None,
) -> dict:
    from backend.db_init import ensure_working_db

    ensure_working_db()
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
    input_file_check = _check_input_file(input_file)

    all_requirements_met = all(item["exists"] for item in requirements)
    output_ready = output_folder_check["exists"] and bool(output_folder_check["writable"])
    input_ready = (
        input_file_check["exists"]
        and input_file_check["readable"]
        and all(input_file_check["sheets"].get(name, False) for name in REQUIRED_INPUT_SHEETS)
    )

    return {
        "valid": all_requirements_met and output_ready and input_ready,
        "requirements": requirements,
        "base_year": base_year,
        "pathChecks": {
            "ifsFolder": ifs_folder_check,
            "outputFolder": output_folder_check,
            "inputFile": input_file_check,
        },
    }


@router.post("/check")
def check_folder(payload: dict) -> dict:
    return validate_ifs_folder(
        payload["path"],
        output_path=payload.get("outputPath"),
        input_file=payload.get("inputFile"),
    )
