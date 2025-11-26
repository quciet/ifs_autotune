import shutil
from pathlib import Path

def ensure_working_db():
    """
    Ensure that desktop/input/bigpopa.db exists.
    If missing, copy desktop/input/template/bigpopa_clean.db.
    """
    template = Path("desktop/input/template/bigpopa_clean.db")
    working = Path("desktop/input/bigpopa.db")

    if working.exists():
        return

    if not template.exists():
        raise FileNotFoundError(
            "Clean DB template missing: desktop/input/template/bigpopa_clean.db"
        )

    working.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(template, working)
    print("[BIGPOPA] Working bigpopa.db created from clean template.")


def ensure_working_startingpointtable():
    """
    Ensure that desktop/input/StartingPointTable.xlsx exists.
    If missing, copy desktop/input/template/StartingPointTable_clean.xlsx.
    """
    template = Path("desktop/input/template/StartingPointTable_clean.xlsx")
    working = Path("desktop/input/StartingPointTable.xlsx")

    if working.exists():
        return

    if not template.exists():
        raise FileNotFoundError(
            "Clean StartingPointTable template missing: "
            "desktop/input/template/StartingPointTable_clean.xlsx"
        )

    working.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(template, working)
    print("[BIGPOPA] Working StartingPointTable.xlsx created from clean template.")
