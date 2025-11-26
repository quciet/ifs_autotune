import shutil
from pathlib import Path

def ensure_working_db():
    """
    Ensure that desktop/input/bigpopa.db exists.
    If missing, clone desktop/input/db_template/bigpopa_clean.db.
    """
    template = Path("desktop/input/db_template/bigpopa_clean.db")
    working = Path("desktop/input/bigpopa.db")

    # If already exists, nothing to do
    if working.exists():
        return

    # Template missing = developer configuration error
    if not template.exists():
        raise FileNotFoundError(
            "Clean database template not found at desktop/input/db_template/bigpopa_clean.db"
        )

    # Ensure folder exists and copy template → working db
    working.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(template, working)
    print("[BIGPOPA] Working bigpopa.db created from clean template.")
