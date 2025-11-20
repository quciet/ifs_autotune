"""Placeholder module for applying BIGPOPA inputs to IFs working files.

This module will later move helper functions from ``model_setup.py`` to apply
parameter and coefficient values from ``bigpopa.model_input`` into
``RUNFILES/Working.run.db`` and ``Scenario/Working.sce`` as part of the ML loop.
Runtime behavior will be added in a future update.
"""


def apply_config_to_ifs_files(
    ifs_root,
    input_param,
    input_coef,
    base_year,
    end_year,
):
    """
    Write parameter values into Scenario/Working.sce and coefficient values into
    RUNFILES/Working.run.db so run_ifs.py can execute a fully configured IFs run.
    This is the bridge between model_input (in bigpopa.db) and the IFs engine.
    """

    import sqlite3
    from pathlib import Path

    # 1. Write parameters to Scenario/Working.sce
    sce_path = Path(ifs_root) / "Scenario" / "Working.sce"
    sce_path.parent.mkdir(parents=True, exist_ok=True)
    sce_path.unlink(missing_ok=True)
    sce_path.touch()

    years = end_year - base_year + 1

    with sce_path.open("w", encoding="utf-8") as f:
        for param, val in input_param.items():
            vals = [f"{float(val):.6f}"] * years
            line = ",".join(["CUSTOM", param, "World"] + vals)
            f.write(line + "\n")

    # 2. Update coefficients in RUNFILES/Working.run.db
    db_path = Path(ifs_root) / "RUNFILES" / "Working.run.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    for func, x_map in input_coef.items():
        for x_name, beta_map in x_map.items():

            cur.execute(
                """
                SELECT Seq FROM ifs_reg
                WHERE UPPER(Name)=UPPER(?)
                  AND UPPER(InputName)=UPPER(?)
                """,
                (func, x_name),
            )
            seq_row = cur.fetchone()
            if not seq_row:
                continue

            seq = seq_row[0]

            for beta_name, new_val in beta_map.items():
                cur.execute(
                    """
                    UPDATE ifs_reg_coeff
                    SET Value = ?
                    WHERE RegressionName = ?
                      AND RegressionSeq = ?
                      AND Name = ?
                    """,
                    (float(new_val), func, seq, beta_name),
                )

    conn.commit()
    conn.close()
