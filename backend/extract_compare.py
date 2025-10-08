from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
import subprocess
import pandas as pd


def log(status: str, message: str, **kwargs) -> None:
    payload = {"status": status, "message": message}
    payload.update(kwargs)
    print(json.dumps(payload), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract IFs output variables and compare with historical data",
    )
    parser.add_argument("--ifs-root", required=True, help="Path to IFs installation root")
    parser.add_argument("--model-db", required=True, help="Path to model_<id>.db from completed IFs run")
    parser.add_argument("--input-file", required=True, help="Path to StartingPointTable.xlsx")
    parser.add_argument("--model-id", required=True, help="Model ID string for output filenames")
    args = parser.parse_args()

    ifs_root = Path(args.ifs_root)
    model_db = Path(args.model_db)
    input_file = Path(args.input_file)
    model_id = args.model_id
    output_dir = model_db.parent

    model_folder = output_dir / f"model_{model_id}"
    if output_dir.name == f"model_{model_id}":
        model_folder = output_dir
    model_folder.mkdir(parents=True, exist_ok=True)

    log("info", "Reading DataDict sheet")
    df = pd.read_excel(input_file, sheet_name="DataDict")
    df = df[df["Switch"] == 1]
    if df.empty:
        log("warn", "No enabled rows found in DataDict sheet")
        return 0

    hist_db_path = ifs_root / "DATA" / "IFsHistSeries.db"
    if not hist_db_path.exists():
        log("error", f"Historical database not found: {hist_db_path}")
        return 1

    conn_model = sqlite3.connect(model_db)
    conn_hist = sqlite3.connect(hist_db_path)
    extracted: list[dict[str, str]] = []

    try:
        for _, row in df.iterrows():
            variable = str(row.get("Variable", "")).strip()
            table_name = str(row.get("Table", "")).strip()
            if not variable or not table_name:
                continue

            query = "SELECT Data FROM ifs_var_blob WHERE VariableName = ?"
            blob = conn_model.execute(query, (variable,)).fetchone()
            if not blob:
                log("warn", f"No Data found for {variable} in ifs_var_blob")
                continue

            raw_blob = blob[0]
            if not raw_blob:
                log("warn", f"No data found for {variable}")
                continue

            parquet_path = model_folder / f"{variable}_{model_id}.parquet"
            with open(parquet_path, "wb") as f:
                f.write(raw_blob)
            log("info", f"Saved Parquet for {variable}", file=str(parquet_path))

            try:
                hist_df = pd.read_sql_query(f"SELECT * FROM [{table_name}]", conn_hist)
                csv_path = model_folder / f"{table_name}_{model_id}.csv"
                hist_df.to_csv(csv_path, index=False)
                log("info", f"Saved historical data for {table_name}", file=str(csv_path))
            except Exception as exc:  # noqa: BLE001
                log("warn", f"Failed to extract table {table_name}: {exc}")

            extracted.append({"Variable": variable, "Table": table_name})
    finally:
        conn_model.close()
        conn_hist.close()

    log("success", "Extraction complete", count=len(extracted))
    print(json.dumps({"status": "success", "extracted": extracted}), flush=True)

    # === Convert Parquet → CSV using BIGPOPA's ParquetReaderlite ===
    try:

        backend_tools = Path(__file__).resolve().parent / "tools"
        parquet_reader = backend_tools / "ParquetReaderlite.exe"

        if parquet_reader.exists():
            subprocess.run([str(parquet_reader), str(model_folder)], check=True)
            log("info", f"Converted Parquet files in {model_folder} to CSV")
        else:
            log("warn", f"ParquetReaderlite.exe not found at {parquet_reader}")
    except Exception as exc:
        log("warn", f"Failed to convert Parquet files: {exc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
