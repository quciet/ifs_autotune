from __future__ import annotations

import argparse
import io
import json
import sqlite3
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


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
            if blob and blob[0]:
                parquet_bytes = io.BytesIO(blob[0])
                parquet_table = pq.read_table(parquet_bytes)
                parquet_path = output_dir / f"{variable}_{model_id}.parquet"
                pq.write_table(parquet_table, parquet_path)
                log("info", f"Saved parquet for {variable}", file=str(parquet_path))
            else:
                log("warn", f"No Data found for {variable} in ifs_var_blob")
                continue

            try:
                hist_df = pd.read_sql_query(f"SELECT * FROM [{table_name}]", conn_hist)
                csv_path = output_dir / f"{table_name}_{model_id}.csv"
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
