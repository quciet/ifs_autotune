"""Extract IFs outputs, combine with history, and persist BIGPOPA metrics."""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
from pathlib import Path
from typing import Dict, List

import pandas as pd

from combine_var_hist import combine_var_hist


def log(status: str, message: str, **kwargs) -> None:
    payload = {"status": status, "message": message}
    if kwargs:
        payload.update(kwargs)
    print(json.dumps(payload), flush=True)


def ensure_model_output_table(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS model_output (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ifs_id INTEGER,
            model_id TEXT NOT NULL,
            model_status TEXT NOT NULL,
            fit_var TEXT,
            fit_pooled REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def fetch_cached(cursor: sqlite3.Cursor, model_id: str) -> tuple[str, str | None, float | None] | None:
    cursor.execute(
        """
        SELECT model_status, fit_var, fit_pooled
        FROM model_output
        WHERE model_id = ?
        ORDER BY rowid DESC LIMIT 1
        """,
        (model_id,),
    )
    return cursor.fetchone()


def write_fit_json(model_dir: Path, model_id: str, mse_map: Dict[str, float] | None, pooled_mse: float | None) -> Path:
    path = model_dir / f"fit_{model_id}.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump({"fit_var": mse_map, "fit_pooled": pooled_mse}, handle, indent=2)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract IFs output variables and compare with historical data",
    )
    parser.add_argument("--ifs-root", required=True, help="Path to IFs installation root")
    parser.add_argument(
        "--model-db", required=True, help="Path to model_<id>.run.db from completed IFs run"
    )
    parser.add_argument("--input-file", required=True, help="Path to StartingPointTable.xlsx")
    parser.add_argument("--model-id", required=True, help="Model ID string for output filenames")
    parser.add_argument("--ifs-id", required=True, type=int, help="IFs version identifier")
    args = parser.parse_args()

    ifs_root = Path(args.ifs_root)
    model_db = Path(args.model_db).resolve()
    input_file = Path(args.input_file)
    model_id = args.model_id
    ifs_id = args.ifs_id

    if model_db.parent.name == model_id:
        output_root = model_db.parent.parent
    else:
        output_root = model_db.parent
    model_dir = output_root / model_id
    model_dir.mkdir(parents=True, exist_ok=True)

    bigpopa_db_path = output_root / "bigpopa.db"
    bp = sqlite3.connect(str(bigpopa_db_path))
    bc = bp.cursor()
    ensure_model_output_table(bc)

    cached = fetch_cached(bc, model_id)
    if cached:
        status, fit_var_json, fit_pooled = cached
        fit_map = json.loads(fit_var_json) if fit_var_json else None
        write_fit_json(model_dir, model_id, fit_map, fit_pooled)
        print(
            json.dumps(
                {
                    "stage": "extract_compare",
                    "is_duplicate": True,
                    "message": "Reusing cached results",
                    "model_id": model_id,
                    "ifs_id": ifs_id,
                    "output": {
                        "model_status": status,
                        "fit_var": fit_map,
                        "fit_pooled": fit_pooled,
                    },
                }
            ),
            flush=True,
        )
        bp.close()
        return 0

    try:
        log("info", "Reading DataDict sheet")
        df = pd.read_excel(input_file, sheet_name="DataDict")
        df = df[df["Switch"] == 1]
        if df.empty:
            log("warn", "No enabled rows found in DataDict sheet")
            bc.execute(
                """
                INSERT INTO model_output (ifs_id, model_id, model_status, fit_var, fit_pooled)
                VALUES (?, ?, 'error', NULL, NULL)
                """,
                (ifs_id, model_id),
            )
            bp.commit()
            return 0

        hist_db_path = ifs_root / "RUNFILES" / "IFsHistSeries.db"
        if not hist_db_path.exists():
            log("error", f"Historical database not found: {hist_db_path}")
            bc.execute(
                """
                INSERT INTO model_output (ifs_id, model_id, model_status, fit_var, fit_pooled)
                VALUES (?, ?, 'error', NULL, NULL)
                """,
                (ifs_id, model_id),
            )
            bp.commit()
            return 1

        extracted: List[Dict[str, str]] = []
        with sqlite3.connect(model_db) as conn_model, sqlite3.connect(hist_db_path) as conn_hist:
            for _, row in df.iterrows():
                variable = str(row.get("Variable", "")).strip()
                table_name = str(row.get("Table", "")).strip()
                if not variable or not table_name:
                    continue

                blob = conn_model.execute(
                    "SELECT Data FROM ifs_var_blob WHERE VariableName = ?", (variable,)
                ).fetchone()
                if not blob:
                    log("warn", f"No Data found for {variable} in ifs_var_blob")
                    continue

                raw_blob = blob[0]
                if not raw_blob:
                    log("warn", f"No data found for {variable}")
                    continue

                parquet_path = model_dir / f"{variable}_{model_id}.parquet"
                with parquet_path.open("wb") as handle:
                    handle.write(raw_blob)
                log("info", f"Saved Parquet for {variable}", file=str(parquet_path))

                try:
                    hist_df = pd.read_sql_query(f"SELECT * FROM [{table_name}]", conn_hist)
                    csv_path = model_dir / f"{table_name}_{model_id}.csv"
                    hist_df.to_csv(csv_path, index=False)
                    log("info", f"Saved historical data for {table_name}", file=str(csv_path))
                except Exception as exc:  # noqa: BLE001
                    log("warn", f"Failed to extract table {table_name}: {exc}")

                extracted.append({"Variable": variable, "Table": table_name})

        log("success", "Extraction complete", count=len(extracted))
        print(json.dumps({"status": "success", "extracted": extracted}), flush=True)

        try:
            backend_tools = Path(__file__).resolve().parent / "tools"
            parquet_reader = backend_tools / "ParquetReaderlite.exe"
            if parquet_reader.exists():
                subprocess.run([str(parquet_reader), str(model_dir)], check=True)
                log("info", f"Converted Parquet files in {model_dir} to CSV")
            else:
                log("warn", f"ParquetReaderlite.exe not found at {parquet_reader}")
        except Exception as exc:  # noqa: BLE001
            log("warn", f"Failed to convert Parquet files: {exc}")

        fit_metrics: List[Dict[str, object]] = []
        total_sq_error = 0.0
        total_count = 0

        for item in extracted:
            var_name = item["Variable"]
            table_name = item["Table"]
            var_csv = model_dir / f"{var_name}_{model_id}.csv"
            hist_csv = model_dir / f"{table_name}_{model_id}.csv"
            if not var_csv.exists() or not hist_csv.exists():
                log(
                    "warn",
                    f"Skipping combination for {var_name}",
                    reason="missing CSV",
                    var_exists=var_csv.exists(),
                    hist_exists=hist_csv.exists(),
                )
                continue

            output_csv = model_dir / f"Combined_{var_name}_{model_id}.csv"
            try:
                combined_df = combine_var_hist(model_db, var_name, var_csv, hist_csv, output_csv)
                log(
                    "info",
                    f"Combined {var_name} with {table_name}",
                    file=str(output_csv),
                )
            except Exception as exc:  # noqa: BLE001
                log("warn", f"Failed to combine {var_name} with {table_name}: {exc}")
                fit_metrics.append({"Variable": var_name, "Table": table_name, "MSE": None})
                continue

            if not {"v", "v_h"}.issubset(combined_df.columns):
                log(
                    "warn",
                    f"Combined data for {var_name} missing required columns",
                    has_v="v" in combined_df.columns,
                    has_v_h="v_h" in combined_df.columns,
                )
                fit_metrics.append({"Variable": var_name, "Table": table_name, "MSE": None})
                continue

            valid = combined_df.dropna(subset=["v", "v_h"])
            if valid.empty:
                log("warn", f"No overlapping data to compute MSE for {var_name}")
                fit_metrics.append({"Variable": var_name, "Table": table_name, "MSE": None})
                continue

            squared_errors = (valid["v"] - valid["v_h"]) ** 2
            mse_v = squared_errors.mean()
            total_sq_error += squared_errors.sum()
            total_count += len(squared_errors)
            fit_metrics.append({"Variable": var_name, "Table": table_name, "MSE": mse_v})

        pooled_mse = total_sq_error / total_count if total_count > 0 else None
        mse_map = {
            metric["Variable"]: metric["MSE"]
            for metric in fit_metrics
            if metric.get("Variable") is not None
        }

        metrics_path = model_dir / f"fit_{model_id}.csv"
        metrics_df = pd.DataFrame(fit_metrics)
        if not metrics_df.empty:
            metrics_df["PooledMSE"] = pooled_mse
        else:
            metrics_df = pd.DataFrame(
                [{"Variable": None, "Table": None, "MSE": None, "PooledMSE": pooled_mse}]
            )
        metrics_df.to_csv(metrics_path, index=False)

        write_fit_json(model_dir, model_id, mse_map, pooled_mse)

        bc.execute(
            """
            INSERT INTO model_output (ifs_id, model_id, model_status, fit_var, fit_pooled)
            VALUES (?, ?, 'completed', ?, ?)
            """,
            (ifs_id, model_id, json.dumps(mse_map), pooled_mse),
        )
        bp.commit()

        if pooled_mse is not None:
            log(
                "success",
                f"Pooled MSE across variables: {pooled_mse:.6f}",
                file=str(metrics_path),
            )
        else:
            log("success", "Pooled MSE across variables: None", file=str(metrics_path))

        print(
            json.dumps(
                {
                    "status": "success",
                    "metrics_file": str(metrics_path),
                    "pooled_mse": pooled_mse,
                    "fit_metrics": fit_metrics,
                }
            ),
            flush=True,
        )

    except Exception:
        bc.execute(
            """
            INSERT INTO model_output (ifs_id, model_id, model_status, fit_var, fit_pooled)
            VALUES (?, ?, 'error', NULL, NULL)
            """,
            (ifs_id, model_id),
        )
        bp.commit()
        raise
    finally:
        bp.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
