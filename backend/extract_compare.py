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


# Emit a structured response for Electron consumption.
def emit_stage_response(status: str, stage: str, message: str, data: Dict[str, object]) -> None:
    payload = {
        "status": status,
        "stage": stage,
        "message": message,
        "data": data,
    }
    print(json.dumps(payload), flush=True)


# Ensure BIGPOPA schema matches the hashed model workflow expectations.
def ensure_bigpopa_schema(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS model_input (
            ifs_id INTEGER,
            model_id TEXT PRIMARY KEY,
            input_param TEXT,
            input_coef TEXT,
            output_set TEXT,
            FOREIGN KEY (ifs_id) REFERENCES ifs_version(ifs_id)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS model_output (
            ifs_id INTEGER,
            model_id TEXT PRIMARY KEY,
            model_status TEXT,
            fit_var TEXT,
            fit_pooled REAL,
            FOREIGN KEY (ifs_id) REFERENCES ifs_version(ifs_id),
            FOREIGN KEY (model_id) REFERENCES model_input(model_id)
        )
        """
    )


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
    parser.add_argument(
        "--bigpopa-db", required=False, help="Path to the BIGPOPA database."
    )
    args = parser.parse_args()

    ifs_root = Path(args.ifs_root)
    model_db = Path(args.model_db).resolve()
    input_file = Path(args.input_file)
    model_id = args.model_id
    ifs_id = args.ifs_id
    bigpopa_override = Path(args.bigpopa_db).resolve() if args.bigpopa_db else None

    if not model_db.exists():
        log("error", "Model database not found", model_db=str(model_db))
        emit_stage_response(
            "error",
            "extract_compare",
            "Model database was not found; cannot compute fit metrics.",
            {"model_db": str(model_db)},
        )
        return 1

    model_dir = model_db.parent
    if model_dir.name != model_id:
        alternate_dir = model_dir / model_id
        if alternate_dir.exists():
            model_dir = alternate_dir
        else:
            log(
                "error",
                "Model directory mismatch",
                expected=model_id,
                actual=str(model_dir),
            )
            emit_stage_response(
                "error",
                "extract_compare",
                "Model directory does not match the provided model_id.",
                {"model_id": model_id, "model_dir": str(model_dir)},
            )
            return 1

    if not model_dir.exists():
        log("error", "Model output folder missing", folder=str(model_dir))
        emit_stage_response(
            "error",
            "extract_compare",
            "Model output folder is missing; run_ifs must complete before comparison.",
            {"model_id": model_id, "run_folder": str(model_dir)},
        )
        return 1

    # Locate the BIGPOPA database adjacent to the output folder unless overridden.
    if bigpopa_override is not None:
        bigpopa_db_path = bigpopa_override
    else:
        if model_dir.parent.name.lower() == "output":
            bigpopa_root = model_dir.parent.parent
        else:
            bigpopa_root = model_dir.parent
        bigpopa_db_path = bigpopa_root / "bigpopa.db"

    if not bigpopa_db_path.exists():
        log("error", "BIGPOPA database missing", database=str(bigpopa_db_path))
        emit_stage_response(
            "error",
            "extract_compare",
            "BIGPOPA database was not found for metric persistence.",
            {"bigpopa_db": str(bigpopa_db_path)},
        )
        return 1

    try:
        bp = sqlite3.connect(str(bigpopa_db_path))
    except sqlite3.Error as exc:
        emit_stage_response(
            "error",
            "extract_compare",
            "Unable to open BIGPOPA database.",
            {"bigpopa_db": str(bigpopa_db_path), "error": str(exc)},
        )
        return 1

    try:
        bc = bp.cursor()
        ensure_bigpopa_schema(bc)
        bc.execute(
            "SELECT model_status FROM model_output WHERE model_id = ?",
            (model_id,),
        )
        existing_status_row = bc.fetchone()
        log(
            "debug",
            "Fetched existing model status",
            model_id=model_id,
            status=existing_status_row[0] if existing_status_row else None,
        )
        bp.commit()
    except sqlite3.Error as exc:
        bp.close()
        emit_stage_response(
            "error",
            "extract_compare",
            "Failed to query BIGPOPA database for model status.",
            {"model_id": model_id, "error": str(exc)},
        )
        return 1

    try:
        log("info", "Reading output_set from BIGPOPA database")
        cursor = bp.cursor()
        cursor.execute("SELECT output_set FROM model_input WHERE model_id = ?", (model_id,))
        row = cursor.fetchone()

        if not row or not row[0]:
            log("error", "No output_set found in model_input for this model_id", model_id=model_id)
            emit_stage_response(
                "error",
                "extract_compare",
                "No output_set found in model_input for this model.",
                {"model_id": model_id},
            )
            return 1

        try:
            output_set = json.loads(row[0])
        except Exception as exc:
            log("error", f"Failed to parse output_set JSON: {exc}", model_id=model_id)
            emit_stage_response(
                "error",
                "extract_compare",
                "Failed to parse output_set JSON.",
                {"model_id": model_id, "error": str(exc)},
            )
            return 1

        if not output_set:
            log("warn", "Output_set is empty; nothing to extract", model_id=model_id)
            emit_stage_response(
                "error",
                "extract_compare",
                "No output_set data available for this model.",
                {"model_id": model_id},
            )
            return 1

    except Exception as exc:
        bp.close()
        emit_stage_response(
            "error",
            "extract_compare",
            "Failed to read output_set from BIGPOPA database.",
            {"model_id": model_id, "error": str(exc)},
        )
        return 1

    try:
        hist_db_path = ifs_root / "RUNFILES" / "IFsHistSeries.db"
        if not hist_db_path.exists():
            log("error", f"Historical database not found: {hist_db_path}")
            with bp:
                cursor = bp.cursor()
                cursor.execute(
                    """
                    UPDATE model_output
                    SET model_status='error', fit_var=NULL, fit_pooled=NULL
                    WHERE model_id=?
                    """,
                    (model_id,),
                )
                if cursor.rowcount == 0:
                    cursor.execute(
                        """
                        INSERT INTO model_output (ifs_id, model_id, model_status, fit_var, fit_pooled)
                        VALUES (?, ?, 'error', NULL, NULL)
                        """,
                        (ifs_id, model_id),
                    )
            emit_stage_response(
                "error",
                "extract_compare",
                "Historical database not found for comparison.",
                {"hist_db": str(hist_db_path)},
            )
            return 1

        extracted: List[Dict[str, str]] = []
        with sqlite3.connect(model_db) as conn_model, sqlite3.connect(hist_db_path) as conn_hist:
            for variable, table_name in output_set.items():
                variable = str(variable).strip()
                table_name = str(table_name).strip()
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

        with bp:
            cursor = bp.cursor()
            cursor.execute(
                """
                UPDATE model_output
                SET model_status='evaluated', fit_var=?, fit_pooled=?
                WHERE model_id=?
                """,
                (json.dumps(mse_map), pooled_mse, model_id),
            )
            if cursor.rowcount == 0:
                cursor.execute(
                    """
                    INSERT INTO model_output (ifs_id, model_id, model_status, fit_var, fit_pooled)
                    VALUES (?, ?, 'evaluated', ?, ?)
                    """,
                    (ifs_id, model_id, json.dumps(mse_map), pooled_mse),
                )

        if pooled_mse is not None:
            log(
                "success",
                f"Pooled MSE across variables: {pooled_mse:.6f}",
                file=str(metrics_path),
            )
        else:
            log("success", "Pooled MSE across variables: None", file=str(metrics_path))

        emit_stage_response(
            "success",
            "extract_compare",
            "Model comparison complete.",
            {"model_id": model_id, "fit_pooled": pooled_mse, "fit_var": mse_map},
        )
        return 0
    except Exception as exc:  # noqa: BLE001
        with bp:
            cursor = bp.cursor()
            cursor.execute(
                """
                UPDATE model_output
                SET model_status='error', fit_var=NULL, fit_pooled=NULL
                WHERE model_id=?
                """,
                (model_id,),
            )
            if cursor.rowcount == 0:
                cursor.execute(
                    """
                    INSERT INTO model_output (ifs_id, model_id, model_status, fit_var, fit_pooled)
                    VALUES (?, ?, 'error', NULL, NULL)
                    """,
                    (ifs_id, model_id),
                )
        emit_stage_response(
            "error",
            "extract_compare",
            "Model comparison failed.",
            {"model_id": model_id, "error": str(exc)},
        )
        return 1
    finally:
        bp.close()


if __name__ == "__main__":
    raise SystemExit(main())
