"""Utilities for combining IFs output with historical series data."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from typing import Dict, List

import pandas as pd


def _read_var_dimensions(conn: sqlite3.Connection, var_name: str) -> pd.DataFrame:
    """Return metadata describing the dimensions for ``var_name``."""

    query = """
        SELECT VariableName, Seq, DimensionId
        FROM ifs_var_dim
        WHERE VariableName = ?
        ORDER BY Seq
    """
    var_dim = pd.read_sql_query(query, conn, params=(var_name,))

    if var_dim.empty:
        raise ValueError(f"No dimension metadata found for variable '{var_name}'.")

    return var_dim


def _decode_var_dimensions(
    conn: sqlite3.Connection, var_df: pd.DataFrame, var_dim: pd.DataFrame
) -> pd.DataFrame:
    """Decode IFs dimension identifiers into human readable names."""

    if var_df.shape[1] <= 1:
        raise ValueError("Variable dataframe must contain at least one dimension column and a value column.")

    if var_df.shape[1] - 1 != var_dim.shape[0]:
        raise ValueError(
            "Mismatch between IFs output dimensions and metadata: "
            f"{var_df.shape[1] - 1} columns vs {var_dim.shape[0]} dimensions."
        )

    decoded = var_df.copy()

    for col_index in range(decoded.shape[1] - 1):
        dim_row = var_dim.loc[var_dim["Seq"] == col_index]
        if dim_row.empty:
            raise ValueError(
                f"Missing dimension metadata for column index {col_index} in variable output."
            )

        dimension_id = int(dim_row.iloc[0]["DimensionId"])
        bucket = pd.read_sql_query(
            "SELECT Seq, Name FROM ifs_dim_bucket WHERE DimensionId = ?",
            conn,
            params=(dimension_id,),
        )

        mapping: Dict[int, str] = bucket.set_index("Seq")["Name"].to_dict()

        col_series = decoded.iloc[:, col_index]
        numeric_series = pd.to_numeric(col_series, errors="coerce")
        mapped = numeric_series.map(mapping)
        decoded.iloc[:, col_index] = mapped.fillna(col_series)

    column_names: List[str] = [str(seq) for seq in var_dim["Seq"].tolist()]
    column_names.append("IFs_Value")
    decoded.columns = column_names

    return decoded


def _prepare_hist_dataframe(
    hist_df: pd.DataFrame, dimension_seqs: List[int]
) -> pd.DataFrame:
    """Transform historical data into a format aligned with IFs dimensions."""

    cleaned = hist_df.drop(columns=["FIPS_CODE", "Earliest", "MostRecent"], errors="ignore").copy()

    numeric_columns = [col for col in cleaned.columns if str(col).isdigit()]
    id_columns = [col for col in cleaned.columns if col not in numeric_columns]

    if not id_columns:
        raise ValueError("Historical dataframe must contain at least one identifier column.")

    hist_long = cleaned.melt(id_vars=id_columns, var_name="Year", value_name="Hist_Value")

    sorted_seqs = sorted(dimension_seqs)
    if len(sorted_seqs) != len(id_columns) + 1:
        raise ValueError(
            "Historical identifiers do not align with IFs dimensions: "
            f"{len(id_columns)} id columns vs {len(sorted_seqs)} dimensions."
        )

    seq_iter = iter(sorted_seqs)
    year_seq = next(seq_iter)

    rename_map = {col: str(seq) for col, seq in zip(id_columns, seq_iter)}
    rename_map["Year"] = str(year_seq)

    hist_long = hist_long.rename(columns=rename_map)

    hist_long[str(year_seq)] = pd.to_numeric(hist_long[str(year_seq)], errors="coerce")

    return hist_long


def combine_var_hist(
    model_db: Path,
    var_name: str,
    var_csv: Path,
    hist_csv: Path,
    output_csv: Path,
) -> pd.DataFrame:
    """Combine IFs variable output with historical data using database metadata."""

    with sqlite3.connect(model_db) as conn:
        var_df = pd.read_csv(var_csv)
        hist_df = pd.read_csv(hist_csv)

        var_dim = _read_var_dimensions(conn, var_name)
        decoded_var = _decode_var_dimensions(conn, var_df, var_dim)

        dimension_seqs = var_dim["Seq"].tolist()
        hist_long = _prepare_hist_dataframe(hist_df, dimension_seqs)

        merge_keys = [str(seq) for seq in sorted(dimension_seqs)]
        combined_df = pd.merge(decoded_var, hist_long, how="left", on=merge_keys)

    combined_df.to_csv(output_csv, index=False)

    return combined_df

