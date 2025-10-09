"""Utilities for combining IFs output with historical series data."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


def combine_var_hist(
    model_db: Path,
    var_name: str,
    var_csv: Path,
    hist_csv: Path,
    output_csv: Path,
) -> pd.DataFrame:
    """Combine an IFs variable extract with the matching historical series."""

    var_df = pd.read_csv(var_csv)
    hist_df = pd.read_csv(hist_csv)

    with sqlite3.connect(model_db) as conn:
        var_dim = pd.read_sql_query(
            """
            SELECT VariableName, Seq, DimensionId
            FROM ifs_var_dim
            WHERE VariableName = ?
            ORDER BY Seq
            """,
            conn,
            params=(var_name,),
        )

        if var_dim.empty:
            raise ValueError(f"No dimension metadata found for variable '{var_name}'.")

        dim_ids = ",".join(str(int(x)) for x in var_dim["DimensionId"].unique())
        dim_bucket = pd.read_sql_query(
            f"SELECT * FROM ifs_dim_bucket WHERE DimensionId IN ({dim_ids})",
            conn,
        )

    for seq in var_dim["Seq"]:
        seq = int(seq)
        col_index = seq - 1
        if col_index < 0 or col_index >= len(var_df.columns):
            raise IndexError(
                "Dimension sequence does not align with variable columns: "
                f"seq={seq}, columns={list(var_df.columns)}"
            )
        
        col = var_df.columns[col_index]
        dim_id = var_dim.loc[var_dim["Seq"] == seq, "DimensionId"].values[0]
        col_map = dim_bucket.loc[dim_bucket["DimensionId"] == dim_id, ["Seq", "Name"]].set_index("Seq")["Name"].to_dict()
        var_df[col] = var_df[col].map(col_map)

    # melt hist_df
    hist_df_long = hist_df.drop(columns=["FIPS_CODE", "Earliest", "MostRecent"]).melt(id_vars=["Country"], var_name="Year", value_name="v_h")
    hist_df_long = hist_df_long.rename(columns={"Country": "1", "Year": "0"})


        # col_name = var_df.columns[col_index]
        # original_series = var_df[col_name]
        # dim_id = int(var_dim.loc[var_dim["Seq"] == seq, "DimensionId"].values[0])
        # mapping = (
        #     dim_bucket.loc[dim_bucket["DimensionId"] == dim_id, ["Seq", "Name"]]
        #     .set_index("Seq")["Name"]
        #     .to_dict()
        # )
        # mapped_series = original_series.map(mapping)
        # var_df[col_name] = mapped_series.fillna(original_series)
        # var_df = var_df.rename(columns={col_name: str(seq)})

    # hist_df_long = (
    #     hist_df.drop(columns=["FIPS_CODE", "Earliest", "MostRecent"], errors="ignore")
    #     .melt(id_vars=["Country"], var_name="Year", value_name="v_h")
    #     .rename(columns={"Country": "1", "Year": "0"})
    # )

    # for key in ["0", "1"]:
    #     if key in var_df.columns:
    #         var_df[key] = (
    #             var_df[key].astype(str).str.replace(r"\.0$", "", regex=True)
    #         )
    #     if key in hist_df_long.columns:
    #         hist_df_long[key] = (
    #             hist_df_long[key].astype(str).str.replace(r"\.0$", "", regex=True)
    #         )

    combined_df = pd.merge(var_df, hist_df_long, how="left", on=["0", "1"])
    combined_df.to_csv(output_csv, index=False)

    return combined_df

