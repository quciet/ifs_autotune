"""Utilities for combining IFs output with historical series data."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


def _melt_with_numeric_years(df: pd.DataFrame, value_name: str) -> pd.DataFrame:
    """Return a long-format dataframe with numeric year columns preserved."""
    id_vars = [col for col in df.columns if not str(col).isdigit()]
    long_df = df.melt(id_vars=id_vars, var_name="Year", value_name=value_name)
    long_df["Year"] = pd.to_numeric(long_df["Year"], errors="coerce")
    long_df = long_df[long_df["Year"].notna()].copy()
    long_df["Year"] = long_df["Year"].astype("Int64")
    return long_df


def _map_country_names(var_long: pd.DataFrame, dim_df: pd.DataFrame) -> pd.DataFrame:
    """Attach country names to the long format IFs dataframe."""
    for key in ("Dimension1", "Seq", "CountryID"):
        if key in var_long.columns:
            merged = var_long.merge(dim_df, left_on=key, right_on="CountryID", how="left")
            merged["CountryName"] = merged["CountryName"].fillna("World")
            return merged

    if "Name" in var_long.columns:
        merged = var_long.rename(columns={"Name": "CountryName"})
        merged["CountryName"] = merged["CountryName"].fillna("World")
        return merged

    var_long = var_long.copy()
    var_long["CountryName"] = "World"
    return var_long


def _map_hist_country_names(hist_long: pd.DataFrame, dim_df: pd.DataFrame) -> pd.DataFrame:
    """Attach country names to historical series data."""
    hist = hist_long.copy()

    if "CountryName" in hist.columns:
        hist["CountryName"] = hist["CountryName"].fillna("World")
        return hist

    if "Name" in hist.columns:
        hist = hist.rename(columns={"Name": "CountryName"})
        hist["CountryName"] = hist["CountryName"].fillna("World")
        return hist

    for key in ("Seq", "Dimension1", "CountryID"):
        if key in hist.columns:
            hist = hist.merge(dim_df, left_on=key, right_on="CountryID", how="left")
            hist["CountryName"] = hist["CountryName"].fillna("World")
            return hist

    hist["CountryName"] = "World"
    return hist


def combine_var_hist(model_db: Path, var_csv: Path, hist_csv: Path, output_csv: Path) -> None:
    """Combine IFs variable output with historical data in long format."""
    with sqlite3.connect(model_db) as conn:
        dim_df = pd.read_sql_query(
            "SELECT Seq AS CountryID, Name AS CountryName FROM ifs_dim_bucket WHERE DimensionId = 1",
            conn,
        )

    var_df = pd.read_csv(var_csv)
    hist_df = pd.read_csv(hist_csv)

    var_long = _melt_with_numeric_years(var_df, "IFs_Value")
    var_long = _map_country_names(var_long, dim_df)

    hist_long = _melt_with_numeric_years(hist_df, "Hist_Value")
    hist_long = _map_hist_country_names(hist_long, dim_df)

    merged = pd.merge(
        var_long,
        hist_long,
        on=["CountryName", "Year"],
        how="outer",
        suffixes=("", "_hist"),
    )

    merged["Year"] = merged["Year"].astype("Int64")

    # Ensure consistent column ordering and drop helper columns
    result_columns = ["CountryName", "Year", "IFs_Value", "Hist_Value"]
    for column in result_columns:
        if column not in merged.columns:
            merged[column] = pd.NA

    merged = merged[result_columns]
    merged.to_csv(output_csv, index=False)

