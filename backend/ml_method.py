from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


ALLOWED_ML_METHOD_ALIASES: dict[str, tuple[str, str]] = {
    "neural network": ("neural network", "nn"),
    "nn": ("neural network", "nn"),
    "poly": ("poly", "poly"),
    "polynomial": ("poly", "poly"),
    "tree": ("tree", "tree"),
}


@dataclass(frozen=True)
class MLMethodConfig:
    raw_value: str
    normalized_value: str
    model_type: str


def normalize_ml_method(raw_value: object) -> MLMethodConfig:
    text = str(raw_value or "").strip().lower()
    if not text:
        raise ValueError(
            "ML sheet parameter 'ml_method' is required and must be one of: "
            "neural network, nn, poly, polynomial, tree."
        )

    resolved = ALLOWED_ML_METHOD_ALIASES.get(text)
    if resolved is None:
        raise ValueError(
            "Unsupported ML method "
            f"'{raw_value}'. Allowed values are: neural network, nn, poly, polynomial, tree."
        )

    normalized_value, model_type = resolved
    return MLMethodConfig(
        raw_value=str(raw_value).strip(),
        normalized_value=normalized_value,
        model_type=model_type,
    )


def _read_ml_sheet(starting_point_table: Path) -> pd.DataFrame:
    if not starting_point_table.exists():
        raise ValueError(
            f"StartingPointTable.xlsx was not found at '{starting_point_table}'. "
            "A valid ML sheet with ml_method is required."
        )

    try:
        return pd.read_excel(starting_point_table, sheet_name="ML", engine="openpyxl")
    except ValueError as exc:
        raise ValueError(
            "StartingPointTable.xlsx must contain an 'ML' sheet with an 'ml_method' row."
        ) from exc
    except Exception as exc:
        raise ValueError(
            f"Unable to read the 'ML' sheet from '{starting_point_table}'."
        ) from exc


def load_required_ml_method(starting_point_table: Path) -> MLMethodConfig:
    df = _read_ml_sheet(starting_point_table)

    ml_method_value: object | None = None
    for _, row in df.iterrows():
        method = str(row.get("Method") or "").strip().lower()
        if method != "general":
            continue

        parameter = str(row.get("Parameter") or "").strip().lower()
        if parameter == "ml_method":
            ml_method_value = row.get("Value")

    if ml_method_value is None:
        raise ValueError(
            "ML sheet must define a 'general' / 'ml_method' row. "
            "Allowed values are: neural network, nn, poly, polynomial, tree."
        )

    return normalize_ml_method(ml_method_value)
