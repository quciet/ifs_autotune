from __future__ import annotations

from dataclasses import dataclass


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
            "Profile setting 'ml_method' is required and must be one of: "
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
