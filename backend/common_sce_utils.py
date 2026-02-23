"""Shared helpers for writing IFs ``Scenario/Working.sce`` CUSTOM lines."""

from __future__ import annotations

from typing import Any, List, Optional


def parse_dimension_flag(raw_value: Any, tolerance: float = 1e-9) -> Optional[int]:
    """Normalize DIMENSION1-like values to 1, 0, or None.

    ``bigpopa.db.parameter.param_type`` is stored as TEXT (e.g. ``"1"``, ``"1.0"``,
    ``"0.0"``, empty string, ``NULL``), so callers should always parse through this
    helper before deciding whether to include ``World`` in ``CUSTOM`` lines.
    """

    if raw_value is None:
        return None

    value = raw_value
    if isinstance(raw_value, str):
        value = raw_value.strip()
        if value == "":
            return None

    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None

    if abs(numeric - 1.0) <= tolerance:
        return 1
    if abs(numeric - 0.0) <= tolerance:
        return 0
    return None


def build_custom_parts(
    param_name: str,
    dim_flag: Optional[int],
    years_count: int,
    value: float,
) -> Optional[List[str]]:
    """Build CSV pieces for a ``CUSTOM`` line.

    Policy:
    - dim_flag == 1: ``CUSTOM,<param>,World,<val repeated ...>``
    - dim_flag == 0: ``CUSTOM,<param>,<val repeated ...>``
    - dim_flag is None: skip parameter (returns ``None``)
    """

    if dim_flag not in (0, 1):
        return None
    if years_count <= 0:
        return None

    value_str = f"{float(value):.6f}".rstrip("0").rstrip(".")
    if not value_str:
        value_str = "0"
    repeated_values = [value_str] * years_count

    parts: List[str] = ["CUSTOM", param_name]
    if dim_flag == 1:
        parts.append("World")
    parts.extend(repeated_values)
    return parts
