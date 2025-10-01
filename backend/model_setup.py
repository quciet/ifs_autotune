"""Perform IFs model setup operations via a CLI entry point.

This script is invoked by the Electron shell to prepare the IFs working
environment before a model run. The heavy lifting is delegated to the
``IFsModel`` class provided by the IFs Python tooling when available. For
development and testing environments where the dependency is absent, a light
weight stub implementation is used so the flow can still succeed.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Mapping, Optional


try:  # pragma: no cover - the real implementation is optional in CI
    from IFsCoreModel import IFsModel as _IFsModel  # type: ignore[import]
except ImportError:  # pragma: no cover - executed when IFs dependencies are missing
    _IFsModel = None


def _ensure_mapping(value: object) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


class _StubIFsModel:
    """Fallback IFs model implementation for development environments."""

    def __init__(self, *, root_dir: str, yr_start: Optional[int], yr_end: int) -> None:
        self.root_dir = Path(root_dir)
        self.yr_start = yr_start
        self.yr_end = yr_end
        self.dir_runfiles = self.root_dir / "RUNFILES"
        self.dir_baserun = self.dir_runfiles / "IFsBase.run.db"
        self.dir_workingrun = self.dir_runfiles / "Working.run.db"
        self.dir_scenario = self.root_dir / "Scenario"
        self.parameters: Dict[str, Any] = {}
        self.coefficients: Dict[str, Any] = {}
        self.param_dim_dict: Dict[str, Any] = {}

    def get_param_coef(self, parameters: Mapping[str, Any], coefficients: Mapping[str, Any]) -> None:
        self.parameters = dict(parameters)
        self.coefficients = dict(coefficients)

    def get_param_dim(self, param_dim_dict: Mapping[str, Any]) -> None:
        self.param_dim_dict = dict(param_dim_dict)

    def create_sce(self) -> tuple[str, str]:
        self.dir_runfiles.mkdir(parents=True, exist_ok=True)
        sce_id = uuid.uuid4().hex
        sce_path = self.dir_runfiles / "Working.sce"
        content = {
            "sce_id": sce_id,
            "base_year": self.yr_start,
            "end_year": self.yr_end,
            "parameters": self.parameters,
            "coefficients": self.coefficients,
            "dimensions": self.param_dim_dict,
        }
        sce_path.write_text(json.dumps(content, indent=2), encoding="utf-8")
        return sce_id, str(sce_path)

    def update_beta_model(self) -> None:
        self.dir_runfiles.mkdir(parents=True, exist_ok=True)
        if self.dir_baserun.exists():
            shutil.copy2(self.dir_baserun, self.dir_workingrun)
        else:
            self.dir_workingrun.touch()


IFsModel = _IFsModel or _StubIFsModel


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Execute IFs model setup tasks.")
    parser.add_argument(
        "--payload",
        required=True,
        help="JSON encoded payload containing setup parameters.",
    )
    return parser


def _normalize_year(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        payload = json.loads(args.payload)
    except json.JSONDecodeError as exc:
        print(json.dumps({"status": "error", "message": f"Invalid payload: {exc}"}))
        return 1

    if not isinstance(payload, dict):
        print(json.dumps({"status": "error", "message": "Payload must be an object."}))
        return 1

    ifs_root = payload.get("ifs_root")
    if not isinstance(ifs_root, str) or not ifs_root.strip():
        print(json.dumps({"status": "error", "message": "Missing IFs root path."}))
        return 1

    end_year = _normalize_year(payload.get("endYear"))
    if end_year is None:
        print(json.dumps({"status": "error", "message": "Invalid end year provided."}))
        return 1

    base_year = _normalize_year(payload.get("baseYear"))
    parameters = _ensure_mapping(payload.get("parameters"))
    coefficients = _ensure_mapping(payload.get("coefficients"))
    param_dim = _ensure_mapping(payload.get("param_dim_dict"))

    try:
        model = IFsModel(root_dir=ifs_root, yr_start=base_year, yr_end=end_year)
        model.get_param_coef(parameters, coefficients)
        model.get_param_dim(param_dim)
        sce_id, sce_file = model.create_sce()
        model.update_beta_model()
    except Exception as exc:  # pragma: no cover - surface unexpected issues to the UI
        print(json.dumps({"status": "error", "message": str(exc)}))
        return 1

    print(json.dumps({"status": "success", "sce_id": sce_id, "sce_file": sce_file}))
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry point
    sys.exit(main())
