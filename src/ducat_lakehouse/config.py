"""Load configs/category_rules.yml and configs/pipeline.yml."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass, fields
from pathlib import Path

import yaml

from ducat_lakehouse.rules import Rule, ValidationError, make_rule, sort_rules

_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str
    schema: str
    transfer_window_days: int = 4
    anomaly_z_threshold: float = 3.0
    anomaly_window_days: int = 90
    anomaly_min_history: int = 5

    def __post_init__(self):
        for name in ("catalog", "schema"):
            if not _IDENTIFIER.match(getattr(self, name)):
                raise ValidationError(f"pipeline config: {name} {getattr(self, name)!r} is not a plain identifier")
        if self.transfer_window_days < 0 or self.anomaly_window_days < 1 or self.anomaly_min_history < 2:
            raise ValidationError("pipeline config: window and history settings are out of range")

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"


def config_dir() -> Path:
    packaged = Path(__file__).resolve().parent / "configs"
    if packaged.is_dir():
        return packaged
    return Path(__file__).resolve().parents[2] / "configs"


def _read_yaml(path: Path) -> dict:
    with open(path, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValidationError(f"{path}: expected a mapping at the top level")
    return data


def load_pipeline_config(path: Path | None = None, **overrides) -> PipelineConfig:
    data = _read_yaml(path or config_dir() / "pipeline.yml")
    data.update({key: value for key, value in overrides.items() if value is not None})
    known = {f.name: f.type for f in fields(PipelineConfig)}
    unknown = sorted(set(data) - set(known))
    if unknown:
        raise ValidationError(f"pipeline config: unknown keys {unknown}")
    missing = [key for key in ("catalog", "schema") if not data.get(key)]
    if missing:
        raise ValidationError(f"pipeline config: missing {missing}")
    return PipelineConfig(
        catalog=str(data["catalog"]),
        schema=str(data["schema"]),
        transfer_window_days=int(data.get("transfer_window_days", 4)),
        anomaly_z_threshold=float(data.get("anomaly_z_threshold", 3.0)),
        anomaly_window_days=int(data.get("anomaly_window_days", 90)),
        anomaly_min_history=int(data.get("anomaly_min_history", 5)),
    )


def load_rules(path: Path | None = None) -> list[Rule]:
    data = _read_yaml(path or config_dir() / "category_rules.yml")
    entries = data.get("rules")
    if not isinstance(entries, list) or not entries:
        raise ValidationError("category rules: expected a non-empty 'rules' list")
    return sort_rules(make_rule(entry) for entry in entries)


def job_args(argv=None, *, volume_path: bool = False) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    if volume_path:
        parser.add_argument("--volume-path", required=True)
    return parser.parse_args(argv)
