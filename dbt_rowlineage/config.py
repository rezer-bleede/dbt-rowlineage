"""Configuration object for dbt-rowlineage."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class RowLineageConfig:
    enabled: bool = True
    export_format: str = "jsonl"
    export_path: str | None = None

    @classmethod
    def from_vars(cls, vars_dict: Dict[str, Any]) -> "RowLineageConfig":
        settings = vars_dict or {}
        enabled = settings.get("rowlineage_enabled", True)
        export_format = settings.get("rowlineage_export_format", "jsonl")
        export_path = settings.get("rowlineage_export_path")
        return cls(enabled=enabled, export_format=export_format, export_path=export_path)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "rowlineage_enabled": self.enabled,
            "rowlineage_export_format": self.export_format,
            "rowlineage_export_path": self.export_path,
        }
