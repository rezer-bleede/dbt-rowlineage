"""Plugin entrypoint for dbt-rowlineage."""

from __future__ import annotations

import importlib
import importlib.util
from typing import Any, Dict

from . import __version__
from .compiler_patch import patch_compiled_sql
from .config import RowLineageConfig
from .runtime_patch import capture_lineage
from .tracer import RowLineageTracer
from .utils.sql import TRACE_COLUMN


class RowLineagePlugin:
    name = "rowlineage"
    version = __version__

    def __init__(self) -> None:
        self.config = RowLineageConfig()
        self.tracer = RowLineageTracer(self.config)

    def initialize(self, vars: Dict[str, Any] | None = None) -> None:
        self.config = RowLineageConfig.from_vars(vars or {})
        self.tracer = RowLineageTracer(self.config)

    def register_with_dbt(self) -> None:
        """Register plugin hooks with dbt if dbt is installed."""

        if importlib.util.find_spec("dbt") is None:
            return
        factory_spec = importlib.util.find_spec("dbt.adapters.factory")
        if factory_spec is None:
            return
        factory = importlib.import_module("dbt.adapters.factory")
        if hasattr(factory, "register_plugin"):
            factory.register_plugin(self)

    # Hook surfaces for dbt runtime
    def on_compile(self, compiled_sql: str) -> str:
        return patch_compiled_sql(compiled_sql)

    def on_execute(
        self,
        source_rows,
        target_rows,
        source_model: str,
        target_model: str,
        compiled_sql: str,
    ):
        return capture_lineage(source_rows, target_rows, source_model, target_model, compiled_sql, self.config)

    def capture_lineage(
        self,
        source_rows,
        target_rows,
        source_model: str,
        target_model: str,
        compiled_sql: str,
    ):
        """Public surface for downstream callers to capture lineage."""

        return capture_lineage(
            source_rows=source_rows,
            target_rows=target_rows,
            source_model=source_model,
            target_model=target_model,
            compiled_sql=compiled_sql,
            config=self.config,
        )


__all__ = [
    "RowLineagePlugin",
    "TRACE_COLUMN",
    "patch_compiled_sql",
    "capture_lineage",
]
