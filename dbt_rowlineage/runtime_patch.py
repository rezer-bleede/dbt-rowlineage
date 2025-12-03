"""Runtime instrumentation for capturing lineage during execution."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Sequence

from .config import RowLineageConfig
from .tracer import RowLineageTracer, MappingRecord


def capture_lineage(
    source_rows: Sequence[Dict[str, Any]],
    target_rows: Sequence[Dict[str, Any]],
    source_model: str,
    target_model: str,
    compiled_sql: str,
    config: RowLineageConfig | None = None,
) -> Iterable[MappingRecord]:
    """Build lineage mappings for a model execution.

    In a real dbt invocation this would be called after a model is executed with
    access to both upstream and produced rows. The function is intentionally
    side-effect free; exporting is delegated to writer implementations.
    """

    tracer = RowLineageTracer(config=config)
    return tracer.build_mappings(
        source_rows=source_rows,
        target_rows=target_rows,
        source_model=source_model,
        target_model=target_model,
        compiled_sql=compiled_sql,
    )
