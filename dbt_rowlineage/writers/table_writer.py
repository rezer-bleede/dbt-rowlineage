"""Writer that exports lineage mappings to a database table."""

from __future__ import annotations

from typing import Iterable, Sequence

from ..tracer import MappingRecord


class TableWriter:
    def __init__(self, connection) -> None:
        self.connection = connection

    def write(self, mappings: Iterable[MappingRecord]) -> None:
        rows: Sequence[MappingRecord] = list(mappings)
        if not rows:
            return
        cursor = self.connection.cursor()
        cursor.executemany(
            """
            INSERT INTO lineage__mappings (
                source_model, target_model, source_trace_id, target_trace_id, compiled_sql, executed_at
            ) VALUES (:source_model, :target_model, :source_trace_id, :target_trace_id, :compiled_sql, :executed_at)
            """,
            rows,
        )
        self.connection.commit()
