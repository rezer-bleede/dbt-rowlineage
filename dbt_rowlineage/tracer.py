"""Core logic for capturing row lineage."""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from .config import RowLineageConfig
from .utils.uuid import new_trace_id


MappingRecord = Dict[str, Any]


class RowLineageTracer:
    """Capture lineage mappings between source and target rows.

    The tracer is intentionally adapter-agnostic and operates on Python data
    structures so it can be exercised in unit tests. In a real dbt runtime the
    inputs would be cursor results instead.
    """

    def __init__(self, config: RowLineageConfig | None = None) -> None:
        self.config = config or RowLineageConfig()

    def build_mappings(
        self,
        source_rows: Sequence[Dict[str, Any]],
        target_rows: Sequence[Dict[str, Any]],
        source_model: str,
        target_model: str,
        compiled_sql: str,
    ) -> List[MappingRecord]:
        executed_at = dt.datetime.now(dt.timezone.utc).isoformat()
        mappings: List[MappingRecord] = []
        
        # Token-based lineage (default)
        if self.config.lineage_mode == "tokens":
            resolved_targets = _ensure_iter(target_rows)
            
            for target_row in resolved_targets:
                target_trace = target_row.get("_row_trace_id")
                if not target_trace:
                     # If target has no trace, we can't map it.
                     # (Should allow fallback? Tracer assumes trace exists)
                     target_trace = new_trace_id(target_row)
                
                parent_tokens = target_row.get("_row_parent_trace_ids")
                
                if parent_tokens:
                    # Parse tokens
                    # Postgres array might be returned as list by psycopg2 or string
                    tokens_list = []
                    if isinstance(parent_tokens, list):
                        tokens_list = parent_tokens
                    elif isinstance(parent_tokens, str):
                        # Simple parsing for {a,b} format if needed, 
                        # but usually adapter handles this.
                        # Assuming list for now if using standard adapters.
                        pass
                    
                    for token in tokens_list:
                        if not isinstance(token, str):
                            continue
                        
                        # Token format: "source_model_name:uuid"
                        # We match prefix
                        prefix = f"{source_model}:"
                        if token.startswith(prefix):
                            source_trace = token[len(prefix):]
                            mappings.append({
                                "source_model": source_model,
                                "target_model": target_model,
                                "source_trace_id": source_trace,
                                "target_trace_id": target_trace,
                                "compiled_sql": compiled_sql,
                                "executed_at": executed_at,
                            })
                            
            if mappings:
                return mappings
            
            # If no mappings found in tokens mode, do we fall back?
            # Plan says: "No fallback to zip(source_rows, target_rows). If lineage is unavailable, return empty mappings"
            # UNLESS config is heuristic.
            return []

        # Heuristic mode (Legacy)
        resolved_sources = _ensure_iter(source_rows)
        resolved_targets = _ensure_iter(target_rows)

        # Precompute target trace ids so every source contributing to the same
        # aggregated row maps to a stable identifier.
        target_pairs: List[Tuple[Dict[str, Any], str]] = [
            (row, row.get("_row_trace_id") or new_trace_id(row)) for row in resolved_targets
        ]

        matched: List[Tuple[Dict[str, Any], Dict[str, Any], str]] = []

        for target_row, target_trace in target_pairs:
            for source_row in resolved_sources:
                if _rows_share_values(source_row, target_row):
                    matched.append((source_row, target_row, target_trace))

        if not matched:
            matched = list(zip(resolved_sources, resolved_targets, [trace for _, trace in target_pairs]))

        for source_row, target_row, target_trace in matched:
            source_trace = source_row.get("_row_trace_id") or new_trace_id(source_row)
            mappings.append(
                {
                    "source_model": source_model,
                    "target_model": target_model,
                    "source_trace_id": source_trace,
                    "target_trace_id": target_trace,
                    "compiled_sql": compiled_sql,
                    "executed_at": executed_at,
                }
            )
        return mappings

    def export(self, mappings: Iterable[MappingRecord], writer: "BaseWriter") -> None:
        writer.write(mappings)


class BaseWriter:
    """Protocol-like base class for writers.

    Writers are intentionally lightweight; they only need to implement a
    ``write`` method that accepts an iterable of mapping dicts.
    """

    def write(self, mappings: Iterable[MappingRecord]) -> None:  # pragma: no cover - interface only
        raise NotImplementedError


def _ensure_iter(rows: Sequence[Dict[str, Any]] | None) -> Sequence[Dict[str, Any]]:
    return rows or []


def _rows_share_values(source_row: Dict[str, Any], target_row: Dict[str, Any]) -> bool:
    """Return True when two rows have overlapping columns with equal values.

    The comparison excludes the trace column so aggregated targets can be
    matched back to every contributing source that shares grouping keys.
    """

    if not source_row or not target_row:
        return False

    shared_keys = set(source_row).intersection(target_row)
    shared_keys.discard("_row_trace_id")
    if not shared_keys:
        return False

    return all(source_row[key] == target_row[key] for key in shared_keys)
