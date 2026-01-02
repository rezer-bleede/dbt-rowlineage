# dbt_rowlineage/auto.py

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from .plugin import RowLineagePlugin
from .tracer import MappingRecord
from .utils.sql import TRACE_COLUMN
from .writers.jsonl_writer import JSONLWriter
from .writers.parquet_writer import ParquetWriter


def _load_manifest(manifest_path: Path) -> Dict[str, Any]:
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found at {manifest_path}")
    with manifest_path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def _relation_from_node(node: Dict[str, Any]) -> Tuple[str, str]:
    schema = node.get("schema")
    table = node.get("alias") or node.get("name")
    if not schema or not table:
        raise ValueError(f"Cannot determine schema/table for node {node.get('unique_id')}")
    return schema, table


def _iter_lineage_edges(manifest: Dict[str, Any]) -> Iterable[Tuple[Dict[str, Any], Dict[str, Any]]]:
    nodes: Dict[str, Dict[str, Any]] = manifest.get("nodes", {})
    queryable_types = {"model", "seed", "snapshot"}
    queryable_nodes = {uid: n for uid, n in nodes.items() if n.get("resource_type") in queryable_types}

    for downstream_uid, downstream in queryable_nodes.items():
        for upstream_uid in downstream.get("depends_on", {}).get("nodes", []):
            upstream = queryable_nodes.get(upstream_uid)
            if upstream is None:
                # Skip sources/tests/etc
                continue
            yield upstream, downstream


def _trace_column_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
            """,
            (schema, table, TRACE_COLUMN),
        )
        return cur.fetchone() is not None


def _ensure_trace_column_on_seed(conn, node: Dict[str, Any]) -> None:
    """Ensure seeds have a trace column populated."""
    if node.get("resource_type") != "seed":
        return

    schema, table = _relation_from_node(node)
    
    # Check if exists
    if _trace_column_exists(conn, schema, table):
        # We might want to check if they are null? 
        # But for now assume if column exists it's fine or was populated.
        # Ideally we run an UPDATE to be sure.
        return
        
    # Add column
    with conn.cursor() as cur:
        # Alter table
        cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN {TRACE_COLUMN} uuid')
        # Update values
        cur.execute(f'UPDATE "{schema}"."{table}" SET {TRACE_COLUMN} = md5(random()::text || clock_timestamp()::text)::uuid WHERE {TRACE_COLUMN} IS NULL')
    conn.commit()


def _fetch_rows(conn, schema: str, table: str, order_by_trace: bool) -> List[Dict[str, Any]]:
    # In tokens mode, we technically don't need to order by trace if we don't zip.
    # But it's good practice.
    with conn.cursor() as cur:
        if order_by_trace:
            sql = f'SELECT * FROM "{schema}"."{table}" ORDER BY "{TRACE_COLUMN}"'
        else:
            sql = f'SELECT * FROM "{schema}"."{table}" ORDER BY 1'
        cur.execute(sql)
        colnames = [desc[0] for desc in cur.description]
        return [dict(zip(colnames, row)) for row in cur.fetchall()]


def _get_writer(plugin: RowLineagePlugin, output_dir: Path):
    """
    Decide writer based on RowLineageConfig.
    """
    cfg = plugin.config
    output_dir = Path(cfg.export_path or output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    fmt = (cfg.export_format or "jsonl").lower()
    if fmt == "jsonl":
        return JSONLWriter(output_dir / "lineage.jsonl")
    elif fmt == "parquet":
        return ParquetWriter(output_dir / "lineage.parquet")
    else:
        raise ValueError(f"Unsupported rowlineage_export_format: {cfg.export_format}")


def generate_lineage_for_project(
    conn,
    project_root: Path,
    plugin: RowLineagePlugin | None = None,
    manifest_path: Path | None = None,
    output_dir: Path | None = None,
    vars: dict | None = None,
) -> List[MappingRecord]:
    """
    High–level API: given a DB connection + dbt project, compute lineage
    for all queryable nodes and write it via the configured writer.

    Returns the full list of MappingRecord for convenience.
    """
    plugin = plugin or RowLineagePlugin()
    # Let dbt vars / env override config in real use; for demo we just use defaults.
    if vars is not None:
        plugin.initialize(vars=vars)

    project_root = project_root.resolve()
    manifest_path = manifest_path or (project_root / "target" / "manifest.json")
    output_dir = output_dir or (project_root / "output" / "lineage")

    manifest = _load_manifest(manifest_path)
    # Determine which nodes are seeds and insure they have trace ids
    nodes = manifest.get("nodes", {})
    for node in nodes.values():
        if node.get("resource_type") == "seed":
             _ensure_trace_column_on_seed(conn, node)

    writer = _get_writer(plugin, output_dir)

    all_mappings: List[MappingRecord] = []

    for upstream, downstream in _iter_lineage_edges(manifest):
        upstream_schema, upstream_table = _relation_from_node(upstream)
        downstream_schema, downstream_table = _relation_from_node(downstream)

        # In tokens mode, we don't strictly need upstream rows, 
        # unless to verify trace column exists or for heuristic fallback.
        # But tracer signature requires source_rows.
        # dbt-rowlineage architecture generally passes rows to tracer.
        # If we pass empty source_rows in tokens mode, tracer must handle it.
        # Tracer implementation I wrote checks target's parent tokens.
        
        is_tokens_mode = plugin.config.lineage_mode == "tokens"
        
        upstream_has_trace = _trace_column_exists(conn, upstream_schema, upstream_table)
        downstream_has_trace = _trace_column_exists(conn, downstream_schema, downstream_table)
        
        upstream_rows = []
        if not is_tokens_mode:
             # Fetch upstream for heuristic
             upstream_rows = _fetch_rows(conn, upstream_schema, upstream_table, order_by_trace=upstream_has_trace)
        
        downstream_rows = _fetch_rows(conn, downstream_schema, downstream_table, order_by_trace=downstream_has_trace)

        compiled_sql: str = downstream.get("compiled_code") or ""

        mappings = plugin.capture_lineage(
            source_rows=upstream_rows,
            target_rows=downstream_rows,
            source_model=upstream.get("name", ""),
            target_model=downstream.get("name", ""),
            compiled_sql=compiled_sql,
        )
        if mappings:
            writer.write(mappings)
            all_mappings.extend(mappings)

    return all_mappings
