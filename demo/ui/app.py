from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import psycopg2
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from dbt_rowlineage.utils.sql import TRACE_COLUMN
from dbt_rowlineage.utils.uuid import new_trace_id


@dataclass
class Mapping:
    source_model: str
    target_model: str
    source_trace_id: str
    target_trace_id: str
    compiled_sql: str = ""
    executed_at: str = ""

    @classmethod
    def from_json(cls, payload: Dict[str, str]) -> "Mapping":
        return cls(
            source_model=payload["source_model"],
            target_model=payload["target_model"],
            source_trace_id=payload["source_trace_id"],
            target_trace_id=payload["target_trace_id"],
            compiled_sql=payload.get("compiled_sql", ""),
            executed_at=payload.get("executed_at", ""),
        )


class LineageRepository:
    def __init__(
        self,
        lineage_path: Path | None = None,
        manifest_path: Path | None = None,
        manifest_index: "ManifestIndex | None" = None,
    ):
        self.lineage_path = lineage_path or Path("/demo/output/lineage/lineage.jsonl")
        self.dbname = os.getenv("DBT_DATABASE", "demo")
        self.user = os.getenv("DBT_USER", "demo")
        self.password = os.getenv("DBT_PASSWORD", "demo")
        self.host = os.getenv("DBT_HOST", "postgres")
        self.port = int(os.getenv("DBT_PORT", "6543"))
        self.manifest = manifest_index or ManifestIndex(manifest_path)

    def _connect(self):
        return psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )

    def _fetch_rows(self, sql: str, params: Iterable | None = None) -> List[dict]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or [])
                columns = [c[0] for c in cur.description]
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]

        if TRACE_COLUMN not in columns:
            for row in rows:
                row[TRACE_COLUMN] = new_trace_id(row)
        return rows

    def _fetch_row_by_trace(self, model: str, trace_id: str) -> Optional[dict]:
        relation = self.manifest.resolve_relation(model)
        if relation is None:
            return None
        schema, table = relation
        
        # First check if the table has the trace column
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s AND column_name = %s", 
                           (schema, table, TRACE_COLUMN))
                has_trace_column = cur.fetchone() is not None
        
        if has_trace_column:
            # Table has trace column, query by trace ID
            sql = (
                f"select * from {schema}.{table} where {TRACE_COLUMN} = %s "
                "order by 1 limit 1"
            )
            rows = self._fetch_rows(sql, (trace_id,))
            return rows[0] if rows else None
        else:
            # Table doesn't have trace column (e.g., seed tables)
            # Fetch all rows and generate trace IDs, then find the matching one
            sql = f"select * from {schema}.{table} order by 1"
            rows = self._fetch_rows(sql)
            # Find the row that would have this trace ID
            for row in rows:
                if row.get(TRACE_COLUMN) == trace_id:
                    return row
            return None

    def _load_mappings(self) -> List[Mapping]:
        if not self.lineage_path.exists():
            return []
        with self.lineage_path.open() as handle:
            return [Mapping.from_json(json.loads(line)) for line in handle if line.strip()]

    def fetch_mart_rows(self) -> List[dict]:
        models = []
        mart_models = self.manifest.mart_models()

        for node in mart_models:
            relation = self.manifest.resolve_relation(node.get("name", ""))
            if relation is None:
                continue
            schema, table = relation
            sql = f"select * from {schema}.{table} order by 1"
            rows = self._fetch_rows(sql)
            columns = self.manifest.columns_for_model(node.get("name", ""))
            if not columns and rows:
                columns = list(rows[0].keys())
            if TRACE_COLUMN not in columns:
                columns.append(TRACE_COLUMN)
            models.append({"name": node.get("name"), "columns": columns, "rows": rows})

        return models

    def fetch_lineage(self, target_model: str, target_trace_id: str) -> dict:
        relation = self.manifest.resolve_relation(target_model)
        if relation is None:
            raise HTTPException(status_code=404, detail="Unknown model")

        target_rows = self._fetch_rows(
            f"select * from {relation[0]}.{relation[1]} order by 1"
        )
        target_row = next((row for row in target_rows if row.get(TRACE_COLUMN) == target_trace_id), None)
        if not target_row:
            raise HTTPException(status_code=404, detail="Mart record not found")

        mappings = self._load_mappings()
        hops = build_lineage_graph(
            target_trace_id=target_trace_id,
            target_model=target_model,
            mappings=mappings,
            row_lookup=lambda model, trace: self._fetch_row_by_trace(model, trace),
        )
        graph = build_visual_graph(
            target_model=target_model,
            target_trace_id=target_trace_id,
            target_row=target_row,
            hops=hops,
        )
        return {
            "target_row": target_row,
            "hops": hops,
            "target_model": target_model,
            "graph": graph,
        }


class ManifestIndex:
    def __init__(self, manifest_path: Path | None = None, manifest_data: Dict | None = None):
        self.manifest_path = manifest_path or Path("/demo/target/manifest.json")
        self._manifest = manifest_data or self._load_manifest()

    def _load_manifest(self) -> Dict:
        if not self.manifest_path.exists():
            return {"nodes": {}}
        with self.manifest_path.open("r", encoding="utf-8") as fp:
            return json.load(fp)

    def _iter_nodes(self) -> Iterable[Dict]:
        return self._manifest.get("nodes", {}).values()

    def resolve_relation(self, model: str) -> Optional[Tuple[str, str]]:
        for node in self._iter_nodes():
            if node.get("name") == model and node.get("resource_type") in {"model", "seed", "snapshot"}:
                schema = node.get("schema")
                table = node.get("alias") or node.get("name")
                if schema and table:
                    return schema, table
        return None

    def mart_models(self) -> List[Dict]:
        nodes = list(self._iter_nodes())
        queryable_types = {"model", "seed", "snapshot"}

        # All unique IDs of nodes that are dependencies for other nodes
        dependency_ids = set()
        for node in nodes:
            for dep_id in node.get("depends_on", {}).get("nodes", []):
                dependency_ids.add(dep_id)

        # Find models that are not dependencies for any other model
        mart_nodes = [
            node
            for node in nodes
            if node.get("resource_type") in queryable_types
            and node.get("unique_id") not in dependency_ids
        ]

        if mart_nodes:
            return mart_nodes

        # Fallback for minimal environments without manifest metadata
        return [node for node in self._iter_nodes() if node.get("name") == "mart_model"]

    def columns_for_model(self, model: str) -> List[str]:
        for node in self._iter_nodes():
            if node.get("name") == model:
                columns = node.get("columns") or {}
                return list(columns.keys())
        return []


def build_lineage_graph(
    target_trace_id: str,
    target_model: str,
    mappings: List[Mapping],
    row_lookup: Callable[[str, str], Optional[dict]],
) -> List[dict]:
    graph: List[dict] = []
    queue: List[tuple[str, str]] = [(target_trace_id, target_model)]
    visited = set()

    while queue:
        current_trace, current_model = queue.pop(0)
        upstream = [
            m
            for m in mappings
            if m.target_trace_id == current_trace and m.target_model == current_model
        ]
        for mapping in upstream:
            node_id = (mapping.source_model, mapping.source_trace_id)
            if node_id in visited:
                continue
            visited.add(node_id)
            source_row = row_lookup(mapping.source_model, mapping.source_trace_id)
            graph.append(
                {
                    "source_model": mapping.source_model,
                    "target_model": mapping.target_model,
                    "source_trace_id": mapping.source_trace_id,
                    "target_trace_id": mapping.target_trace_id,
                    "compiled_sql": mapping.compiled_sql,
                    "executed_at": mapping.executed_at,
                    "row": source_row,
                }
            )
            queue.append((mapping.source_trace_id, mapping.source_model))

    return graph


def build_visual_graph(
    target_model: str, target_trace_id: str, target_row: Optional[dict], hops: List[dict]
) -> dict:
    """Convert lineage hops into a node-link structure for UI rendering."""

    nodes: Dict[str, dict] = {}
    edges: List[dict] = []
    kind_priority = {"source": 0, "intermediate": 1, "target": 2}

    def ensure_node(model: str, trace_id: str, kind: str = "source") -> str:
        node_id = f"{model}:{trace_id}"
        existing = nodes.get(node_id)
        if existing:
            current_priority = kind_priority.get(existing.get("kind", "source"), 0)
            new_priority = kind_priority.get(kind, 0)
            if new_priority > current_priority:
                existing["kind"] = kind
            return node_id

        nodes[node_id] = {
            "id": node_id,
            "label": model,
            "trace_id": trace_id,
            "kind": kind,
        }
        return node_id

    target_id = ensure_node(target_model, target_trace_id, kind="target")
    if target_row is not None:
        nodes[target_id]["row"] = target_row

    for hop in hops:
        source_id = ensure_node(hop["source_model"], hop["source_trace_id"], kind="source")
        hop_target_id = ensure_node(
            hop["target_model"], hop["target_trace_id"], kind="intermediate"
        )

        if hop.get("row") is not None:
            nodes[source_id].setdefault("row", hop["row"])

        edges.append(
            {
                "source": source_id,
                "target": hop_target_id,
                "label": hop.get("compiled_sql") or "",
            }
        )

    return {"nodes": list(nodes.values()), "edges": edges}


def create_app(repository_provider: Optional[Callable[[], LineageRepository]] = None) -> FastAPI:
    app = FastAPI(title="Row Level Lineage Demo")
    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    repo_dependency = repository_provider or (lambda: LineageRepository())

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        html = (static_dir / "index.html").read_text(encoding="utf-8")
        return HTMLResponse(content=html)

    @app.get("/api/mart_rows")
    def mart_rows(repo: LineageRepository = Depends(repo_dependency)) -> dict:
        return {"models": repo.fetch_mart_rows()}

    @app.get("/api/lineage/{model}/{trace_id}")
    def lineage(model: str, trace_id: str, repo: LineageRepository = Depends(repo_dependency)) -> dict:
        return repo.fetch_lineage(model, trace_id)

    return app


app = create_app()
