from __future__ import annotations

import importlib
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


class DatabaseClient:
    def fetch_rows(
        self,
        schema: str,
        table: str,
        *,
        order_by_trace: bool = False,
        trace_id: str | None = None,
        limit: int | None = None,
    ) -> List[dict]:
        raise NotImplementedError

    def has_column(self, schema: str, table: str, column: str) -> bool:
        raise NotImplementedError


class PostgresDatabaseClient(DatabaseClient):
    def __init__(self, dbname: str, user: str, password: str, host: str, port: int):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def _connect(self):
        return psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )

    def fetch_rows(
        self,
        schema: str,
        table: str,
        *,
        order_by_trace: bool = False,
        trace_id: str | None = None,
        limit: int | None = None,
    ) -> List[dict]:
        params: Dict[str, object] = {}
        sql = f'SELECT * FROM "{schema}"."{table}"'
        if trace_id is not None:
            sql += f' WHERE "{TRACE_COLUMN}" = %(trace_id)s'
            params["trace_id"] = trace_id
        sql += f' ORDER BY "{TRACE_COLUMN}"' if order_by_trace else " ORDER BY 1"
        if limit is not None:
            sql += " LIMIT %(limit)s"
            params["limit"] = limit

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [c[0] for c in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def has_column(self, schema: str, table: str, column: str) -> bool:
        sql = (
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %(schema)s AND table_name = %(table)s AND column_name = %(column)s"
        )
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"schema": schema, "table": table, "column": column})
                return cur.fetchone() is not None


class ClickHouseDatabaseClient(DatabaseClient):
    def __init__(self, dbname: str, user: str, password: str, host: str, port: int):
        clickhouse_connect = importlib.import_module("clickhouse_connect")
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=dbname,
        )

    @staticmethod
    def _escape(value: str) -> str:
        return value.replace("'", "''")

    def fetch_rows(
        self,
        schema: str,
        table: str,
        *,
        order_by_trace: bool = False,
        trace_id: str | None = None,
        limit: int | None = None,
    ) -> List[dict]:
        order_by = TRACE_COLUMN if order_by_trace else "1"
        sql = f"SELECT * FROM {schema}.{table}"
        if trace_id is not None:
            escaped = self._escape(trace_id)
            sql += f" WHERE {TRACE_COLUMN} = '{escaped}'"
        sql += f" ORDER BY {order_by}"
        if limit is not None:
            sql += f" LIMIT {limit}"

        result = self.client.query(sql)
        columns = result.column_names
        return [dict(zip(columns, row)) for row in result.result_rows]

    def has_column(self, schema: str, table: str, column: str) -> bool:
        sql = (
            "SELECT 1 FROM system.columns "
            f"WHERE database = '{self._escape(schema)}' "
            f"AND table = '{self._escape(table)}' "
            f"AND name = '{self._escape(column)}' "
            "LIMIT 1"
        )
        result = self.client.query(sql)
        return bool(result.result_rows)


class LineageRepository:
    def __init__(
        self,
        lineage_path: Path | None = None,
        manifest_path: Path | None = None,
        manifest_index: "ManifestIndex | None" = None,
        adapter_type: str | None = None,
        db_client: DatabaseClient | None = None,
    ):
        self.lineage_path = lineage_path or Path("/demo/output/lineage/lineage.jsonl")
        self.dbname = os.getenv("DBT_DATABASE", "demo")
        self.user = os.getenv("DBT_USER", "demo")
        self.password = os.getenv("DBT_PASSWORD", "demo")
        self.host = os.getenv("DBT_HOST", "postgres")
        self.port = int(os.getenv("DBT_PORT", "6543"))
        self.adapter_type = (adapter_type or os.getenv("DBT_ADAPTER", "postgres")).lower()
        self.db_client = db_client or self._build_db_client()
        self.manifest = manifest_index or ManifestIndex(manifest_path)

    def _build_db_client(self) -> DatabaseClient:
        if self.adapter_type.startswith("clickhouse"):
            return ClickHouseDatabaseClient(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
        return PostgresDatabaseClient(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )

    def _fetch_row_by_trace(self, model: str, trace_id: str) -> Optional[dict]:
        relation = self.manifest.resolve_relation(model)
        if relation is None:
            return None
        schema, table = relation

        has_trace_column = self.db_client.has_column(schema, table, TRACE_COLUMN)

        if has_trace_column:
            rows = self.db_client.fetch_rows(
                schema,
                table,
                trace_id=trace_id,
                limit=1,
            )
            return rows[0] if rows else None

        rows = self.db_client.fetch_rows(schema, table)
        for row in rows:
            if TRACE_COLUMN not in row:
                row[TRACE_COLUMN] = new_trace_id(row)
            if row.get(TRACE_COLUMN) == trace_id:
                return row
        return None

    def _load_mappings(self) -> List[Mapping]:
        if not self.lineage_path.exists():
            return []
        with self.lineage_path.open() as handle:
            return [Mapping.from_json(json.loads(line)) for line in handle if line.strip()]

    def _root_models_from_mappings(self, mappings: List[Mapping]) -> List[str]:
        """
        Infer top‑level (mart) models directly from lineage mappings.

        A "root" model is any model that only appears as a target in the
        lineage graph and never as a source. This is computed dynamically
        from the mappings so it works for arbitrary depths and shapes of
        the DAG without relying on folder or schema conventions.
        """
        if not mappings:
            return []

        source_models = {m.source_model for m in mappings}
        target_models = {m.target_model for m in mappings}

        roots = target_models - source_models
        if roots:
            return sorted(roots)

        # Degenerate case: if every target also appears as a source, fall
        # back to treating all targets as potential roots.
        if target_models:
            return sorted(target_models)
        return sorted(source_models)

    def fetch_mart_rows(self) -> List[dict]:
        models: List[dict] = []

        # Prefer dynamic discovery from lineage mappings so the UI reflects
        # whatever the export actually produced, regardless of folder or
        # schema layout.
        mappings = self._load_mappings()
        mart_model_names = self._root_models_from_mappings(mappings)

        # If no mappings are available yet (e.g. before the export has run),
        # fall back to manifest‑based mart discovery to keep the demo usable.
        if not mart_model_names:
            mart_nodes = self.manifest.mart_models()
            mart_model_names = [
                node.get("name", "") for node in mart_nodes if node.get("name")
            ]

        for model_name in mart_model_names:
            relation = self.manifest.resolve_relation(model_name)
            if relation is None:
                continue
            schema, table = relation
            rows = self.db_client.fetch_rows(schema, table)
            if rows and TRACE_COLUMN not in rows[0]:
                for row in rows:
                    row[TRACE_COLUMN] = new_trace_id(row)
            columns = self.manifest.columns_for_model(model_name)
            if not columns and rows:
                columns = list(rows[0].keys())
            if TRACE_COLUMN not in columns:
                columns.append(TRACE_COLUMN)
            models.append({"name": model_name, "columns": columns, "rows": rows})

        return models

    def fetch_lineage(self, target_model: str, target_trace_id: str) -> dict:
        relation = self.manifest.resolve_relation(target_model)
        if relation is None:
            raise HTTPException(status_code=404, detail="Unknown model")

        target_rows = self.db_client.fetch_rows(relation[0], relation[1])
        if target_rows and TRACE_COLUMN not in target_rows[0]:
            for row in target_rows:
                row[TRACE_COLUMN] = new_trace_id(row)
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

    def _path_candidates(self, node: Dict) -> List[str]:
        return [
            str(node.get("path", "")),
            str(node.get("original_file_path", "")),
        ]

    def _normalize_path(self, path: str) -> str:
        """Normalize manifest paths for reliable mart detection.

        Manifest paths can vary depending on the operating system and dbt
        configuration. Windows builds, for example, emit backslashes while
        some projects omit the leading ``models/`` prefix entirely. We convert
        backslashes to forward slashes and strip optional ``./`` and
        ``models/`` prefixes so downstream checks can rely on a consistent
        shape.
        """

        normalized = path.replace("\\", "/").lstrip("./")
        while normalized.startswith("models/"):
            normalized = normalized[len("models/") :]
        return normalized

    def _is_mart_path(self, path: str) -> bool:
        normalized = self._normalize_path(path)
        return normalized.startswith("marts/") or "/marts/" in normalized

    def mart_models(self) -> List[Dict]:
        mart_nodes = [
            node
            for node in self._iter_nodes()
            if node.get("resource_type") == "model"
            and any(self._is_mart_path(path) for path in self._path_candidates(node))
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
