from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional

import psycopg2
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from dbt_rowlineage.utils.sql import TRACE_COLUMN
from dbt_rowlineage.utils.uuid import new_trace_id


MODEL_TABLES: Dict[str, tuple[str, str]] = {
    "mart_model": ("public_marts", "mart_model"),
    "staging_model": ("public_staging", "staging_model"),
    "example_source": ("public_staging", "example_source"),
}


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
    def __init__(self, lineage_path: Path | None = None):
        self.lineage_path = lineage_path or Path("/demo/output/lineage/lineage.jsonl")
        self.dbname = os.getenv("DBT_DATABASE", "demo")
        self.user = os.getenv("DBT_USER", "demo")
        self.password = os.getenv("DBT_PASSWORD", "demo")
        self.host = os.getenv("DBT_HOST", "postgres")
        self.port = int(os.getenv("DBT_PORT", "6543"))

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
        schema_table = MODEL_TABLES.get(model)
        if not schema_table:
            return None
        schema, table = schema_table
        
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
                "order by id limit 1"
            )
            rows = self._fetch_rows(sql, (trace_id,))
            return rows[0] if rows else None
        else:
            # Table doesn't have trace column (e.g., seed tables)
            # Fetch all rows and generate trace IDs, then find the matching one
            sql = f"select * from {schema}.{table} order by id"
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
        schema, table = MODEL_TABLES["mart_model"]
        sql = f"select * from {schema}.{table} order by id"
        return self._fetch_rows(sql)

    def fetch_lineage(self, target_trace_id: str) -> dict:
        mart_rows = self.fetch_mart_rows()
        target_row = next((row for row in mart_rows if row.get(TRACE_COLUMN) == target_trace_id), None)
        if not target_row:
            raise HTTPException(status_code=404, detail="Mart record not found")

        mappings = self._load_mappings()
        hops = build_lineage_graph(
            target_trace_id=target_trace_id,
            target_model="mart_model",
            mappings=mappings,
            row_lookup=lambda model, trace: self._fetch_row_by_trace(model, trace),
        )
        return {"target_row": target_row, "hops": hops}


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
        return {"rows": repo.fetch_mart_rows()}

    @app.get("/api/lineage/{trace_id}")
    def lineage(trace_id: str, repo: LineageRepository = Depends(repo_dependency)) -> dict:
        return repo.fetch_lineage(trace_id)

    return app


app = create_app()
