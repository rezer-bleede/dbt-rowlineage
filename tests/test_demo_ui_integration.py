from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from dbt_rowlineage.utils.sql import TRACE_COLUMN
from dbt_rowlineage.utils.uuid import new_trace_id
from demo.ui.app import LineageRepository, create_app


class FakeDatabaseClient:
    def __init__(self, tables: dict[str, list[dict]], traced_tables: set[str]):
        self.tables = tables
        self.traced_tables = traced_tables

    def fetch_rows(
        self,
        schema: str,
        table: str,
        *,
        order_by_trace: bool = False,
        trace_id: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        key = f"{schema}.{table}"
        rows = list(self.tables.get(key, []))
        if trace_id is not None:
            rows = [row for row in rows if row.get(TRACE_COLUMN) == trace_id]
        if limit is not None:
            rows = rows[:limit]
        return rows

    def has_column(self, schema: str, table: str, column: str) -> bool:
        return column == TRACE_COLUMN and f"{schema}.{table}" in self.traced_tables


def _write_manifest(tmp_path: Path) -> Path:
    manifest = {
        "nodes": {
            "seed.rowlineage_demo.example_source": {
                "resource_type": "seed",
                "schema": "staging",
                "name": "example_source",
            },
            "model.rowlineage_demo.staging_model": {
                "resource_type": "model",
                "schema": "staging",
                "name": "staging_model",
                "compiled_code": "select * from example_source",
                "depends_on": {"nodes": ["seed.rowlineage_demo.example_source"]},
            },
            "model.rowlineage_demo.mart_model": {
                "resource_type": "model",
                "schema": "marts",
                "name": "mart_model",
                "compiled_code": "select * from staging_model",
                "depends_on": {"nodes": ["model.rowlineage_demo.staging_model"]},
            },
        }
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    return manifest_path


def _write_lineage(tmp_path: Path, seed_trace: str) -> Path:
    lineage_path = tmp_path / "lineage.jsonl"
    records = [
        {
            "source_model": "example_source",
            "target_model": "staging_model",
            "source_trace_id": seed_trace,
            "target_trace_id": "stg-1",
            "compiled_sql": "select * from example_source",
            "executed_at": "2024-01-01T00:00:00Z",
        },
        {
            "source_model": "staging_model",
            "target_model": "mart_model",
            "source_trace_id": "stg-1",
            "target_trace_id": "mart-1",
            "compiled_sql": "select * from staging_model",
            "executed_at": "2024-01-01T00:00:00Z",
        },
    ]
    lineage_path.write_text("\n".join(json.dumps(record) for record in records))
    return lineage_path


def test_ui_integration_with_manifest_and_lineage(tmp_path: Path):
    seed_row = {"id": 1, "customer_name": "Alice", "region": "west"}
    seed_trace = new_trace_id(seed_row)
    manifest_path = _write_manifest(tmp_path)
    lineage_path = _write_lineage(tmp_path, seed_trace)

    tables = {
        "staging.example_source": [seed_row],
        "staging.staging_model": [
            {"id": 1, "customer_name_upper": "ALICE", "region": "west", TRACE_COLUMN: "stg-1"}
        ],
        "marts.mart_model": [
            {"id": 1, "customer_name_upper": "ALICE", "region": "west", TRACE_COLUMN: "mart-1"}
        ],
    }
    db_client = FakeDatabaseClient(
        tables=tables,
        traced_tables={"staging.staging_model", "marts.mart_model"},
    )
    repository = LineageRepository(
        lineage_path=lineage_path,
        manifest_path=manifest_path,
        db_client=db_client,
    )

    app = create_app(repository_provider=lambda: repository)
    client = TestClient(app)

    mart_response = client.get("/api/mart_rows")
    assert mart_response.status_code == 200
    models = mart_response.json()["models"]
    assert models[0]["name"] == "mart_model"

    lineage_response = client.get("/api/lineage/mart_model/mart-1")
    assert lineage_response.status_code == 200
    graph = lineage_response.json()["graph"]
    assert any(node.get("row", {}).get("customer_name") == "Alice" for node in graph["nodes"])


def test_ui_serves_static_index():
    app = create_app()
    client = TestClient(app)

    response = client.get("/")

    assert response.status_code == 200
    assert "Row Level Lineage Explorer" in response.text
