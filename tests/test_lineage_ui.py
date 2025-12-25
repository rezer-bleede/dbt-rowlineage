import sys
from pathlib import Path
from typing import Dict, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient

from demo.ui.app import Mapping, build_lineage_graph, build_visual_graph, create_app


def test_build_lineage_graph_traverses_upstream():
    mappings = [
        Mapping(
            source_model="staging_model",
            target_model="mart_model",
            source_trace_id="stg-1",
            target_trace_id="mart-1",
            compiled_sql="select * from mart_model",
            executed_at="2024-01-01T00:00:00Z",
        ),
        Mapping(
            source_model="example_source",
            target_model="staging_model",
            source_trace_id="src-1",
            target_trace_id="stg-1",
            compiled_sql="select * from staging_model",
            executed_at="2024-01-01T00:00:00Z",
        ),
    ]

    rows = {
        ("staging_model", "stg-1"): {"id": 1, "region": "west"},
        ("example_source", "src-1"): {"id": 1, "region": "west"},
    }

    def lookup(model: str, trace: str) -> Optional[Dict]:
        return rows.get((model, trace))

    graph = build_lineage_graph("mart-1", "mart_model", mappings, lookup)

    assert len(graph) == 2
    assert graph[0]["source_model"] == "staging_model"
    assert graph[1]["source_model"] == "example_source"
    assert graph[0]["row"] == {"id": 1, "region": "west"}
    assert graph[1]["target_trace_id"] == "stg-1"


def test_build_visual_graph_returns_nodes_and_edges():
    hops = [
        {
            "source_model": "staging_model",
            "target_model": "mart_model",
            "source_trace_id": "stg-1",
            "target_trace_id": "mart-1",
            "compiled_sql": "select * from staging_model",
            "executed_at": "2024-01-01T00:00:00Z",
            "row": {"id": 1},
        },
        {
            "source_model": "example_source",
            "target_model": "staging_model",
            "source_trace_id": "src-1",
            "target_trace_id": "stg-1",
            "compiled_sql": "select * from example_source",
            "executed_at": "2024-01-01T00:00:00Z",
            "row": {"id": 1},
        },
    ]

    graph = build_visual_graph(
        target_model="mart_model",
        target_trace_id="mart-1",
        target_row={"id": 1},
        hops=hops,
    )

    assert {node["trace_id"] for node in graph["nodes"]} == {"mart-1", "stg-1", "src-1"}
    assert any(edge["target"].endswith("mart-1") for edge in graph["edges"])
    assert len(graph["edges"]) == 2


def test_build_visual_graph_populates_rows_on_nodes():
    hops = [
        {
            "source_model": "upstream_one",
            "target_model": "mart_model",
            "source_trace_id": "up-1",
            "target_trace_id": "mart-1",
            "compiled_sql": "select * from upstream_one",
            "executed_at": "2024-01-01T00:00:00Z",
            "row": {"id": 5, "region": "west"},
        },
        {
            "source_model": "seed_one",
            "target_model": "upstream_one",
            "source_trace_id": "seed-1",
            "target_trace_id": "up-1",
            "compiled_sql": "select * from seed_one",
            "executed_at": "2024-01-01T00:00:00Z",
            "row": {"id": 2, "customer": "alice"},
        },
    ]

    graph = build_visual_graph(
        target_model="mart_model",
        target_trace_id="mart-1",
        target_row={"id": 99},
        hops=hops,
    )

    nodes_by_trace = {node["trace_id"]: node for node in graph["nodes"]}

    assert nodes_by_trace["mart-1"]["row"] == {"id": 99}
    assert nodes_by_trace["up-1"]["row"] == {"id": 5, "region": "west"}
    assert nodes_by_trace["seed-1"]["row"] == {"id": 2, "customer": "alice"}


def test_fastapi_endpoints_with_stubbed_repository(tmp_path: Path):
    class StubRepository:
        def fetch_mart_rows(self):
            return [
                {
                    "name": "mart_model",
                    "columns": ["id", "customer_name_upper", "region", "_row_trace_id"],
                    "rows": [
                        {"id": 1, "region": "west", "customer_name_upper": "ALICE", "_row_trace_id": "mart-1"}
                    ],
                }
            ]

        def fetch_lineage(self, model: str, trace_id: str):
            assert trace_id == "mart-1"
            assert model == "mart_model"
            return {
                "target_row": self.fetch_mart_rows()[0]["rows"][0],
                "hops": [],
                "target_model": model,
                "graph": {"nodes": [], "edges": []},
            }

    app = create_app(repository_provider=lambda: StubRepository())
    client = TestClient(app)

    mart_response = client.get("/api/mart_rows")
    assert mart_response.status_code == 200
    models = mart_response.json()["models"]
    assert models[0]["name"] == "mart_model"
    assert models[0]["rows"][0]["_row_trace_id"] == "mart-1"

    lineage_response = client.get("/api/lineage/mart_model/mart-1")
    assert lineage_response.status_code == 200
    assert lineage_response.json()["target_row"]["id"] == 1


def test_lineage_endpoint_includes_rows_in_graph():
    class StubRepository:
        def fetch_mart_rows(self):
            return [
                {
                    "name": "mart_model",
                    "columns": ["id", "_row_trace_id"],
                    "rows": [{"id": 1, "_row_trace_id": "mart-1"}],
                }
            ]

        def fetch_lineage(self, model: str, trace_id: str):
            hops = [
                {
                    "source_model": "source_table",
                    "target_model": model,
                    "source_trace_id": "src-1",
                    "target_trace_id": trace_id,
                    "compiled_sql": "select * from source_table",
                    "executed_at": "2024-01-01T00:00:00Z",
                    "row": {"id": 44, "name": "widget"},
                }
            ]
            return {
                "target_row": {"id": 1, "_row_trace_id": trace_id},
                "hops": hops,
                "target_model": model,
                "graph": build_visual_graph(
                    target_model=model,
                    target_trace_id=trace_id,
                    target_row={"id": 1},
                    hops=hops,
                ),
            }

    app = create_app(repository_provider=lambda: StubRepository())
    client = TestClient(app)

    response = client.get("/api/lineage/mart_model/mart-1")

    assert response.status_code == 200
    graph = response.json()["graph"]
    assert any(node.get("row", {}).get("name") == "widget" for node in graph.get("nodes", []))
