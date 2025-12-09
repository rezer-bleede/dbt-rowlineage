import sys
from pathlib import Path
from typing import Dict, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient

from demo.ui.app import Mapping, build_lineage_graph, create_app


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


def test_fastapi_endpoints_with_stubbed_repository(tmp_path: Path):
    class StubRepository:
        def fetch_mart_rows(self):
            return [
                {"id": 1, "region": "west", "customer_name_upper": "ALICE", "_row_trace_id": "mart-1"}
            ]

        def fetch_lineage(self, trace_id: str):
            assert trace_id == "mart-1"
            return {"target_row": self.fetch_mart_rows()[0], "hops": []}

    app = create_app(repository_provider=lambda: StubRepository())
    client = TestClient(app)

    mart_response = client.get("/api/mart_rows")
    assert mart_response.status_code == 200
    assert mart_response.json()["rows"][0]["_row_trace_id"] == "mart-1"

    lineage_response = client.get("/api/lineage/mart-1")
    assert lineage_response.status_code == 200
    assert lineage_response.json()["target_row"]["id"] == 1
