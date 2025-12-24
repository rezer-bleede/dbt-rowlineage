import sys
from typing import Dict, List

from dbt_rowlineage.utils.sql import TRACE_COLUMN

ROOT = __import__("pathlib").Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from demo.ui.app import LineageRepository, ManifestIndex  # noqa: E402


MANIFEST_FIXTURE: Dict[str, Dict] = {
    "nodes": {
        "model.demo.mart_model": {
            "name": "mart_model",
            "resource_type": "model",
            "schema": "analytics",
            "alias": "mart_model",
            "path": "marts/mart_model.sql",
            "original_file_path": "models/marts/mart_model.sql",
            "columns": {"id": {}, "region": {}},
        },
        "model.demo.region_rollup": {
            "name": "region_rollup",
            "resource_type": "model",
            "schema": "analytics",
            "alias": "region_rollup",
            "path": "marts/region_rollup.sql",
            "original_file_path": "models/marts/region_rollup.sql",
            "columns": {"region": {}, "customer_count": {}},
        },
        "model.demo.windows_rollup": {
            "name": "windows_rollup",
            "resource_type": "model",
            "schema": "analytics",
            "alias": "windows_rollup",
            "path": r"marts\windows_rollup.sql",
            "original_file_path": r"marts\windows_rollup.sql",
            "columns": {"region": {}, "customer_count": {}},
        },
        "model.demo.staging_model": {
            "name": "staging_model",
            "resource_type": "model",
            "schema": "analytics",
            "alias": "staging_model",
            "path": "staging/staging_model.sql",
            "original_file_path": "models/staging/staging_model.sql",
            "columns": {"id": {}, "region": {}},
        },
    }
}


class StubRepository(LineageRepository):
    def __init__(self):
        super().__init__(manifest_index=ManifestIndex(manifest_data=MANIFEST_FIXTURE))

    def _fetch_rows(self, sql: str, params: List | None = None) -> List[dict]:  # type: ignore[override]
        if "region_rollup" in sql:
            return [
                {"region": "west", "customer_count": 2, TRACE_COLUMN: "agg-west"},
                {"region": "east", "customer_count": 1, TRACE_COLUMN: "agg-east"},
            ]
        if "windows_rollup" in sql:
            return [
                {"region": "west", "customer_count": 3, TRACE_COLUMN: "win-west"},
            ]
        return [
            {"id": 1, "region": "west", TRACE_COLUMN: "mart-1"},
            {"id": 2, "region": "east", TRACE_COLUMN: "mart-2"},
        ]


def test_manifest_index_detects_marts_without_models_prefix():
    index = ManifestIndex(manifest_data=MANIFEST_FIXTURE)
    mart_names = {node["name"] for node in index.mart_models()}

    assert mart_names == {"mart_model", "region_rollup", "windows_rollup"}


def test_fetch_mart_rows_uses_manifest_marts():
    repo = StubRepository()

    models = repo.fetch_mart_rows()
    model_lookup = {model["name"]: model for model in models}

    assert set(model_lookup.keys()) == {"mart_model", "region_rollup", "windows_rollup"}
    assert model_lookup["mart_model"]["rows"][0][TRACE_COLUMN] == "mart-1"
    assert model_lookup["region_rollup"]["rows"][0]["customer_count"] == 2
    assert TRACE_COLUMN in model_lookup["region_rollup"]["columns"]
    assert model_lookup["windows_rollup"]["rows"][0][TRACE_COLUMN] == "win-west"


def test_manifest_index_handles_windows_style_paths():
    index = ManifestIndex(manifest_data=MANIFEST_FIXTURE)

    paths = [
        index._normalize_path(MANIFEST_FIXTURE["nodes"]["model.demo.windows_rollup"]["path"]),
        index._normalize_path(MANIFEST_FIXTURE["nodes"]["model.demo.windows_rollup"]["original_file_path"]),
    ]

    assert paths == ["marts/windows_rollup.sql", "marts/windows_rollup.sql"]
    assert any(index._is_mart_path(path) for path in paths)
