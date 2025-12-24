import json
from pathlib import Path

from demo.ui.app import ManifestIndex


def test_manifest_index_reads_mart_models(tmp_path: Path):
    manifest = {
        "nodes": {
            "model.rowlineage_demo.mart_model": {
                "name": "mart_model",
                "resource_type": "model",
                "path": "models/marts/mart_model.sql",
                "schema": "public_marts",
                "alias": "mart_model",
                "columns": {"id": {}, "region": {}},
            },
            "model.rowlineage_demo.region_rollup": {
                "name": "region_rollup",
                "resource_type": "model",
                "path": "models/marts/region_rollup.sql",
                "schema": "public_marts",
                "alias": "region_rollup",
                "columns": {"region": {}, "customer_count": {}},
            },
            "model.rowlineage_demo.staging_model": {
                "name": "staging_model",
                "resource_type": "model",
                "path": "models/staging/staging_model.sql",
                "schema": "public_staging",
                "alias": "staging_model",
            },
        }
    }

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    index = ManifestIndex(manifest_path)

    marts = index.mart_models()
    assert {node["name"] for node in marts} == {"mart_model", "region_rollup"}
    assert index.resolve_relation("region_rollup") == ("public_marts", "region_rollup")
    assert set(index.columns_for_model("region_rollup")) == {"region", "customer_count"}
