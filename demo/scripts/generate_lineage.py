"""Generate demo lineage artifacts in both JSONL and Parquet formats."""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

from dbt_rowlineage import cli
from dbt_rowlineage.auto import generate_lineage_for_project
from dbt_rowlineage.plugin import RowLineagePlugin
from dbt_rowlineage.tracer import RowLineageTracer
from dbt_rowlineage.writers.jsonl_writer import JSONLWriter
from dbt_rowlineage.writers.parquet_writer import ParquetWriter


PROJECT_ROOT = Path(os.getenv("DBT_PROJECT_ROOT", "/demo")).resolve()
MANIFEST_PATH = PROJECT_ROOT / "target" / "manifest.json"
OUTPUT_DIR = PROJECT_ROOT / "output" / "lineage"


def _connect():
    """Reuse CLI resolution logic to pick up env vars or dbt profile settings."""

    args = SimpleNamespace(
        db_host=None,
        db_port=None,
        db_name=None,
        db_user=None,
        db_password=None,
    )
    return cli._get_connection(args, PROJECT_ROOT)  # type: ignore[attr-defined]


def _run_for_format(conn, export_format: str, filename: str):
    plugin = RowLineagePlugin()
    plugin.config.export_format = export_format
    plugin.config.export_path = str(OUTPUT_DIR / filename)
    plugin.tracer = RowLineageTracer(plugin.config)

    return generate_lineage_for_project(
        conn=conn,
        project_root=PROJECT_ROOT,
        plugin=plugin,
        manifest_path=MANIFEST_PATH,
        output_dir=OUTPUT_DIR,
    )


def main() -> int:
    conn = _connect()
    try:
        mappings_jsonl = _run_for_format(conn, "jsonl", "lineage.jsonl")
        mappings_parquet = _run_for_format(conn, "parquet", "lineage.parquet")
        print(
            "[demo] Generated lineage: "
            f"{len(mappings_jsonl)} JSONL records, "
            f"{len(mappings_parquet)} Parquet records."
        )
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
