import os
from pathlib import Path
from typing import Iterable, List

import psycopg2

from dbt_rowlineage.runtime.capture_lineage import capture_lineage
from dbt_rowlineage.tracer import MappingRecord
from dbt_rowlineage.utils.sql import TRACE_COLUMN
from dbt_rowlineage.utils.uuid import new_trace_id
from dbt_rowlineage.writers.jsonl_writer import JSONLWriter
from dbt_rowlineage.writers.parquet_writer import ParquetWriter


def fetch_rows(conn, sql: str) -> List[dict]:
    with conn.cursor() as cur:
        cur.execute(sql)
        columns = [c[0] for c in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    if TRACE_COLUMN not in columns:
        for row in rows:
            row[TRACE_COLUMN] = new_trace_id(row)

    return rows


def write_outputs(mappings: Iterable[MappingRecord], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    mappings = list(mappings)
    JSONLWriter(output_dir / "lineage.jsonl").write(mappings)
    ParquetWriter(output_dir / "lineage.parquet").write(mappings)


def main() -> None:
    conn = psycopg2.connect(
        dbname=os.getenv("DBT_DATABASE", "demo"),
        user=os.getenv("DBT_USER", "demo"),
        password=os.getenv("DBT_PASSWORD", "demo"),
        host=os.getenv("DBT_HOST", "postgres"),
        port=int(os.getenv("DBT_PORT", "5432")),
    )

    try:
        source_rows = fetch_rows(
            conn,
            "select id, customer_name, region from example_source order by id",
        )
        staging_rows = fetch_rows(
            conn,
            "select id, customer_name_upper, region, concat(region, '-', id) as customer_key from staging_model order by id",
        )
        mart_rows = fetch_rows(
            conn,
            "select id, customer_name_upper, region, customer_key from mart_model order by id",
        )

        mappings: List[MappingRecord] = []
        mappings.extend(
            capture_lineage(
                source_rows,
                staging_rows,
                "select * from staging_model",
            )
        )
        mappings.extend(
            capture_lineage(
                staging_rows,
                mart_rows,
                "select * from mart_model",
            )
        )
        write_outputs(mappings, Path("/demo/output/lineage"))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
