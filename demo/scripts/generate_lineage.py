import os
import sys
from pathlib import Path

import psycopg2
from dbt_rowlineage.auto import generate_lineage_for_project
from dbt_rowlineage.writers.jsonl_writer import JSONLWriter
from dbt_rowlineage.writers.parquet_writer import ParquetWriter

if __name__ == "__main__":
    try:
        conn = psycopg2.connect(
            host=os.environ["DBT_HOST"],
            port=os.environ["DBT_PORT"],
            dbname=os.environ["DBT_DATABASE"],
            user=os.environ["DBT_USER"],
            password=os.environ["DBT_PASSWORD"],
        )
        generate_lineage_for_project(
            conn=conn,
            project_root=Path("."),
        )
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if "conn" in locals() and conn:
            conn.close()
