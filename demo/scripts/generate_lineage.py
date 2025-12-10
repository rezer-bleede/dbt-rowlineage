import os
from pathlib import Path

import psycopg2

from dbt_rowlineage.auto import generate_lineage_for_project


def _get_connection():
    host = os.getenv("DBT_HOST", "postgres")
    port = int(os.getenv("DBT_PORT", "5432"))
    database = os.getenv("DBT_DATABASE", "demo")
    user = os.getenv("DBT_USER", "demo_user")
    password = os.getenv("DBT_PASSWORD", "demo_password")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password,
    )


def main() -> None:
    project_root = Path(os.getenv("DBT_PROJECT_ROOT", "/demo"))
    conn = _get_connection()
    try:
        generate_lineage_for_project(conn=conn, project_root=project_root)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
