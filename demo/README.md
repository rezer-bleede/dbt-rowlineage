# dbt-rowlineage Demo

This demo runs dbt against a lightweight Postgres container while the `dbt-rowlineage` plugin, installed from PyPI, injects trace IDs and exports row-level lineage.

## Prerequisites

- Docker and Docker Compose
- Network access to download images and packages

## Quickstart

```bash
cd demo
docker-compose up --build
```

The command builds a Python image that installs `dbt-postgres` and the published `dbt-rowlineage` package, waits for Postgres to become healthy, runs the dbt project, and executes a lineage export script.

## What gets created

- **Database:** Postgres database `demo` with `example_source`, `staging_model`, and `mart_model` tables.
- **Lineage output:** JSONL and Parquet files written to `output/lineage/` in your working directory.
- **Trace columns:** The plugin injects `_row_trace_id` into compiled SQL used by the export script so mappings can be generated deterministically.

Example JSONL line:

```json
{"source_model": "example_source", "target_model": "staging_model", "source_trace_id": "d8e1d1d3-f5fe-5f0b-9ef7-177c51bf1c6e", "target_trace_id": "f4c47ab0-1b02-5720-a2c4-e45f77dd8df3", "compiled_sql": "select * from staging_model", "executed_at": "2024-01-01T00:00:00+00:00"}
```

Parquet output contains the same columns.

## Project layout

- `dbt_project.yml` and `profiles.yml` configure dbt for the Postgres service.
- `models/` contains staging and mart models that keep row counts aligned to make lineage easy to inspect.
- `seeds/` stores the seed data (`example_source.csv`).
- `docker/Dockerfile` installs `dbt-postgres` and `dbt-rowlineage` from PyPI and runs dbt plus the lineage export script.
- `docker-compose.yml` wires together the Postgres container, the dbt runner, and the SQLMesh UI, mounting `./output` so lineage artifacts are available on the host.
- `scripts/generate_lineage.py` patches SQL with `_row_trace_id`, captures lineage across the two model hops, and writes JSONL/Parquet outputs.

## SQLMesh UI

The demo now bundles [SQLMesh UI](https://sqlmesh.com/docs/ui) so you can explore and edit the dbt project from your browser.

- **Access the UI:** http://localhost:8000
- **Project mount:** The entire demo directory is mounted into `/app` inside the SQLMesh container, so saving a file in the UI updates the files on your host.
- **dbt compatibility:** SQLMesh uses its built-in dbt compatibility to render the models defined in this project.

The SQLMesh container waits for Postgres to become healthy before starting the UI.

## Cleaning up

Stop the stack with `docker-compose down`. To reset state, remove the volume and output folder:

```bash
docker-compose down -v
rm -rf output
```
