# dbt-rowlineage Demo

This demo runs dbt against a lightweight Postgres container while the `dbt-rowlineage` plugin injects trace IDs and exports row-level lineage.

## Prerequisites

- Docker and Docker Compose
- Network access to download images and packages

## Quickstart

```bash
cd demo
docker-compose up --build
```

The command builds a Python image that installs `dbt-postgres` and the published `dbt-rowlineage` package, waits for Postgres to become healthy, installs dbt packages, seeds the example data, runs the dbt project, and executes a lineage export script.
It also starts a small UI service that can render mart rows and their upstream lineage.

The bundled `dbt-rowlineage` CLI reads credentials from the demo's `profiles.yml`, so you don't need to manually export `DBT_DATABASE` or `DBT_USER` when the stack starts.

> **Note:** Earlier iterations of this demo referenced a `rowlineage` adapter type. The plugin is adapter-agnostic, so the bundled `profiles.yml` now uses the standard `postgres` adapter to avoid dbt import errors.

## What gets created

- **Database:** Postgres database `demo` with `example_source`, `staging_model`, and `mart_model` tables.
- **Lineage output:** JSONL and Parquet files written to `output/lineage/` in your working directory.
- **Lineage UI:** A FastAPI-powered UI available at http://localhost:8080 that lists mart records and lets you click a row to see upstream lineage.
- **Trace columns:** The adapter injects `_row_trace_id` into compiled SQL used by the export script so mappings can be generated deterministically.

Example JSONL line:

```json
{"source_model": "example_source", "target_model": "staging_model", "source_trace_id": "d8e1d1d3-f5fe-5f0b-9ef7-177c51bf1c6e", "target_trace_id": "f4c47ab0-1b02-5720-a2c4-e45f77dd8df3", "compiled_sql": "select * from staging_model", "executed_at": "2024-01-01T00:00:00+00:00"}
```

Parquet output contains the same columns.

## Project layout

- `dbt_project.yml` and `profiles.yml` configure dbt for the Postgres service using the standard `postgres` adapter with rowlineage enabled via vars and model configs.
- `packages.yml` installs the `dbt-rowlineage` plugin package so dbt can load the hooks that instrument compilation and execution.
- `models/` contains staging and mart models that keep row counts aligned to make lineage easy to inspect.
- `seeds/` stores the seed data (`example_source.csv`).
- `docker/Dockerfile` installs `dbt-postgres` and the `dbt-rowlineage` adapter from PyPI and runs dbt plus the lineage export script.
- `docker-compose.yml` wires together the Postgres container, the dbt runner, the lineage UI, and the SQLMesh UI, mounting `./output` so lineage artifacts are available on the host.
- `scripts/generate_lineage.py` patches SQL with `_row_trace_id`, captures lineage across the two model hops, and writes JSONL/Parquet outputs.

## SQLMesh UI

The demo now bundles [SQLMesh UI](https://sqlmesh.com/docs/ui) so you can explore and edit the dbt project from your browser.

- **Access the UI:** http://localhost:8000
- **Project mount:** The entire demo directory is mounted into `/app` inside the SQLMesh container, so saving a file in the UI updates the files on your host.
- **dbt compatibility:** SQLMesh uses its built-in dbt compatibility to render the models defined in this project.

The SQLMesh container waits for Postgres to become healthy before starting the UI. The lineage UI waits for both Postgres and the dbt run so the sample data and lineage mappings are available when the page is loaded.

## Cleaning up

Stop the stack with `docker-compose down`. To reset state, remove the volume and output folder:

```bash
docker-compose down -v
rm -rf output
```
