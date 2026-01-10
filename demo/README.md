# dbt-rowlineage Demo

This demo runs dbt against both Postgres and ClickHouse containers while the `dbt-rowlineage` plugin injects trace IDs and exports row-level lineage.

## Prerequisites

- Docker and Docker Compose
- Network access to download images and packages

## Quickstart

```bash
cd demo
docker-compose up --build
```

The command builds a Python image that installs `dbt-postgres`, `dbt-clickhouse`, and the published `dbt-rowlineage` package, waits for Postgres and ClickHouse to become healthy, installs dbt packages, seeds the example data, runs the dbt project, and then calls the `dbt-rowlineage` CLI to export lineage.
It also starts UI services that can render mart rows and their upstream lineage for each backend.

Postgres now listens on port `6543` inside the Compose network and on your host to avoid conflicts with any local Postgres instance already bound to `5432` or `5433`. ClickHouse uses `8123` (HTTP) and `9000` (native). Update the `DBT_PORT` environment variable if you need to run the stack on a different port.

The bundled `dbt-rowlineage` CLI reads credentials from the demo's `profiles.yml`, so you don't need to manually export `DBT_DATABASE` or `DBT_USER` when the stack starts.
Override the output format or path by passing flags such as `--export-format parquet` or `--export-path /demo/output/lineage/lineage.parquet` to the CLI invocation.

> **Note:** Earlier iterations of this demo referenced a `rowlineage` adapter type. The plugin is adapter-agnostic, so the bundled `profiles.yml` now uses the standard `postgres` adapter to avoid dbt import errors.

## What gets created

- **Database:** Postgres database `demo` and ClickHouse database `demo` with `example_source`, `staging_model`, `mart_model`, and a region-level aggregation `region_rollup` table that shows how grouped marts retain lineage.
- **Lineage output:** JSONL is written to `output/lineage/` in your working directory for Postgres and `output-clickhouse/lineage/` for ClickHouse using the dbt project configuration. Switch to Parquet by updating `rowlineage_export_format` in `dbt_project.yml` or by passing CLI overrides.
- **Lineage UI:** FastAPI-powered UIs available at http://localhost:8080 (Postgres) and http://localhost:8081 (ClickHouse) that list mart records and let you click a row to see upstream lineage as both detailed hops and an interactive graph. The graph is a rooted tree with the mart record pinned to the top, upstream tables flowing downward, and every node displaying both the model name and a short trace ID. Clicking a node opens a modal overlay that shows all captured column values for that record.
- **Trace columns:** The adapter injects `_row_trace_id` into compiled SQL used by the export script so mappings can be generated deterministically.

### Tracing aggregated marts

The `region_rollup` model groups staging rows by region while preserving row lineage. It uses the `ordered_array_agg` macro so Postgres keeps `array_agg(... order by ...)` semantics and ClickHouse uses `groupArray(order by ...)` while keeping customer ordering deterministic. The lineage UI reads dbt's `manifest.json` to discover marts dynamically, so additional models appear automatically without code changes. Click any aggregated row in the UI to see every upstream staging and seed record that contributed to the grouped result.

Example JSONL line:

```json
{"source_model": "example_source", "target_model": "staging_model", "source_trace_id": "d8e1d1d3-f5fe-5f0b-9ef7-177c51bf1c6e", "target_trace_id": "f4c47ab0-1b02-5720-a2c4-e45f77dd8df3", "compiled_sql": "select * from staging_model", "executed_at": "2024-01-01T00:00:00+00:00"}
```

Parquet output contains the same columns.

## Project layout

- `dbt_project.yml` and `profiles.yml` configure dbt for the Postgres and ClickHouse services using the standard `postgres` and `clickhouse` adapters with rowlineage enabled via vars and model configs.
- `packages.yml` installs the `dbt-rowlineage` plugin package so dbt can load the hooks that instrument compilation and execution.
- `models/` contains staging and mart models that keep row counts aligned to make lineage easy to inspect.
- `seeds/` stores the seed data (`example_source.csv`).
- `docker/Dockerfile` installs `dbt-postgres`, `dbt-clickhouse`, and the `dbt-rowlineage` adapter from PyPI and runs dbt plus the lineage export CLI.
- `docker-compose.yml` wires together the Postgres and ClickHouse containers, dbt runners, lineage UIs, and a code-server instance, mounting `./output` and `./output-clickhouse` so lineage artifacts are available on the host.

## Code Server IDE

The demo now bundles [code-server](https://github.com/coder/code-server) so you can edit the dbt project from your browser.

- **Access the IDE:** http://localhost:8443
- **Project mount:** The entire demo directory is mounted into `/home/coder/project` inside the code-server container, so saving a file in the IDE updates the files on your host.
- **Authentication:** The instance uses the `PASSWORD` environment variable exposed as `CODE_SERVER_PASSWORD` in `docker-compose.yml` (default `demo`).

The code-server container waits for Postgres to become healthy before starting. The lineage UIs wait for their respective databases and dbt runs so the sample data and lineage mappings are available when the page is loaded.

## Cleaning up

Stop the stack with `docker-compose down`. To reset state, remove the volumes and output folders:

```bash
docker-compose down -v
rm -rf output output-clickhouse
```
