# dbt-rowlineage

A dbt adapter-agnostic plugin that adds row-level lineage tracing to dbt model execution. The plugin injects a deterministic `_row_trace_id` column into compiled SQL, captures mappings between upstream and downstream rows, and can export lineage to multiple targets for observability.

## Installation

Install the published package directly from PyPI:

```bash
pip install dbt-rowlineage
```

After installation the plugin is discovered automatically by dbt; keep using your existing adapter (for example `postgres`, `bigquery`, or `snowflake`) and enable row lineage with vars and model configs.

### Command line utility

The project ships a `dbt-rowlineage` CLI that can export lineage for a compiled dbt project. Connection parameters are read in this order:

1. CLI flags such as `--db-host`, `--db-user`, and `--db-password`.
2. Environment variables (`DBT_HOST`, `PGUSER`, `DBT_DATABASE`, etc.).
3. The dbt profile defined in `dbt_project.yml` and loaded from `profiles.yml` (respecting `DBT_PROFILES_DIR` and `DBT_TARGET`).

When no port override is provided, the CLI defaults to `DBT_PORT`/`PGPORT` and falls back to `6543` to avoid clashing with other Postgres containers that may already be bound to `5432` or `5433`.

A minimal invocation that relies on the project profile looks like:

```bash
DBT_PROFILES_DIR=/path/to/profiles \
dbt-rowlineage --project-root /path/to/dbt/project
```

Override output details on the command line instead of editing `dbt_project.yml`:

```bash
dbt-rowlineage \
  --project-root /path/to/dbt/project \
  --export-format parquet \
  --export-path /tmp/lineage/lineage.parquet
```

## Configuration

Enable the plugin in `dbt_project.yml` by setting vars and model configs:

```yaml
vars:
  rowlineage: true
models:
  +rowlineage_enabled: true
  +rowlineage_export_format: jsonl  # jsonl|parquet|table
  +rowlineage_export_path: target/lineage/lineage.jsonl
```

## How it works

1. **Compilation hook**: during SQL rendering the plugin injects a trace expression into the top-level `SELECT` list when `_row_trace_id` is not already present.
2. **Execution hook**: after model execution the plugin captures input and output rows, pairs their trace ids, and writes mappings into the `lineage__mappings` table (or to JSONL/Parquet when configured).
3. **Deterministic IDs**: UUIDs are produced deterministically from row content to keep tests reproducible.

The `RowLineagePlugin` exposes a `capture_lineage` method for callers that need to drive lineage collection manually (for example when using the `auto.generate_lineage_for_project` helper). The method mirrors the runtime hook signature and automatically reuses the plugin's active configuration.

The lineage mapping table schema:

| column          | description                              |
| --------------- | ---------------------------------------- |
| source_model    | upstream model name                      |
| target_model    | downstream model name                    |
| source_trace_id | trace id from the upstream row           |
| target_trace_id | trace id from the downstream row         |
| compiled_sql    | SQL statement executed for the target    |
| executed_at     | UTC timestamp when the mapping occurred  |

## Export targets

- **JSONL**: append mappings to a JSON Lines file.
- **Parquet**: write mappings to a Parquet file (overwrites existing file).
- **Database table**: insert mappings into `lineage__mappings` via the provided database connection.

## Demo with Docker Compose

A ready-to-run demo lives in the `demo/` directory and uses Docker Compose to provision Postgres, install `dbt-rowlineage` from PyPI, run dbt end-to-end, and expose a lightweight lineage explorer UI. From the repository root:

```bash
cd demo
docker-compose up --build
```

Lineage artifacts are written to `demo/output/lineage/` by the `dbt-rowlineage` CLI rather than a helper script. The Compose entrypoint now installs packages and seeds the demo data before the first `dbt run`, preventing missing table errors for `example_source`. After the stack comes up, visit http://localhost:8080 to browse mart rows—including the aggregated `region_rollup` mart—and trace them back to staging and source records in both a descriptive list and an interactive graph. See `demo/README.md` for full instructions and an example JSONL record.
When your dbt project keeps marts under a folder such as `marts/` (without the `models/` prefix baked into the manifest `path`), the lineage UI still discovers them so the dropdown stays populated instead of reporting that no mart rows were found. On Windows, manifest entries can include backslashes instead of forward slashes; those paths are normalized before filtering so marts are still listed.

The graph view renders a rooted tree that keeps the selected mart record at the top and fans upstream sources downward for a stable, readable layout. Each node shows the model name and a short trace ID, and clicking a node opens an overlay listing every captured column value for that record.

> Tip: dbt-generated artifacts such as `target/`, `dbt_packages/`, and `logs/` are ignored via `.gitignore` to keep compiled files out of the repository.

## Development

Install dependencies and run the test suite:

```bash
pip install -e .[dev]
pytest
```

## Publishing

To publish a new version to PyPI:

1. Update the version string in `dbt_rowlineage/__init__.py` (e.g., `__version__ = "0.1.1"`).
2. Commit the change and push it to the default branch.
3. In GitHub, navigate to **Releases** and choose **Draft new release**.
4. Create a tag that matches the version number prefixed with `v` (for example, `v0.1.1`), then publish the release.

When the release is published, GitHub Actions builds the source distribution and wheel and uploads both to PyPI automatically using the configured secrets. No additional manual publishing steps are required.
