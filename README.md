# dbt-rowlineage

A dbt adapter-agnostic plugin that adds row-level lineage tracing to dbt model execution. The plugin injects a deterministic `_row_trace_id` column into compiled SQL, captures mappings between upstream and downstream rows, and can export lineage to multiple targets for observability.

## Installation

Install the published package directly from PyPI:

```bash
pip install dbt-rowlineage
```

The package registers an entrypoint under `dbt.adapters` named `rowlineage`, allowing dbt to discover the plugin automatically.

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

A ready-to-run demo lives in the `demo/` directory and uses Docker Compose to provision Postgres, install `dbt-rowlineage` from PyPI, and run dbt end-to-end. From the repository root:

```bash
cd demo
docker-compose up --build
```

Lineage artifacts are written to `demo/output/lineage/`. See `demo/README.md` for full instructions and an example JSONL record.

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
