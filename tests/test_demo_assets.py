from pathlib import Path


def test_dockerfile_uses_pypi_package():
    dockerfile = Path("demo/docker/Dockerfile").read_text()
    assert "dbt-rowlineage" in dockerfile
    assert "pip install --no-cache-dir dbt-postgres dbt-rowlineage" in dockerfile


def test_compose_waits_for_postgres():
    compose = Path("demo/docker-compose.yml").read_text()
    assert "service_healthy" in compose
    assert "postgres" in compose


def test_dbt_project_exports_lineage():
    project = Path("demo/dbt_project.yml").read_text()
    assert "rowlineage_export_path" in project
    assert "output/lineage/lineage.jsonl" in project


def test_lineage_script_exports_both_formats():
    script = Path("demo/scripts/generate_lineage.py").read_text()
    assert "JSONLWriter" in script
    assert "ParquetWriter" in script
    assert "patch_compiled_sql" in script
