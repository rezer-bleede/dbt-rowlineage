from pathlib import Path


def test_dockerfile_uses_pypi_package():
    dockerfile = Path("demo/docker/Dockerfile").read_text()
    assert "dbt-rowlineage" in dockerfile
    assert "pip install --no-cache-dir dbt-postgres dbt-rowlineage" in dockerfile


def test_compose_waits_for_postgres():
    compose = Path("demo/docker-compose.yml").read_text()
    assert "service_healthy" in compose
    assert "postgres" in compose


def test_compose_includes_sqlmesh_ui():
    compose = Path("demo/docker-compose.yml").read_text()
    assert "sqlmesh" in compose
    assert "8000:8000" in compose
    assert "sqlmesh/Dockerfile" in compose


def test_compose_includes_lineage_ui():
    compose = Path("demo/docker-compose.yml").read_text()
    assert "lineage-ui" in compose
    assert "demo/ui/Dockerfile" in compose
    assert "8080:8080" in compose


def test_dbt_project_exports_lineage():
    project = Path("demo/dbt_project.yml").read_text()
    assert "rowlineage_export_path" in project
    assert "output/lineage/lineage.jsonl" in project


def test_dbt_project_paths_align():
    project = Path("demo/dbt_project.yml").read_text()
    assert "model-paths: [\"models\"]" in project
    assert "seed-paths: [\"seeds\"]" in project


def test_packages_file_present():
    packages = Path("demo/packages.yml").read_text()
    assert "packages:" in packages


def test_lineage_script_exports_both_formats():
    script = Path("demo/scripts/generate_lineage.py").read_text()
    assert "JSONLWriter" in script
    assert "ParquetWriter" in script
