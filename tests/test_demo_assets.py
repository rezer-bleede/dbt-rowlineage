from pathlib import Path


def test_dockerfile_uses_pypi_package():
    dockerfile = Path("demo/docker/Dockerfile").read_text()
    assert "dbt-rowlineage" in dockerfile
    assert "pip install --no-cache-dir dbt-postgres dbt-rowlineage" in dockerfile


def test_compose_waits_for_postgres():
    compose = Path("demo/docker-compose.yml").read_text()
    assert "service_healthy" in compose
    assert "postgres" in compose


def test_compose_includes_code_server():
    compose = Path("demo/docker-compose.yml").read_text()
    assert "code-server" in compose
    assert "8443:8443" in compose
    assert "code-server/Dockerfile" in compose


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


def test_dockerfile_runs_seed_before_models():
    dockerfile = Path("demo/docker/Dockerfile").read_text()
    assert "dbt deps && dbt seed --full-refresh && dbt run" in dockerfile
    assert "dbt run && dbt deps" not in dockerfile


def test_packages_file_present():
    packages = Path("demo/packages.yml").read_text()
    assert "packages:" in packages


def test_docker_compose_runs_cli_lineage_export():
    compose = Path("demo/docker-compose.yml").read_text()
    assert "dbt-rowlineage" in compose
    assert "python3 scripts/generate_lineage.py" not in compose


def test_profile_uses_base_adapter():
    profile = Path("demo/profiles.yml").read_text()
    assert "type: postgres" in profile
    assert "type: rowlineage" not in profile
    assert "base_adapter" not in profile


def test_gitignore_excludes_dbt_artifacts():
    gitignore = Path(".gitignore").read_text()
    assert "demo/target/" in gitignore
    assert "target/" in gitignore
    assert "dbt_packages/" in gitignore


def test_demo_uses_nondefault_postgres_port():
    compose = Path("demo/docker-compose.yml").read_text()
    assert "6543:6543" in compose
    assert "DBT_PORT: 6543" in compose

    profile = Path("demo/profiles.yml").read_text()
    assert "port: 6543" in profile


def test_demo_includes_aggregation_model():
    mart_model = Path("demo/models/marts/region_rollup.sql").read_text()
    assert "group by region" in mart_model.lower()
    assert "array_agg" in mart_model.lower()
