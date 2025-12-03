from pathlib import Path

import tomllib

from dbt_rowlineage import __version__
from dbt_rowlineage.plugin import RowLineagePlugin


def load_pyproject():
    return tomllib.loads(Path("pyproject.toml").read_text())


def test_version_matches_pyproject():
    project = load_pyproject()["project"]
    assert project["version"] == __version__ == RowLineagePlugin.version


def test_dependencies_include_dbt_core():
    project = load_pyproject()["project"]
    assert any(dep.startswith("dbt-core") for dep in project["dependencies"])


def test_manifest_includes_docs():
    manifest = Path("MANIFEST.in").read_text().splitlines()
    for required in ("include README.md", "include LICENSE", "include pyproject.toml"):
        assert required in manifest


def test_publish_workflow_uses_release_tags():
    workflow = Path(".github/workflows/publish.yml").read_text()
    assert "v*" in workflow
    assert "twine upload" in workflow
