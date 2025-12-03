from pathlib import Path

import tomllib

from dbt_rowlineage import __version__
from dbt_rowlineage.plugin import RowLineagePlugin


def load_pyproject():
    return tomllib.loads(Path("pyproject.toml").read_text())


def test_version_matches_pyproject():
    pyproject = load_pyproject()
    project = pyproject["project"]

    assert "version" not in project
    assert "version" in project["dynamic"]

    dynamic_version = pyproject["tool"]["setuptools"]["dynamic"]["version"]
    assert dynamic_version["attr"] == "dbt_rowlineage.__version__"
    assert __version__ == RowLineagePlugin.version


def test_dependencies_include_dbt_core():
    project = load_pyproject()["project"]
    assert any(dep.startswith("dbt-core") for dep in project["dependencies"])


def test_manifest_includes_docs():
    manifest = Path("MANIFEST.in").read_text().splitlines()
    for required in ("include README.md", "include LICENSE", "include pyproject.toml"):
        assert required in manifest


def test_publish_workflow_uses_release_tags():
    workflow = Path(".github/workflows/publish.yml").read_text()
    assert "release:" in workflow
    assert "types: [published]" in workflow
    assert "startsWith(github.event.release.tag_name, 'v')" in workflow
    assert "twine upload" in workflow
