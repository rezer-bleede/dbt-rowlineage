import argparse
from pathlib import Path

import dbt_rowlineage.cli as cli


def _write_project_and_profiles(tmp_path: Path):
    project_root = tmp_path
    (project_root / "dbt_project.yml").write_text(
        """
name: demo_project
profile: demo_profile
        """.strip()
    )

    profiles_dir = project_root / "profiles_dir"
    profiles_dir.mkdir()
    (profiles_dir / "profiles.yml").write_text(
        """
demo_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: profile-host
      port: 6543
      user: demo_user
      password: secret
      dbname: demo_db
        """.strip()
    )
    return project_root, profiles_dir


def test_get_connection_defaults_to_new_port(monkeypatch, tmp_path):
    project_root = tmp_path
    (project_root / "dbt_project.yml").write_text("profile: demo\n")

    monkeypatch.delenv("DBT_PORT", raising=False)
    monkeypatch.delenv("PGPORT", raising=False)
    monkeypatch.setenv("DBT_DATABASE", "demo_db")
    monkeypatch.setenv("DBT_USER", "demo_user")
    monkeypatch.setenv("DBT_PASSWORD", "secret")

    captured = {}

    class DummyConn:
        def close(self):
            pass

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return DummyConn()

    monkeypatch.setattr(cli.psycopg2, "connect", fake_connect)

    args = argparse.Namespace(
        db_host=None,
        db_port=None,
        db_name=None,
        db_user=None,
        db_password=None,
        project_root=str(project_root),
        manifest_path=None,
        output_dir=None,
    )

    conn = cli._get_connection(args, project_root)
    assert isinstance(conn, DummyConn)
    assert captured.get("port") == 6543


def test_load_profile_connection(tmp_path, monkeypatch):
    project_root, profiles_dir = _write_project_and_profiles(tmp_path)
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))

    defaults = cli._load_profile_connection(project_root)

    assert defaults == {
        "host": "profile-host",
        "port": "6543",
        "database": "demo_db",
        "user": "demo_user",
        "password": "secret",
    }


def test_get_connection_uses_profile_defaults(monkeypatch, tmp_path):
    project_root, profiles_dir = _write_project_and_profiles(tmp_path)
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))

    captured = {}

    class DummyConn:
        def close(self):
            pass

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return DummyConn()

    monkeypatch.setattr(cli.psycopg2, "connect", fake_connect)

    args = argparse.Namespace(
        db_host=None,
        db_port=None,
        db_name=None,
        db_user=None,
        db_password=None,
        project_root=str(project_root),
        manifest_path=None,
        output_dir=None,
    )

    conn = cli._get_connection(args, project_root)
    assert isinstance(conn, DummyConn)
    assert captured == {
        "host": "profile-host",
        "port": 6543,
        "dbname": "demo_db",
        "user": "demo_user",
        "password": "secret",
    }


def test_main_succeeds_with_profile(monkeypatch, tmp_path, capsys):
    project_root, profiles_dir = _write_project_and_profiles(tmp_path)
    (project_root / "target").mkdir()
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))

    class DummyConn:
        def close(self):
            pass

    def fake_connect(**kwargs):
        return DummyConn()

    monkeypatch.setattr(cli.psycopg2, "connect", fake_connect)

    calls = {}

    def fake_generate_lineage_for_project(**kwargs):
        calls.update(kwargs)
        return []

    monkeypatch.setattr(cli, "generate_lineage_for_project", fake_generate_lineage_for_project)

    exit_code = cli.main(["--project-root", str(project_root)])

    assert exit_code == 0
    assert isinstance(calls.get("conn"), DummyConn)
    assert calls.get("project_root") == project_root

    captured = capsys.readouterr()
    assert "Generated 0 lineage mappings" in captured.out


def test_main_applies_export_overrides(monkeypatch, tmp_path, capsys):
    project_root, profiles_dir = _write_project_and_profiles(tmp_path)
    (project_root / "target").mkdir()
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))

    class DummyConn:
        def close(self):
            pass

    monkeypatch.setattr(cli.psycopg2, "connect", lambda **kwargs: DummyConn())

    captured_kwargs = {}

    def fake_generate_lineage_for_project(**kwargs):
        captured_kwargs.update(kwargs)
        return []

    monkeypatch.setattr(cli, "generate_lineage_for_project", fake_generate_lineage_for_project)

    export_path = project_root / "custom" / "lineage.parquet"
    exit_code = cli.main(
        [
            "--project-root",
            str(project_root),
            "--export-format",
            "parquet",
            "--export-path",
            str(export_path),
        ]
    )

    assert exit_code == 0
    assert captured_kwargs.get("vars") == {
        "rowlineage_export_format": "parquet",
        "rowlineage_export_path": str(export_path),
    }

    captured = capsys.readouterr()
    assert "Generated 0 lineage mappings" in captured.out
