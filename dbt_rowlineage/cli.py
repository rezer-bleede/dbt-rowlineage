# dbt_rowlineage/cli.py

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Optional

import yaml

import psycopg2
from psycopg2 import OperationalError

from .auto import generate_lineage_for_project


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="dbt-rowlineage",
        description="Compute row-level lineage for a dbt project using dbt-rowlineage.",
    )

    parser.add_argument(
        "--project-root",
        type=str,
        default=os.getenv("DBT_PROJECT_ROOT", "."),
        help="Path to dbt project root (default: DBT_PROJECT_ROOT or current directory).",
    )
    parser.add_argument(
        "--manifest-path",
        type=str,
        default=None,
        help="Optional explicit path to manifest.json. "
             "If not set, uses <project-root>/target/manifest.json.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Optional output directory override. If not set, uses plugin config "
             "rowlineage_export_path or <project-root>/output/lineage.",
    )

    parser.add_argument(
        "--export-format",
        type=str,
        default=None,
        choices=["jsonl", "parquet"],
        help="Override rowlineage export format (jsonl or parquet). Defaults to project config.",
    )

    parser.add_argument(
        "--export-path",
        type=str,
        default=None,
        help="Override rowlineage export path. Defaults to project config.",
    )

    # DB connection overrides; env vars still work if flags omitted
    parser.add_argument(
        "--db-host",
        type=str,
        default=None,
        help="Database host (default: DBT_HOST or 'localhost').",
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=None,
        help="Database port (default: DBT_PORT or 6543).",
    )
    parser.add_argument(
        "--db-name",
        type=str,
        default=None,
        help="Database name (default: DBT_DATABASE or PGDATABASE).",
    )
    parser.add_argument(
        "--db-user",
        type=str,
        default=None,
        help="Database user (default: DBT_USER or PGUSER).",
    )
    parser.add_argument(
        "--db-password",
        type=str,
        default=None,
        help="Database password (default: DBT_PASSWORD or PGPASSWORD).",
    )

    return parser.parse_args(argv)


def _load_profile_connection(project_root: Path) -> dict[str, Optional[str]]:
    """Load connection defaults from the dbt profile, if present.

    The project profile name is read from ``dbt_project.yml`` under ``project_root``.
    The target is resolved using ``DBT_TARGET`` env var, the profile ``target`` key,
    or the first defined output. Missing files or malformed YAML are treated as the
    absence of profile defaults rather than hard failures.
    """

    project_file = project_root / "dbt_project.yml"
    try:
        project_cfg = yaml.safe_load(project_file.read_text(encoding="utf-8")) if project_file.exists() else None
        profile_name = project_cfg.get("profile") if isinstance(project_cfg, dict) else None
        if not profile_name:
            return {}

        profiles_dir = Path(os.getenv("DBT_PROFILES_DIR", Path.home() / ".dbt"))
        profiles_file = profiles_dir / "profiles.yml"
        profiles_cfg = (
            yaml.safe_load(profiles_file.read_text(encoding="utf-8")) if profiles_file.exists() else None
        )
        profile_block = profiles_cfg.get(profile_name) if isinstance(profiles_cfg, dict) else None
        if not profile_block:
            return {}

        outputs = profile_block.get("outputs") or {}
        target_name = os.getenv("DBT_TARGET") or profile_block.get("target")
        if not target_name and outputs:
            target_name = next(iter(outputs.keys()))

        target_cfg = outputs.get(target_name, {}) if isinstance(outputs, dict) else {}

        host = target_cfg.get("host")
        port = target_cfg.get("port")
        database = target_cfg.get("dbname") or target_cfg.get("database")
        user = target_cfg.get("user")
        password = target_cfg.get("password") or target_cfg.get("pass") or target_cfg.get("passphrase")

        return {
            "host": host,
            "port": str(port) if port is not None else None,
            "database": database,
            "user": user,
            "password": password,
        }
    except Exception:
        # Swallow YAML/IO errors; CLI will fall back to env/flags and raise a
        # clearer error if required parameters are still missing.
        return {}


def _resolve_db_param(
    cli_value: Optional[str],
    env_keys: list[str],
    default: Optional[str] = None,
) -> Optional[str]:
    if cli_value is not None:
        return cli_value
    for key in env_keys:
        val = os.getenv(key)
        if val:
            return val
    return default


def _get_connection(args: argparse.Namespace, project_root: Path):
    profile_defaults = _load_profile_connection(project_root)

    host = _resolve_db_param(args.db_host, ["DBT_HOST", "PGHOST"], profile_defaults.get("host", "localhost"))
    port_str = _resolve_db_param(
        str(args.db_port) if args.db_port is not None else None,
        ["DBT_PORT", "PGPORT"],
        profile_defaults.get("port", "6543"),
    )
    try:
        port = int(port_str) if port_str is not None else 6543
    except ValueError:
        raise RuntimeError(f"Invalid DB port value: {port_str!r}")

    database = _resolve_db_param(args.db_name, ["DBT_DATABASE", "PGDATABASE"], profile_defaults.get("database"))
    user = _resolve_db_param(args.db_user, ["DBT_USER", "PGUSER"], profile_defaults.get("user"))
    password = _resolve_db_param(args.db_password, ["DBT_PASSWORD", "PGPASSWORD"], profile_defaults.get("password"))

    missing: list[str] = []
    if not database:
        missing.append("database (DBT_DATABASE / PGDATABASE / --db-name)")
    if not user:
        missing.append("user (DBT_USER / PGUSER / --db-user)")
    if not password:
        missing.append("password (DBT_PASSWORD / PGPASSWORD / --db-password)")

    if missing:
        raise RuntimeError(
            "Missing DB connection parameters:\n- " + "\n- ".join(missing)
        )

    try:
        return psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
        )
    except OperationalError as exc:
        raise RuntimeError(f"Failed to connect to database: {exc}") from exc


def _resolve_paths(args: argparse.Namespace) -> tuple[Path, Optional[Path], Optional[Path]]:
    project_root = Path(args.project_root).resolve()
    manifest_path = Path(args.manifest_path).resolve() if args.manifest_path else None
    output_dir = Path(args.output_dir).resolve() if args.output_dir else None
    return project_root, manifest_path, output_dir


def _build_vars_overrides(args: argparse.Namespace) -> dict[str, str]:
    vars_overrides: dict[str, str] = {}
    if args.export_format:
        vars_overrides["rowlineage_export_format"] = args.export_format
    if args.export_path:
        vars_overrides["rowlineage_export_path"] = args.export_path
    return vars_overrides


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)

    try:
        project_root, manifest_path, output_dir = _resolve_paths(args)
        conn = _get_connection(args, project_root)
    except Exception as e:
        print(f"[dbt-rowlineage] ERROR: {e}", file=sys.stderr)
        return 1

    vars_overrides = _build_vars_overrides(args)

    try:
        mappings = generate_lineage_for_project(
            conn=conn,
            project_root=project_root,
            manifest_path=manifest_path,
            output_dir=output_dir,
            vars=vars_overrides if vars_overrides else None,
        )
        print(f"[dbt-rowlineage] Generated {len(mappings)} lineage mappings.")
        return 0
    except Exception as e:
        print(f"[dbt-rowlineage] ERROR while generating lineage: {e}", file=sys.stderr)
        return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
