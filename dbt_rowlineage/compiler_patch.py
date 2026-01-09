"""Compilation-time patching utilities."""

from __future__ import annotations

from .utils import sql as sql_utils


def patch_compiled_sql(compiled_sql: str) -> str:
    """Inject trace column into compiled SQL if enabled.

    The compiler hook keeps the injection lightweight and deterministic by
    using the string-based helper from ``dbt_rowlineage.utils.sql``.
    """

    return sql_utils.inject_trace_column(compiled_sql)
