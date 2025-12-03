"""Compilation-time patching utilities."""

from __future__ import annotations

from .utils import sql as sql_utils


def patch_compiled_sql(compiled_sql: str) -> str:
    """Inject trace column into compiled SQL if enabled.

    This function can be wired into dbt's compilation pipeline by wrapping the
    ``SqlNode.compile`` or equivalent hook. Here we operate purely on strings to
    keep the package lightweight and test-friendly.
    """

    return sql_utils.inject_trace_column(compiled_sql)
