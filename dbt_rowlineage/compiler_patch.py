"""Compilation-time patching utilities."""

from __future__ import annotations

from . import sql_instrumentation
from .utils import sql as sql_utils


def patch_compiled_sql(compiled_sql: str) -> str:
    """Inject trace column into compiled SQL if enabled.

    If sqlglot is available and tokens mode is preferred (default), we use
    advanced instrumentation. Otherwise fallback to simple injection?
    Actually for now we just use the new instrumentation as per plan.
    """

    # We could check config here if we had access to it, but this hook 
    # is often stateless. 
    # For now, we assume we want the new behavior.
    # We should ideally detect dialect from dbt context if possible?
    # But this function doesn't receive it. 
    
    # Defaults to postgres if unknown, but instrument_sql is generic now.
    return sql_instrumentation.instrument_sql(compiled_sql, dialect="postgres")
