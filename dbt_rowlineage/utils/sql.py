"""SQL helper utilities for trace column injection."""

from __future__ import annotations

import re

TRACE_COLUMN = "_row_trace_id"
PARENT_TRACE_COLUMN = "_row_parent_trace_ids"
TRACE_ALIAS = f"{TRACE_COLUMN}"
TRACE_EXPRESSION = "md5(random()::text || clock_timestamp()::text)::uuid"


def has_trace_column(sql: str) -> bool:
    pattern = re.compile(r"\b" + re.escape(TRACE_COLUMN) + r"\b", re.IGNORECASE)
    return bool(pattern.search(sql))


def inject_trace_column(sql: str) -> str:
    """Inject the trace column into the top-level SELECT list.

    The logic is intentionally conservative and only operates on simple SELECT
    statements. Complex SQL should be handled upstream by dbt's Jinja context,
    but this helper keeps the behaviour deterministic for unit tests.
    """

    if has_trace_column(sql):
        return sql

    select_match = re.match(r"\s*select\s", sql, flags=re.IGNORECASE)
    if not select_match:
        return sql

    # Split only on the first FROM to avoid rewriting subqueries.
    lower_sql = sql.lower()
    from_idx = lower_sql.find(" from ")
    if from_idx == -1:
        return sql

    select_clause = sql[:from_idx]
    rest = sql[from_idx:]
    # Ensure comma placement is predictable.
    if select_clause.strip().lower().startswith("select distinct"):
        prefix = "select distinct"
        trailing = select_clause[len(prefix):]
        new_select = f"{prefix} {TRACE_EXPRESSION} as {TRACE_ALIAS},{trailing}"
    else:
        prefix = "select"
        trailing = select_clause[len(prefix):]
        new_select = f"{prefix} {TRACE_EXPRESSION} as {TRACE_ALIAS},{trailing}"

    return new_select + rest


def normalize_whitespace(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()
