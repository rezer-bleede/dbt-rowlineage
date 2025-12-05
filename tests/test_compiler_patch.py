from dbt_rowlineage.compiler_patch import patch_compiled_sql
from dbt_rowlineage.utils.sql import TRACE_COLUMN, TRACE_EXPRESSION, normalize_whitespace


def test_patch_compiled_sql_injects_trace_column():
    raw = "select id, value from my_table"
    patched = patch_compiled_sql(raw)
    normalized = normalize_whitespace(patched)
    assert TRACE_COLUMN in patched
    assert normalized.startswith(f"select {TRACE_EXPRESSION} as {TRACE_COLUMN}, id")


def test_patch_compiled_sql_skips_when_present():
    raw = f"select {TRACE_EXPRESSION} as {TRACE_COLUMN}, id from my_table"
    patched = patch_compiled_sql(raw)
    assert patched == raw


def test_patch_handles_distinct():
    raw = "select distinct id from t"
    patched = normalize_whitespace(patch_compiled_sql(raw))
    assert patched.startswith(f"select distinct {TRACE_EXPRESSION} as {TRACE_COLUMN}, id")


def test_patch_strips_jinja_from_sql():
    raw = "select id from t"
    patched = patch_compiled_sql(raw)
    assert "{{" not in patched and "}}" not in patched
