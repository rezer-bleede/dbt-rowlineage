"""Unit tests for SQL instrumentation under different dialects."""

import unittest
try:
    import sqlglot
except ImportError:
    sqlglot = None

from dbt_rowlineage.sql_instrumentation import instrument_sql
from dbt_rowlineage.utils.sql import PARENT_TRACE_COLUMN, TRACE_COLUMN

class TestSqlInstrumentation(unittest.TestCase):
    
    @unittest.skipIf(sqlglot is None, "sqlglot not installed")
    def test_instrument_basic_select_postgres(self):
        sql = "SELECT id, name FROM users"
        instrumented = instrument_sql(sql, dialect="postgres")
        
        # Postgres uses || for concat and ARRAYS
        # or CONCAT(...)
        self.assertTrue("||" in instrumented or "concat" in instrumented.lower())
        # ARRAY[...] or array(...)
        self.assertTrue("array[" in instrumented.lower() or "array(" in instrumented.lower())

    @unittest.skipIf(sqlglot is None, "sqlglot not installed")
    def test_instrument_basic_select_snowflake(self):
        sql = "SELECT id, name FROM users"
        
        instrumented = instrument_sql(sql, dialect="snowflake")
        
        # Snowflake uses ARRAY_CONSTRUCT or [...]
        self.assertTrue(TRACE_COLUMN in instrumented.lower())
        # Check basic valid SQL generation (no exception thrown above)

    @unittest.skipIf(sqlglot is None, "sqlglot not installed")
    def test_instrument_aggregation_row_reducing_postgres(self):
        sql = "SELECT region, count(*) FROM users GROUP BY region"
        instrumented = instrument_sql(sql, dialect="postgres")
        
        # Should rely on standard SQL array agg or similar (e.g. array_agg(distinct ...))
        self.assertTrue("rowlineage" not in instrumented.lower())
        
        # Check for array agg
        # sqlglot might output array_agg or array_unique_agg
        self.assertTrue("array_agg" in instrumented.lower() or "array_unique_agg" in instrumented.lower())

    @unittest.skipIf(sqlglot is None, "sqlglot not installed")
    def test_instrument_aggregation_row_reducing_snowflake(self):
        sql = "SELECT region, count(*) FROM users GROUP BY region"
        instrumented = instrument_sql(sql, dialect="snowflake")
        
        # Snowflake has ARRAY_UNIQUE_AGG or similar. 
        self.assertTrue("array_unique_agg" in instrumented.lower() or "array_agg" in instrumented.lower())

    @unittest.skipIf(sqlglot is None, "sqlglot not installed")
    def test_instrument_join_generic(self):
        sql = "SELECT u.id, o.amount FROM users u JOIN orders o ON u.id = o.user_id"
        # Use generic dialect? sqlglot default.
        instrumented = instrument_sql(sql, dialect="postgres")
        
        self.assertTrue(PARENT_TRACE_COLUMN in instrumented.lower())
        # Should combine traces
        self.assertTrue("concat" in instrumented.lower() or "||" in instrumented)
