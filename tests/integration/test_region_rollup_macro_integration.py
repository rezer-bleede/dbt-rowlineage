from pathlib import Path


def test_region_rollup_uses_ordered_array_agg():
    model_sql = Path("demo/models/marts/region_rollup.sql").read_text()
    assert "ordered_array_agg('customer_name_upper', 'id')" in model_sql


def test_clickhouse_macro_keeps_ordered_array_agg():
    macro = Path("demo/macros/ordered_array_agg.sql").read_text()
    assert "groupArray" in macro
    assert "arraySort" in macro
