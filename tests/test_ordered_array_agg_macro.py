import re
from pathlib import Path


def test_clickhouse_ordered_array_agg_syntax():
    macro = Path("demo/macros/ordered_array_agg.sql").read_text()
    pattern = re.compile(
        r"arrayMap\(\s*x\s*->\s*x\.2\s*,\s*arraySort\(\s*x\s*->\s*x\.1\s*,\s*groupArray\(\s*\(\s*\{\{\s*order_by\s*\}\}\s*,\s*\{\{\s*value\s*\}\}\s*\)\s*\)\s*\)\s*\)",
        re.IGNORECASE,
    )
    assert pattern.search(macro)
