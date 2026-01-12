import re
from pathlib import Path


def test_clickhouse_ordered_array_agg_syntax():
    macro = Path("demo/macros/ordered_array_agg.sql").read_text()
    pattern = re.compile(
        r"groupArray\(\s*\{\{\s*value\s*\}\}\s+order by\s+\{\{\s*order_by\s*\}\}\s*\)",
        re.IGNORECASE,
    )
    assert pattern.search(macro)
