import json
import sqlite3

import pandas as pd

from dbt_rowlineage.runtime_patch import capture_lineage
from dbt_rowlineage.writers.jsonl_writer import JSONLWriter
from dbt_rowlineage.writers.parquet_writer import ParquetWriter
from dbt_rowlineage.writers.table_writer import TableWriter


def sample_rows():
    return (
        [{"id": 1, "_row_trace_id": "src-1"}, {"id": 2, "_row_trace_id": "src-2"}],
        [{"id": 10}, {"id": 20}],
    )


def test_capture_lineage_returns_mappings(tmp_path):
    source_rows, target_rows = sample_rows()
    mappings = list(
        capture_lineage(
            source_rows,
            target_rows,
            "source.model",
            "target.model",
            "select statement",
        )
    )
    assert len(mappings) == 2
    assert all(m["source_model"] == "source.model" for m in mappings)


def test_jsonl_writer(tmp_path):
    source_rows, target_rows = sample_rows()
    mappings = capture_lineage(source_rows, target_rows, "s", "t", "sql")
    path = tmp_path / "lineage.jsonl"
    writer = JSONLWriter(path)
    writer.write(mappings)
    with path.open() as fp:
        lines = [json.loads(line) for line in fp.readlines()]
    assert len(lines) == 2
    assert lines[0]["source_model"] == "s"


def test_parquet_writer(tmp_path):
    source_rows, target_rows = sample_rows()
    mappings = capture_lineage(source_rows, target_rows, "s", "t", "sql")
    path = tmp_path / "lineage.parquet"
    writer = ParquetWriter(path)
    writer.write(mappings)
    assert path.exists()
    frame = pd.read_parquet(path)
    assert len(frame) == 2


def test_table_writer_round_trip(tmp_path):
    db_path = tmp_path / "lineage.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE lineage__mappings (
            source_model TEXT,
            target_model TEXT,
            source_trace_id TEXT,
            target_trace_id TEXT,
            compiled_sql TEXT,
            executed_at TEXT
        )
        """
    )
    source_rows, target_rows = sample_rows()
    mappings = capture_lineage(source_rows, target_rows, "s", "t", "sql")
    writer = TableWriter(conn)
    writer.write(mappings)

    rows = list(conn.execute("SELECT source_model, target_model, source_trace_id, target_trace_id FROM lineage__mappings"))
    assert len(rows) == 2
    assert rows[0][0] == "s"
    conn.close()
