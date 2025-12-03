import datetime
from dbt_rowlineage.tracer import RowLineageTracer
from dbt_rowlineage.config import RowLineageConfig


def test_build_mappings_assigns_trace_ids():
    tracer = RowLineageTracer(RowLineageConfig())
    source_rows = [
        {"id": 1, "value": "a"},
        {"id": 2, "value": "b"},
    ]
    target_rows = [
        {"id": 101, "value": "a"},
        {"id": 102, "value": "b"},
    ]

    mappings = tracer.build_mappings(
        source_rows=source_rows,
        target_rows=target_rows,
        source_model="src.model",
        target_model="target.model",
        compiled_sql="select * from something",
    )

    assert len(mappings) == 2
    for mapping in mappings:
        assert mapping["source_trace_id"].startswith("")
        assert mapping["target_trace_id"].startswith("")
        # executed_at should be iso formatted datetime
        datetime.datetime.fromisoformat(mapping["executed_at"])


def test_build_mappings_respects_existing_trace_id():
    tracer = RowLineageTracer(RowLineageConfig())
    source_rows = [{"id": 1, "_row_trace_id": "source-uuid"}]
    target_rows = [{"id": 1, "_row_trace_id": "target-uuid"}]

    mappings = tracer.build_mappings(
        source_rows, target_rows, "src", "tgt", "select *"
    )

    assert mappings[0]["source_trace_id"] == "source-uuid"
    assert mappings[0]["target_trace_id"] == "target-uuid"
