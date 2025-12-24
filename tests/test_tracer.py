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


def test_build_mappings_matches_aggregations_to_all_sources():
    tracer = RowLineageTracer(RowLineageConfig())
    source_rows = [
        {"region": "north", "customer_name_upper": "ALICE"},
        {"region": "north", "customer_name_upper": "DAVID"},
        {"region": "south", "customer_name_upper": "BOB"},
    ]
    target_rows = [
        {"region": "north", "customer_count": 2},
        {"region": "south", "customer_count": 1},
    ]

    mappings = tracer.build_mappings(
        source_rows=source_rows,
        target_rows=target_rows,
        source_model="staging_model",
        target_model="region_rollup",
        compiled_sql="select region, count(*) from staging_model group by region",
    )

    assert len(mappings) == 3
    target_traces = [m["target_trace_id"] for m in mappings]
    assert len(set(target_traces)) == 2
    assert any(target_traces.count(trace_id) >= 2 for trace_id in set(target_traces))
