"""Unit tests for tracer scenarios with token-based lineage."""

from typing import Dict, Any, List
from dbt_rowlineage.tracer import RowLineageTracer
from dbt_rowlineage.config import RowLineageConfig

def make_tracer():
    config = RowLineageConfig(lineage_mode="tokens")
    return RowLineageTracer(config)

def test_count_rollup_no_groupby():
    """
    Scenario:
    source: staging_model (4 rows)
    target: count_model (1 row)
    
    Target row contains tokens for all 4 source rows.
    Expect 4 mappings.
    """
    tracer = make_tracer()
    
    # Source rows (only used for trace lookup if needed, but in tokens mode usually ignored for matching)
    source_rows = [
        {"id": 1, "_row_trace_id": "uuid1"},
        {"id": 2, "_row_trace_id": "uuid2"},
        {"id": 3, "_row_trace_id": "uuid3"},
        {"id": 4, "_row_trace_id": "uuid4"},
    ]
    
    # Target row has tokens
    target_row = {
        "count": 4,
        "_row_trace_id": "uuid_target",
        "_row_parent_trace_ids": [
            "staging_model:uuid1",
            "staging_model:uuid2",
            "staging_model:uuid3",
            "staging_model:uuid4",
        ]
    }
    
    mappings = tracer.build_mappings(
        source_rows=source_rows,
        target_rows=[target_row],
        source_model="staging_model",
        target_model="count_model",
        compiled_sql="SELECT count(*) ... "
    )
    
    assert len(mappings) == 4
    source_ids = {m["source_trace_id"] for m in mappings}
    assert source_ids == {"uuid1", "uuid2", "uuid3", "uuid4"}
    for m in mappings:
        assert m["target_trace_id"] == "uuid_target"


def test_join_model_two_upstreams():
    """
    Scenario:
    upstreams: model_a (uuidA), model_b (uuidB)
    target: join_model (joined row)
    
    Target row has tokens ["model_a:uuidA", "model_b:uuidB"]
    
    When processing edge model_a -> join_model, should only emit uuidA.
    """
    tracer = make_tracer()
    
    target_row = {
        "id": 1,
        "_row_trace_id": "uuid_join",
        "_row_parent_trace_ids": ["model_a:uuidA", "model_b:uuidB"]
    }
    
    # Edge A -> Join
    mappings_a = tracer.build_mappings(
        source_rows=[{"_row_trace_id": "uuidA"}],
        target_rows=[target_row],
        source_model="model_a",
        target_model="join_model",
        compiled_sql="..."
    )
    
    assert len(mappings_a) == 1
    assert mappings_a[0]["source_trace_id"] == "uuidA"
    
    # Edge B -> Join
    mappings_b = tracer.build_mappings(
        source_rows=[{"_row_trace_id": "uuidB"}],
        target_rows=[target_row],
        source_model="model_b",
        target_model="join_model",
        compiled_sql="..."
    )
    
    assert len(mappings_b) == 1
    assert mappings_b[0]["source_trace_id"] == "uuidB"


def test_aggregation_on_aggregated_table():
    """
    Scenario:
    upstream: region_rollup (has own trace ids)
    downstream: windows_rollup (aggregates region_rollup)
    
    windows_rollup row tokens should point to region_rollup IDs.
    """
    tracer = make_tracer()
    
    target_row = {
        "val": 100,
        "_row_trace_id": "uuid_win",
        "_row_parent_trace_ids": ["region_rollup:uuidAggWest", "region_rollup:uuidAggEast"]
    }
    
    mappings = tracer.build_mappings(
        source_rows=[], # Empty should work in tokens mode
        target_rows=[target_row],
        source_model="region_rollup",
        target_model="windows_rollup",
        compiled_sql="..."
    )
    
    assert len(mappings) == 2
    assert {m["source_trace_id"] for m in mappings} == {"uuidAggWest", "uuidAggEast"}
