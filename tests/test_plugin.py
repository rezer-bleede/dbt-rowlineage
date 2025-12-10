from __future__ import annotations

from dbt_rowlineage import RowLineagePlugin
from dbt_rowlineage import plugin as plugin_module


def test_plugin_exposes_capture_lineage(monkeypatch):
    plugin = RowLineagePlugin()

    calls = {}

    def fake_capture(source_rows, target_rows, source_model, target_model, compiled_sql, config):
        calls["config"] = config
        calls["compiled_sql"] = compiled_sql
        return ["ok"]

    monkeypatch.setattr(plugin_module, "capture_lineage", fake_capture)

    result = plugin.capture_lineage(
        source_rows=[{"id": 1}],
        target_rows=[{"id": 2}],
        source_model="source_model",
        target_model="target_model",
        compiled_sql="select 1",
    )

    assert result == ["ok"]
    assert calls["config"] is plugin.config
    assert calls["compiled_sql"] == "select 1"
