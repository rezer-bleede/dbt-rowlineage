from __future__ import annotations

from pathlib import Path

from dbt_rowlineage import RowLineagePlugin
from dbt_rowlineage import auto


class DummyWriter:
    def __init__(self):
        self.written = []

    def write(self, mappings):
        self.written.extend(mappings)


class DummyPlugin(RowLineagePlugin):
    def __init__(self):
        super().__init__()
        self.calls = []

    def capture_lineage(self, source_rows, target_rows, source_model: str, target_model: str, compiled_sql: str):
        self.calls.append((source_model, target_model, compiled_sql))
        return [
            {
                "source_model": source_model,
                "target_model": target_model,
                "source_trace_id": "s",
                "target_trace_id": "t",
                "compiled_sql": compiled_sql,
                "executed_at": "now",
            }
        ]


def test_generate_lineage_uses_plugin_capture_lineage(monkeypatch, tmp_path):
    manifest = {
        "nodes": {
            "model.project.upstream": {
                "resource_type": "model",
                "schema": "public",
                "name": "upstream",
                "depends_on": {"nodes": []},
            },
            "model.project.downstream": {
                "resource_type": "model",
                "schema": "public",
                "name": "downstream",
                "compiled_code": "select * from upstream",
                "depends_on": {"nodes": ["model.project.upstream"]},
            },
        }
    }

    monkeypatch.setattr(auto, "_load_manifest", lambda _: manifest)
    monkeypatch.setattr(auto, "_trace_column_exists", lambda *_: False)
    monkeypatch.setattr(auto, "_fetch_rows", lambda *_args, **_kwargs: [{"id": 1}])

    writer = DummyWriter()
    monkeypatch.setattr(auto, "_get_writer", lambda _plugin, _output_dir: writer)

    plugin = DummyPlugin()

    result = auto.generate_lineage_for_project(
        conn=object(),
        project_root=Path(tmp_path),
        plugin=plugin,
        manifest_path=Path(tmp_path) / "manifest.json",
        output_dir=Path(tmp_path) / "output",
    )

    assert len(plugin.calls) == 1
    assert plugin.calls[0][:2] == ("upstream", "downstream")
    assert writer.written == result
    assert result[0]["compiled_sql"] == "select * from upstream"
