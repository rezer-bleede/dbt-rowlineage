"""Writer that exports lineage mappings to JSONL."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from ..tracer import MappingRecord


class JSONLWriter:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, mappings: Iterable[MappingRecord]) -> None:
        with self.path.open("a", encoding="utf-8") as fp:
            for mapping in mappings:
                fp.write(json.dumps(mapping))
                fp.write("\n")
