"""Writer that exports lineage mappings to a parquet file."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

import pandas as pd

from ..tracer import MappingRecord


class ParquetWriter:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, mappings: Iterable[MappingRecord]) -> None:
        rows: List[MappingRecord] = list(mappings)
        if not rows:
            return
        frame = pd.DataFrame(rows)
        frame.to_parquet(self.path, index=False)
