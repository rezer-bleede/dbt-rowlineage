"""UUID helpers for deterministic trace id generation."""

from __future__ import annotations

import uuid
from typing import Any, Dict


def deterministic_uuid(payload: Dict[str, Any] | str) -> str:
    """Return a deterministic UUID5 for the given payload.

    Accepts either a string seed or a mapping. For mappings, the keys are
    sorted to guarantee reproducibility across runs and Python versions.
    """

    if isinstance(payload, str):
        seed = payload
    else:
        seed = _normalize_payload(payload)
    digest = uuid.uuid5(uuid.NAMESPACE_URL, seed)
    return str(digest)


def _normalize_payload(payload: Dict[str, Any]) -> str:
    parts = []
    for key in sorted(payload.keys()):
        value = payload[key]
        parts.append(f"{key}:{_stringify(value)}")
    return "|".join(parts)


def _stringify(value: Any) -> str:
    if isinstance(value, dict):
        return _normalize_payload(value)
    if isinstance(value, (list, tuple)):
        normalized_items = ",".join(_stringify(item) for item in value)
        return f"[{normalized_items}]"
    if value is None:
        return "<null>"
    return str(value)


def new_trace_id(row: Dict[str, Any]) -> str:
    """Generate a deterministic trace id for a row.

    The row content (excluding the existing ``_row_trace_id`` if present) is
    hashed so that tests can rely on stable ids without reaching for random
    UUID generation.
    """

    scrubbed = {k: v for k, v in row.items() if k != "_row_trace_id"}
    return deterministic_uuid(scrubbed or "empty-row")
