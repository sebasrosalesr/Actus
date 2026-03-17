from __future__ import annotations

from dataclasses import asdict, is_dataclass
import gzip
import json
import os
from pathlib import Path
from typing import Any


def default_canonical_snapshot_path() -> Path:
    raw = (
        os.environ.get("ACTUS_RAG_CANONICAL_SNAPSHOT_PATH", "").strip()
        or os.environ.get("ACTUS_NEW_RAG_CANONICAL_SNAPSHOT_PATH", "").strip()
    )
    if raw:
        return Path(raw)
    return Path(__file__).resolve().parents[3] / "rag_data" / "canonical_tickets.json.gz"


def _to_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_to_jsonable(item) for item in value]
    return value


def save_canonical_tickets(
    canonical_tickets: dict[str, Any],
    path: str | Path | None = None,
) -> Path:
    target = Path(path) if path is not None else default_canonical_snapshot_path()
    target.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "ticket_count": len(canonical_tickets),
        "canonical_tickets": _to_jsonable(canonical_tickets),
    }
    with gzip.open(target, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"), ensure_ascii=True)
    return target


def load_canonical_tickets(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else default_canonical_snapshot_path()
    with gzip.open(target, "rt", encoding="utf-8") as handle:
        payload = json.load(handle)

    canonical_tickets = payload.get("canonical_tickets")
    if not isinstance(canonical_tickets, dict):
        raise RuntimeError(f"Canonical snapshot at {target} is invalid.")
    return canonical_tickets
