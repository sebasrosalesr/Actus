from __future__ import annotations

from dataclasses import asdict, is_dataclass
import gzip
import json
import os
from pathlib import Path
from typing import Any

from .models import CanonicalTicket, InvestigationChunk, InvestigationNote, TicketLine


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


def _hydrate_investigation_chunk(payload: Any) -> InvestigationChunk:
    if isinstance(payload, InvestigationChunk):
        return payload
    if not isinstance(payload, dict):
        raise RuntimeError("Canonical snapshot investigation chunk is invalid.")
    return InvestigationChunk(**payload)


def _hydrate_investigation_note(payload: Any) -> InvestigationNote:
    if isinstance(payload, InvestigationNote):
        return payload
    if not isinstance(payload, dict):
        raise RuntimeError("Canonical snapshot investigation note is invalid.")
    return InvestigationNote(**payload)


def _hydrate_ticket_line(payload: Any) -> TicketLine:
    if isinstance(payload, TicketLine):
        return payload
    if not isinstance(payload, dict):
        raise RuntimeError("Canonical snapshot ticket line is invalid.")

    line_payload = dict(payload)
    line_payload.setdefault("row_index", 0)
    line_payload.setdefault(
        "combo_key",
        f"{line_payload.get('invoice_number') or ''}|{line_payload.get('item_number') or ''}",
    )
    line_payload.setdefault("reason_for_credit_raw_list", [])
    line_payload.setdefault("investigation_notes", [])
    line_payload.setdefault("investigation_chunks", [])
    line_payload.setdefault("root_cause_ids", [])
    line_payload.setdefault("root_cause_labels", [])
    line_payload.setdefault("root_cause_primary_id", "unidentified")
    line_payload.setdefault("root_cause_primary_label", "Unidentified")
    line_payload.setdefault("root_cause_triggers", [])
    line_payload.setdefault("root_cause_score", 0.0)
    line_payload.setdefault("credit_request_total", 0.0)
    line_payload["investigation_chunks"] = [
        _hydrate_investigation_chunk(value)
        for value in (line_payload.get("investigation_chunks") or [])
    ]
    line_payload["investigation_notes"] = [
        _hydrate_investigation_note(value)
        for value in (line_payload.get("investigation_notes") or [])
    ]
    return TicketLine(**line_payload)


def _hydrate_canonical_ticket(payload: Any) -> CanonicalTicket:
    if isinstance(payload, CanonicalTicket):
        return payload
    if not isinstance(payload, dict):
        raise RuntimeError("Canonical snapshot ticket payload is invalid.")

    ticket_payload = dict(payload)
    ticket_payload.setdefault("source_nodes", [])
    ticket_payload.setdefault("customer_numbers", [])
    ticket_payload.setdefault("sales_reps", [])
    ticket_payload.setdefault("invoice_numbers", [])
    ticket_payload.setdefault("item_numbers", [])
    ticket_payload.setdefault("credit_numbers", [])
    ticket_payload.setdefault("credit_request_totals", [])
    ticket_payload.setdefault("reason_for_credit_raw_list", [])
    ticket_payload.setdefault("status_raw_list", [])
    ticket_payload.setdefault("root_cause_ids", [])
    ticket_payload.setdefault("root_cause_labels", [])
    ticket_payload.setdefault("root_cause_primary_id", "unidentified")
    ticket_payload.setdefault("root_cause_primary_label", "Unidentified")
    ticket_payload.setdefault("root_cause_triggers", [])
    ticket_payload.setdefault("line_map", {})
    ticket_payload.setdefault("account_prefixes", [])
    ticket_payload["line_map"] = {
        str(combo_key): [_hydrate_ticket_line(line) for line in (lines or [])]
        for combo_key, lines in (ticket_payload.get("line_map") or {}).items()
    }
    return CanonicalTicket(**ticket_payload)


def load_canonical_ticket_models(path: str | Path | None = None) -> dict[str, CanonicalTicket]:
    raw = load_canonical_tickets(path)
    return {
        str(ticket_id).strip().upper(): _hydrate_canonical_ticket(ticket_payload)
        for ticket_id, ticket_payload in raw.items()
    }
