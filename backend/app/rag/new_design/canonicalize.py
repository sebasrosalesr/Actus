from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from .chunking import build_investigation_section_chunks, clean_investigation_html
from .models import CanonicalTicket, InvestigationNote, RootCauseRule, TicketLine
from .root_cause import detect_root_causes


def _clean_scalar(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _norm_upper(value: Any) -> str | None:
    text = _clean_scalar(value)
    return text.upper() if text else None


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).strip().replace(",", "").replace("$", "")
    if not text or text.lower() == "nan":
        return 0.0
    try:
        return float(text)
    except Exception:
        return 0.0


def _extract_account_prefix(value: str | None) -> str | None:
    if not value:
        return None
    match = re.match(r"^[A-Z]+", str(value).upper())
    return match.group(0) if match else None


def _unique(values: list[str | None]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _note_combo_key(invoice_number: str | None, item_number: str | None, fallback: str | None = None) -> str | None:
    if fallback and str(fallback).strip():
        return str(fallback).strip().upper()
    if invoice_number and item_number:
        return f"{invoice_number}|{item_number}"
    return None


def build_line_map(credit_rows: list[dict[str, Any]]) -> dict[str, dict[str, list[TicketLine]]]:
    ticket_line_map: dict[str, dict[str, list[TicketLine]]] = {}

    for row_idx, row in enumerate(credit_rows):
        ticket_id = _norm_upper(row.get("Ticket Number") or row.get("ticket_id"))
        invoice = _norm_upper(row.get("Invoice Number") or row.get("invoice_number"))
        item = _norm_upper(row.get("Item Number") or row.get("item_number"))

        if not ticket_id or not invoice or not item:
            continue

        combo_key = f"{invoice}|{item}"
        ticket_line_map.setdefault(ticket_id, {})
        ticket_line_map[ticket_id].setdefault(combo_key, [])

        reason = _clean_scalar(row.get("Reason for Credit") or row.get("reason_for_credit"))
        reasons = [reason] if reason else []

        line = TicketLine(
            row_index=row_idx,
            invoice_number=invoice,
            item_number=item,
            combo_key=combo_key,
            reason_for_credit_raw_list=reasons,
            credit_request_total=_safe_float(
                row.get("Credit Request Total") or row.get("credit_request_total")
            ),
        )
        ticket_line_map[ticket_id][combo_key].append(line)

    return ticket_line_map


def build_canonical_investigation_note(row: dict[str, Any]) -> InvestigationNote | None:
    ticket_id = _norm_upper(row.get("ticket_number") or row.get("ticket_id") or row.get("Ticket Number"))
    if not ticket_id:
        return None

    invoice = _norm_upper(row.get("invoice_number") or row.get("Invoice Number"))
    item = _norm_upper(row.get("item_number") or row.get("Item Number"))
    combo_key = _note_combo_key(invoice, item, row.get("combo_key"))

    note_id = _clean_scalar(row.get("note_id")) or "unknown"
    body_raw = str(row.get("body") or row.get("body_raw") or row.get("html") or "")
    body_clean = clean_investigation_html(body_raw)

    return InvestigationNote(
        ticket_id=ticket_id,
        invoice_number=invoice,
        item_number=item,
        combo_key=combo_key,
        note_id=note_id,
        title=_clean_scalar(row.get("title")),
        body_raw=body_raw,
        body_clean=body_clean,
        customer_number=_norm_upper(row.get("customer_number")),
        created_at=_clean_scalar(row.get("created_at")),
        created_by=_clean_scalar(row.get("created_by")),
        updated_at=_clean_scalar(row.get("updated_at")),
        updated_by=_clean_scalar(row.get("updated_by")),
    )


def attach_investigation_notes(
    ticket_line_map: dict[str, dict[str, list[TicketLine]]],
    investigation_rows: list[dict[str, Any]],
    fallback_max_chars: int = 700,
) -> None:
    for row in investigation_rows:
        note = build_canonical_investigation_note(row)
        if note is None or not note.combo_key:
            continue

        by_ticket = ticket_line_map.get(note.ticket_id)
        if by_ticket is None:
            continue

        line_list = by_ticket.get(note.combo_key)
        if not line_list:
            continue

        chunks = build_investigation_section_chunks(note, fallback_max_chars=fallback_max_chars)

        for line in line_list:
            line.investigation_notes.append(note)
            line.investigation_chunks.extend(chunks)


def compute_line_root_causes(
    ticket_line_map: dict[str, dict[str, list[TicketLine]]],
    rules: list[RootCauseRule],
) -> None:
    for _, combo_map in ticket_line_map.items():
        for _, lines in combo_map.items():
            for line in lines:
                evidence = list(line.reason_for_credit_raw_list)
                for chunk in line.investigation_chunks:
                    text = str(chunk.chunk_text)
                    if line.item_number in text or line.invoice_number in text:
                        evidence.append(text)

                result = detect_root_causes(evidence, rules)
                line.root_cause_ids = list(result["root_cause_ids"])
                line.root_cause_labels = list(result["root_cause_labels"])
                line.root_cause_primary_id = str(result["root_cause_primary_id"])
                line.root_cause_primary_label = str(result["root_cause_primary_label"])
                line.root_cause_triggers = list(result["root_cause_triggers"])
                line.root_cause_score = float(result["root_cause_score"])


def _build_ticket_summary_map(credit_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summary_map: dict[str, dict[str, Any]] = {}

    for row in credit_rows:
        ticket_id = _norm_upper(row.get("Ticket Number") or row.get("ticket_id"))
        if not ticket_id:
            continue

        summary = summary_map.setdefault(
            ticket_id,
            {
                "ticket_id": ticket_id,
                "source_nodes": ["credit_requests"],
                "customer_numbers": [],
                "sales_reps": [],
                "invoice_numbers": [],
                "item_numbers": [],
                "credit_numbers": [],
                "credit_request_totals": [],
                "reason_for_credit_raw_list": [],
                "status_raw_list": [],
            },
        )

        summary["customer_numbers"].append(_norm_upper(row.get("Customer Number") or row.get("customer_number")))
        summary["sales_reps"].append(_norm_upper(row.get("Sales Rep") or row.get("sales_rep")))
        summary["invoice_numbers"].append(_norm_upper(row.get("Invoice Number") or row.get("invoice_number")))
        summary["item_numbers"].append(_norm_upper(row.get("Item Number") or row.get("item_number")))
        summary["credit_numbers"].append(_norm_upper(row.get("RTN_CR_No") or row.get("credit_number")))

        total = _safe_float(row.get("Credit Request Total") or row.get("credit_request_total"))
        if total:
            summary["credit_request_totals"].append(total)

        reason = _clean_scalar(row.get("Reason for Credit") or row.get("reason_for_credit"))
        if reason:
            summary["reason_for_credit_raw_list"].append(reason)

        status = _clean_scalar(row.get("Status") or row.get("status"))
        if status:
            summary["status_raw_list"].append(status)

    for ticket_id, summary in summary_map.items():
        for key in (
            "customer_numbers",
            "sales_reps",
            "invoice_numbers",
            "item_numbers",
            "credit_numbers",
            "reason_for_credit_raw_list",
            "status_raw_list",
        ):
            summary[key] = _unique(summary[key])

        prefixes = [_extract_account_prefix(c) for c in summary["customer_numbers"]]
        summary["account_prefixes"] = _unique(prefixes)

    return summary_map


def _aggregate_ticket_root_causes(line_map: dict[str, list[TicketLine]]) -> dict[str, Any]:
    counts: dict[str, int] = defaultdict(int)
    label_by_id: dict[str, str] = {}
    score_by_id: dict[str, float] = defaultdict(float)
    trigger_by_id: dict[str, set[str]] = defaultdict(set)

    for lines in line_map.values():
        for line in lines:
            for cid, label in zip(line.root_cause_ids, line.root_cause_labels):
                if not cid:
                    continue
                counts[cid] += 1
                label_by_id[cid] = label
                score_by_id[cid] += float(line.root_cause_score)
            for trigger in line.root_cause_triggers:
                if line.root_cause_primary_id:
                    trigger_by_id[line.root_cause_primary_id].add(trigger)

    ranked_ids = sorted(counts.keys(), key=lambda cid: (-counts[cid], cid))
    if not ranked_ids:
        return {
            "root_cause_ids": [],
            "root_cause_labels": [],
            "root_cause_primary_id": "unidentified",
            "root_cause_primary_label": "Unidentified",
            "root_cause_triggers": [],
        }

    primary_id = ranked_ids[0]
    return {
        "root_cause_ids": ranked_ids,
        "root_cause_labels": [label_by_id.get(cid, cid) for cid in ranked_ids],
        "root_cause_primary_id": primary_id,
        "root_cause_primary_label": label_by_id.get(primary_id, primary_id),
        "root_cause_triggers": sorted(trigger_by_id.get(primary_id, set())),
    }


def build_canonical_tickets(
    credit_rows: list[dict[str, Any]],
    ticket_line_map: dict[str, dict[str, list[TicketLine]]],
) -> dict[str, CanonicalTicket]:
    summary_map = _build_ticket_summary_map(credit_rows)
    all_ticket_ids = sorted(set(summary_map.keys()) | set(ticket_line_map.keys()))

    tickets: dict[str, CanonicalTicket] = {}

    for ticket_id in all_ticket_ids:
        summary = summary_map.get(ticket_id, {})
        line_map = ticket_line_map.get(ticket_id, {})

        all_invoice_numbers = set(summary.get("invoice_numbers", []))
        all_item_numbers = set(summary.get("item_numbers", []))
        all_credit_totals = list(summary.get("credit_request_totals", []))

        for lines in line_map.values():
            for line in lines:
                all_invoice_numbers.add(line.invoice_number)
                all_item_numbers.add(line.item_number)
                if line.credit_request_total:
                    all_credit_totals.append(line.credit_request_total)

        root_summary = _aggregate_ticket_root_causes(line_map)

        source_nodes = set(summary.get("source_nodes", ["credit_requests"]))
        if line_map:
            source_nodes.add("investigation_notes")

        tickets[ticket_id] = CanonicalTicket(
            ticket_id=ticket_id,
            source_nodes=sorted(source_nodes),
            customer_numbers=list(summary.get("customer_numbers", [])),
            sales_reps=list(summary.get("sales_reps", [])),
            invoice_numbers=sorted(v for v in all_invoice_numbers if v),
            item_numbers=sorted(v for v in all_item_numbers if v),
            credit_numbers=list(summary.get("credit_numbers", [])),
            credit_request_totals=all_credit_totals,
            reason_for_credit_raw_list=list(summary.get("reason_for_credit_raw_list", [])),
            status_raw_list=list(summary.get("status_raw_list", [])),
            root_cause_ids=list(root_summary["root_cause_ids"]),
            root_cause_labels=list(root_summary["root_cause_labels"]),
            root_cause_primary_id=str(root_summary["root_cause_primary_id"]),
            root_cause_primary_label=str(root_summary["root_cause_primary_label"]),
            root_cause_triggers=list(root_summary["root_cause_triggers"]),
            line_map=line_map,
            account_prefixes=list(summary.get("account_prefixes", [])),
        )

    return tickets
