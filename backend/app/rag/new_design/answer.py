from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from .models import SearchResult
from .retrieve import AGING_QUERY_PATTERNS


def clean_answer_text(text: str, max_len: int = 220) -> str:
    value = re.sub(r"\s+", " ", str(text or "").replace("\n", " ")).strip()
    if len(value) <= max_len:
        return value
    return value[:max_len].rsplit(" ", 1)[0] + "..."


def group_results_by_ticket(results: list[SearchResult], max_chunks_per_ticket: int = 2) -> list[dict[str, Any]]:
    grouped: dict[str, list[SearchResult]] = defaultdict(list)
    for result in results:
        grouped[result.ticket_id].append(result)

    rows: list[dict[str, Any]] = []
    for ticket_id, items in grouped.items():
        items = sorted(items, key=lambda r: r.score, reverse=True)[:max_chunks_per_ticket]
        best = items[0]
        root_causes: list[str] = []
        for item in items:
            for root in item.metadata.get("root_cause_ids", []):
                if root not in root_causes:
                    root_causes.append(root)

        rows.append(
            {
                "ticket_id": ticket_id,
                "best_score": best.score,
                "intent": best.intent,
                "top_chunk_type": best.chunk_type,
                "root_cause_ids": root_causes,
                "chunks": items,
            }
        )

    rows.sort(key=lambda row: row["best_score"], reverse=True)
    return rows


def summarize_grouped_ticket_result(group: dict[str, Any]) -> str:
    ticket_id = group["ticket_id"]
    root_causes = group.get("root_cause_ids", [])
    chunks: list[SearchResult] = group.get("chunks", [])

    if not chunks:
        return f"{ticket_id}: no supporting evidence found."

    top = chunks[0]
    metadata = top.metadata or {}
    evidence_text = _resolve_evidence_text(chunks)

    if top.chunk_type == "ticket_line_summary":
        item = metadata.get("item_number")
        invoice = metadata.get("invoice_number")
        prefix = [ticket_id]
        if item:
            prefix.append(f"for item {item}")
        if invoice:
            prefix.append(f"on invoice {invoice}")

        cause_text = f"Root cause: {', '.join(root_causes)}." if root_causes else ""
        return f"{' '.join(prefix)}. {cause_text} Evidence: {evidence_text}"

    if top.chunk_type == "ticket_summary":
        summary_text = clean_answer_text(top.text, max_len=220)
        return f"{ticket_id}. {summary_text} Evidence: {evidence_text}"

    return f"{ticket_id}. Evidence: {evidence_text}"


def answer_from_results(query: str, results: list[SearchResult], max_tickets_in_answer: int = 5) -> str:
    query_lower = str(query or "").lower()
    is_aging_query = any(pattern in query_lower for pattern in AGING_QUERY_PATTERNS)

    if is_aging_query or (results and results[0].intent == "aging_lookup"):
        return _format_aging_answer(results, threshold_days=30, max_tickets_in_answer=max_tickets_in_answer)

    grouped = group_results_by_ticket(results)
    top = grouped[:max_tickets_in_answer]

    if not top:
        return f"I couldn't find strong matches for: {query}"

    lines = [f"Top results for: {query}"]
    for row in top:
        lines.append(f"- {summarize_grouped_ticket_result(row)}")
    return "\n".join(lines)


def _clean_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    normalized = re.sub(r"[\s\.\,\;\:]+$", "", text).strip().lower()
    if not text or normalized in {"none", "null", "n/a", "na"}:
        return None
    return text


def _extract_reason_from_chunk(chunk: SearchResult) -> str | None:
    metadata = chunk.metadata or {}
    reason_list = metadata.get("reason_for_credit_raw_list")
    if isinstance(reason_list, list):
        for reason in reason_list:
            text = _clean_optional(reason)
            if text:
                return clean_answer_text(text, max_len=180)
    reason = metadata.get("reason_for_credit")
    text = _clean_optional(reason)
    if text:
        return clean_answer_text(text, max_len=180)

    match = re.search(r"Reason for credit:\s*(.*)", chunk.text, flags=re.IGNORECASE)
    if match:
        parsed = _clean_optional(match.group(1))
        if parsed:
            return clean_answer_text(parsed, max_len=180)
    return None


def _extract_status_from_chunk(chunk: SearchResult) -> str | None:
    metadata = chunk.metadata or {}
    status_list = metadata.get("status_raw_list")
    if isinstance(status_list, list):
        for status in status_list:
            text = _clean_optional(status)
            if text:
                return clean_answer_text(text, max_len=180)

    status = _clean_optional(metadata.get("status"))
    if status:
        return clean_answer_text(status, max_len=180)

    match = re.search(r"Status:\s*(.*)", chunk.text, flags=re.IGNORECASE)
    if match:
        parsed = _clean_optional(match.group(1))
        if parsed:
            return clean_answer_text(parsed, max_len=180)
    return None


def _resolve_evidence_text(chunks: list[SearchResult]) -> str:
    # 1) investigation chunk text
    for chunk in chunks:
        if chunk.chunk_type != "ticket_investigation_section":
            continue
        text = _clean_optional(chunk.text)
        if text:
            return clean_answer_text(text, max_len=180)

    # 2) reason for credit
    for chunk in chunks:
        reason = _extract_reason_from_chunk(chunk)
        if reason:
            return reason

    # 3) ticket status summary
    for chunk in chunks:
        status = _extract_status_from_chunk(chunk)
        if status:
            return status

    # 4) deterministic fallback
    return "No direct investigation text found."


def _format_days(value: Any) -> str:
    try:
        return f"{float(value):.2f}"
    except Exception:
        return "0.00"


def _format_aging_answer(
    results: list[SearchResult],
    threshold_days: int = 30,
    max_tickets_in_answer: int = 5,
) -> str:
    lines = [f"Tickets exceeding the {threshold_days}-day threshold:"]
    count = 0

    for result in sorted(results, key=lambda r: (-r.score, r.ticket_id, r.chunk_id)):
        if count >= max_tickets_in_answer:
            break

        metadata = result.metadata or {}
        entered_days = metadata.get("entered_to_credited_days")
        investigation_days = metadata.get("investigation_to_credited_days")

        if entered_days is None or investigation_days is None:
            continue

        root_cause_ids = metadata.get("root_cause_ids") or []
        if isinstance(root_cause_ids, list) and root_cause_ids:
            primary_root = str(root_cause_ids[0])
        else:
            primary_root = str(metadata.get("root_cause_primary_id") or "unidentified")

        lines.append(
            f"- {result.ticket_id} — {_format_days(entered_days)} days from entry to credit, "
            f"{_format_days(investigation_days)} days from investigation to credit. "
            f"Primary root cause: {primary_root}."
        )
        count += 1

    if count == 0:
        return f"I couldn't find tickets exceeding the {threshold_days}-day threshold."

    return "\n".join(lines)
