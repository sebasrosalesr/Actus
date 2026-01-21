from __future__ import annotations

import html
import json
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any

import firebase_admin
import numpy as np
from firebase_admin import credentials, db
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
BACKEND_DIR = SCRIPT_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.rag.runtime_env import ensure_openmp_env
ensure_openmp_env()

from app.rag.embeddings import embed_texts
from app.rag.store import get_rag_store


def _load_env() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)


def load_tickets() -> list[dict[str, Any]]:
    """
    Load tickets from Firebase (local dev).
    Returns a list of dicts.
    """
    firebase_json = os.environ.get("ACTUS_FIREBASE_JSON")
    firebase_path = os.environ.get("ACTUS_FIREBASE_PATH")

    if firebase_json:
        firebase_config = json.loads(firebase_json)
    elif firebase_path:
        with open(firebase_path, "r") as handle:
            firebase_config = json.load(handle)
    else:
        raise RuntimeError(
            "Missing Firebase credentials. Set ACTUS_FIREBASE_JSON or ACTUS_FIREBASE_PATH."
        )

    if "private_key" in firebase_config and "\\n" in firebase_config["private_key"]:
        firebase_config["private_key"] = firebase_config["private_key"].replace("\\n", "\n")

    database_url = os.environ.get(
        "ACTUS_FIREBASE_URL", "https://creditapp-tm-default-rtdb.firebaseio.com/"
    )

    if not firebase_admin._apps:
        cred = credentials.Certificate(firebase_config)
        firebase_admin.initialize_app(
            cred,
            {"databaseURL": database_url},
        )

    ref = db.reference("credit_requests")
    raw = ref.get() or {}
    return list(raw.values())

def _normalize_ticket_id(value: Any) -> str | None:
    if not value:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    if text.startswith("R-"):
        return text
    if text.startswith("R") and text[1:].isdigit():
        return "R-" + text[1:]
    return text


def _extract_ticket_id_from_body(body: str) -> str | None:
    body = body or ""
    match = re.search(r"\bcase number:\s*(r-\d{4,6})\b", body, flags=re.IGNORECASE)
    if match:
        return _normalize_ticket_id(match.group(1))
    match = re.search(r"\b(r-\d{4,6})\b", body, flags=re.IGNORECASE)
    if match:
        return _normalize_ticket_id(match.group(1))
    return None


def load_investigation_notes() -> dict[str, list[dict[str, Any]]]:
    ref = db.reference("investigation_notes")
    raw = ref.get() or {}
    notes_by_ticket: dict[str, list[dict[str, Any]]] = {}
    for _, note in raw.items():
        if not isinstance(note, dict):
            continue
        ticket_id = (
            note.get("ticket_number")
            or note.get("ticket_id")
            or note.get("case_number")
        )
        body = note.get("body") or note.get("html") or ""
        ticket_id = _normalize_ticket_id(ticket_id) or _extract_ticket_id_from_body(str(body))
        if not ticket_id:
            continue
        notes_by_ticket.setdefault(ticket_id, []).append(note)
    return notes_by_ticket


def normalize_text(text: str) -> str:
    return " ".join(str(text or "").lower().strip().split())


def _collapse_ws(text: str) -> str:
    return " ".join(str(text or "").split())


def _strip_html(text: str) -> str:
    text = html.unescape(str(text or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    return _collapse_ws(text)


def _cap_text(text: str, limit: int = 2000) -> str:
    text = _collapse_ws(text)
    if len(text) <= limit:
        return text
    return text[:limit].rstrip()


def _extract_discrepancy_section(text: str) -> str:
    """
    Extract only the Discrepancy/Margin Analysis section from a note body.
    Returns a clean, single-paragraph string or empty string if not found.
    """
    clean = _collapse_ws(text)
    if not clean:
        return ""
    pattern = re.compile(
        r"(discrepancy(?:\s*&\s*margin)?\s+analysis|discrepancy\s+analysis)\s*:?\s*(.+?)(?=(?:invoice\s*&\s*item\s+review|price\s+trace\s+review|order\s+history\s+review|price\s+history\s+review|usage\s+review|internal\s+reference|$))",
        flags=re.IGNORECASE,
    )
    match = pattern.search(clean)
    if not match:
        return ""
    section = match.group(2).strip().strip("-:;,.")
    return section


def _format_status_log(entry: Any) -> str:
    if isinstance(entry, dict):
        timestamp = entry.get("timestamp") or entry.get("date") or entry.get("time")
        body = entry.get("text") or entry.get("status") or entry.get("message")
        if timestamp and body:
            return f"{timestamp} - {body}"
        if body:
            return str(body)
        return json.dumps(entry, ensure_ascii=True)
    return str(entry)


def _build_status_fallback(row: dict[str, Any]) -> str | None:
    status = (
        row.get("Status")
        or row.get("status")
        or row.get("Latest Status")
        or row.get("latest_status")
        or row.get("Last Status")
        or row.get("last_status")
    )
    if not status:
        return None
    timestamp = (
        row.get("Status Updated")
        or row.get("status_updated")
        or row.get("Status Date")
        or row.get("status_date")
        or row.get("Updated At")
        or row.get("updated_at")
        or row.get("Last Updated")
        or row.get("last_updated")
    )
    if timestamp:
        return f"{timestamp} - {status}"
    return str(status)


def _format_note(entry: Any) -> str:
    if isinstance(entry, dict):
        body = (
            entry.get("body")
            or entry.get("html")
            or entry.get("text")
            or entry.get("note")
            or entry.get("message")
        )
        body = _strip_html(body)
        discrepancy = _extract_discrepancy_section(body)
        if discrepancy:
            return str(discrepancy)
        return ""
    return str(entry)


def _coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _split_values(value: str) -> list[str]:
    parts = [p.strip() for p in re.split(r"[;,]", str(value)) if p.strip()]
    return parts if parts else [str(value).strip()]


def _extract_item_values(ticket: dict[str, Any]) -> list[str]:
    keys = [
        "Item Number",
        "Item #",
        "Item",
        "Item ID",
        "Item Code",
        "ItemNum",
        "item_number",
        "item",
        "item_id",
        "item_code",
        "sku",
        "product",
    ]
    found: list[str] = []
    for key in keys:
        if key not in ticket:
            continue
        value = ticket.get(key)
        for entry in _coerce_list(value):
            if entry is None:
                continue
            if isinstance(entry, dict):
                for k in ("item", "item_number", "item_id", "sku", "product"):
                    if entry.get(k):
                        found.extend(_split_values(entry.get(k)))
                continue
            text = str(entry).strip()
            if not text:
                continue
            found.extend(_split_values(text))

    cleaned = []
    seen = set()
    for item in found:
        item = str(item).strip()
        if not item:
            continue
        if item in seen:
            continue
        seen.add(item)
        cleaned.append(item)
    return cleaned


PPD_TRIGGERS = [
    "ppd",
    "non-ppd",
    "covered under",
    "contract",
    "non-contract",
    "price trace",
    "effective date",
    "revert pricing",
]

ROOT_CAUSE_RULESET_VERSION = "v1"
ROOT_CAUSE_RULES = [
    {
        "id": "ppd_mismatch",
        "label": "Item should be PPD",
        "priority": 90,
        "threshold": 1,
        "keywords": [
            "ppd",
            "non-ppd",
            "ppd formulary",
            "part of the ppd",
            "should have been sent as ppd",
            "should be ppd",
        ],
        "negative_keywords": ["non ppd confirmed", "not ppd"],
    },
    {
        "id": "sub_price_mismatch",
        "label": "Item not price matched when subbing",
        "priority": 70,
        "threshold": 1,
        "keywords": [
            "sub was not price matched",
            "sub not price matched",
            "not price matched",
            "subbed",
            "subbing",
            "substitute",
            "substitution",
            "price match",
            "should have been matched",
        ],
    },
    {
        "id": "freight_error",
        "label": "Freight should not of been charged",
        "priority": 60,
        "threshold": 1,
        "keywords": [
            "freight",
            "shipping",
            "handling",
            "delivery",
            "should not have been charged",
        ],
    },
    {
        "id": "post_change_invoice",
        "label": "Item invoiced after price change",
        "priority": 50,
        "threshold": 2,
        "keywords": [
            "price change",
            "after price",
            "invoiced after",
            "effective date",
            "updated on",
        ],
    },
    {
        "id": "price_discrepancy",
        "label": "Price discrepancy",
        "priority": 40,
        "threshold": 1,
        "keywords": [
            "wrong price",
            "priced wrong",
            "pricing error",
            "priced incorrectly",
            "incorrect amount",
            "discrepanc",
        ],
        "negative_keywords": [
            "not a pricing discrepancy",
            "not pricing discrepancy",
            "not a price discrepancy",
            "not incorrect pricing",
            "price correctly reflects",
        ],
    },
]


def _detect_root_cause(text: str) -> dict[str, Any]:
    value = normalize_text(text)
    scored: list[dict[str, Any]] = []

    for rule in ROOT_CAUSE_RULES:
        keywords = rule.get("keywords") or []
        negative = rule.get("negative_keywords") or []
        threshold = int(rule.get("threshold") or 1)

        if any(neg in value for neg in negative):
            continue

        triggers = [kw for kw in keywords if kw in value]
        if not triggers:
            continue

        score = len(triggers) / max(1, threshold)
        if score < 1.0:
            continue

        scored.append(
            {
                "id": rule.get("id"),
                "label": rule.get("label"),
                "priority": int(rule.get("priority") or 0),
                "score": score,
                "triggers": triggers,
            }
        )

    if not scored:
        return {
            "root_cause": "UNKNOWN",
            "root_cause_confidence": "low",
            "root_cause_score": 0.0,
            "root_cause_rule_id": None,
            "root_cause_triggers": [],
            "root_cause_ruleset_version": ROOT_CAUSE_RULESET_VERSION,
        }

    scored.sort(key=lambda item: (item["score"], item["priority"]), reverse=True)
    best = scored[0]
    score = float(best["score"])
    if score >= 2.0:
        confidence = "high"
    elif score >= 1.3:
        confidence = "medium"
    else:
        confidence = "low"

    return {
        "root_cause": best["label"],
        "root_cause_confidence": confidence,
        "root_cause_score": score,
        "root_cause_rule_id": best["id"],
        "root_cause_triggers": best["triggers"],
        "root_cause_ruleset_version": ROOT_CAUSE_RULESET_VERSION,
    }


_CR_INVALID = {"", "none", "n/a", "na", "null", "undefined", "duplicate"}


def _extract_credit_numbers(row: dict[str, Any]) -> list[str]:
    values: list[Any] = []
    for key in (
        "RTN_CR_No",
        "RTN",
        "CR Number",
        "credit_number",
        "credit_request_number",
        "cr_number",
        "cr_no",
    ):
        if key in row:
            values.append(row.get(key))

    found: set[str] = set()
    for value in values:
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            items = value
        else:
            items = [value]
        for item in items:
            text = str(item or "").strip()
            if not text:
                continue
            lowered = text.lower()
            if lowered in _CR_INVALID:
                continue
            # Allow comma/space separated lists; extract RTN-like tokens.
            matches = re.findall(r"\brtn[a-z0-9_-]{6,}\b", lowered)
            if matches:
                for m in matches:
                    found.add(m.upper())
                continue
            # Fallback: accept raw token if it looks alphanumeric and long enough.
            if re.match(r"^[a-z0-9_-]{6,}$", lowered):
                found.add(lowered.upper())
    return sorted(found)


def _ppd_match_count(text: str) -> int:
    text = normalize_text(text)
    return sum(1 for t in PPD_TRIGGERS if t in text)


def _reason_is_generic(reason: str | None) -> bool:
    if not reason:
        return True
    value = normalize_text(reason)
    return value in {"", "n/a", "na", "none", "unknown", "other"}


def _format_invoice_value(invoice: Any) -> str | None:
    if invoice is None:
        return None
    if isinstance(invoice, list):
        clean = [str(v).strip() for v in invoice if v]
        if not clean:
            return None
        if len(clean) <= 3:
            return ", ".join(clean)
        return ", ".join(clean[:3]) + f" (+{len(clean) - 3} more)"
    value = str(invoice).strip()
    return value if value else None


def _build_summary(
    ticket_id: str,
    customer: Any,
    invoice: Any,
    item_numbers: list[str],
    total_credit: Any,
    status_recent: list[str],
    note_recent: list[str],
    reason: str | None,
    created_date: Any,
    root_cause: str | None,
) -> str:
    what_parts: list[str] = []
    if reason:
        what_parts.append(str(reason).strip())
    if customer:
        what_parts.append(f"Customer {customer}")
    invoice_value = _format_invoice_value(invoice)
    if invoice_value:
        what_parts.append(f"Invoice {invoice_value}")
    if item_numbers:
        what_parts.append(f"Items {', '.join(item_numbers[:6])}")
    if created_date:
        what_parts.append(f"Created {created_date}")

    evidence_parts: list[str] = []
    if status_recent:
        status_text = _collapse_ws(status_recent[-1])
        if status_text.lower().startswith("reason:"):
            evidence_parts.append(status_text)
        else:
            evidence_parts.append(f"Status: {status_text}")

    impact_parts: list[str] = []
    if total_credit is not None and str(total_credit).strip() != "":
        impact_parts.append(f"Total credit {total_credit}")

    next_parts: list[str] = []

    bullets = []
    if what_parts:
        bullets.append(f"- What happened: {'; '.join(what_parts)}")
    else:
        bullets.append("- What happened:")
    if root_cause:
        bullets.append(f"- Root cause: {root_cause}")
    if evidence_parts:
        bullets.append(f"- Evidence: {' | '.join(evidence_parts)}")
    if impact_parts:
        bullets.append(f"- Impact: {'; '.join(impact_parts)}")
    if next_parts:
        bullets.append(f"- Next action: {'; '.join(next_parts)}")
    return "\n".join(bullets)


def _coerce_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _sort_notes(notes: list[Any]) -> list[Any]:
    def key_fn(value: Any) -> str:
        if isinstance(value, dict):
            return str(
                value.get("created_at")
                or value.get("updated_at")
                or value.get("timestamp")
                or ""
            )
        return ""
    return sorted(notes, key=key_fn)


def build_chunks(
    tickets: list[dict[str, Any]],
    investigation_notes_map: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    chunk_id = 1
    tickets_by_id: dict[str, list[dict[str, Any]]] = {}
    for ticket in tickets:
        ticket_id = ticket.get("Ticket Number") or ticket.get("ticket_id") or ticket.get("Record ID")
        if not ticket_id:
            continue
        tickets_by_id.setdefault(str(ticket_id), []).append(ticket)

    for ticket_id, rows in tickets_by_id.items():
        customer = None
        status = None
        reason = None
        created_date = None
        invoices: list[str] = []
        item_numbers: list[str] = []
        credit_numbers: list[str] = []
        total_credit_sum = 0.0
        status_logs: list[Any] = []
        investigation_notes: list[Any] = []
        status_fallback = None

        for row in rows:
            if customer is None:
                customer = row.get("Customer Number") or row.get("customer")
            if status is None:
                status = row.get("Status") or row.get("status")
            if reason is None:
                reason = row.get("Reason for Credit") or row.get("reason_for_credit")
            if created_date is None:
                created_date = row.get("Date") or row.get("created_date")
            if status_fallback is None:
                status_fallback = _build_status_fallback(row)
            inv = row.get("Invoice Number") or row.get("invoice")
            if inv:
                invoices.append(str(inv).strip())
            credit_numbers.extend(_extract_credit_numbers(row))
            total_credit_sum += _coerce_float(row.get("Credit Request Total") or row.get("total_credit"))
            item_numbers.extend(_extract_item_values(row))
            status_logs.extend(_coerce_list(row.get("status_logs")))
            investigation_notes.extend(_coerce_list(row.get("investigation_notes")))

        extra_notes = investigation_notes_map.get(_normalize_ticket_id(ticket_id) or "", [])
        investigation_notes.extend(extra_notes)

        invoices = list(dict.fromkeys([v for v in invoices if v]))
        item_numbers = list(dict.fromkeys([v for v in item_numbers if v]))
        credit_numbers = list(dict.fromkeys([v for v in credit_numbers if v]))

        status_texts = [_format_status_log(entry) for entry in status_logs]
        note_texts = [
            text
            for text in (_format_note(entry) for entry in _sort_notes(investigation_notes))
            if text
        ]

        last_status = status_texts[-1] if status_texts else (status_fallback or "")
        note_texts_for_events = note_texts[-1:] if note_texts else []

        status_recent = status_texts[-8:] if status_texts else ([status_fallback] if status_fallback else [])
        note_recent = note_texts[-3:]

        clean_notes = [_cap_text(n, 500) for n in note_recent if n]
        root_cause_meta = _detect_root_cause(
            " ".join([str(reason or ""), " ".join(status_recent), " ".join(clean_notes)])
        )
        root_cause = root_cause_meta.get("root_cause")
        root_cause_display = (
            "Needs review" if root_cause == "UNKNOWN" else root_cause
        )
        summary_text = _build_summary(
            ticket_id=ticket_id,
            customer=customer,
            invoice=invoices,
            item_numbers=item_numbers,
            total_credit=round(total_credit_sum, 2) if total_credit_sum else total_credit_sum,
            status_recent=status_recent[-2:],
            note_recent=[],
            reason=reason,
            created_date=created_date,
            root_cause=root_cause_display,
        )

        summary_context = " ".join([summary_text, " ".join(clean_notes), str(reason or "")])
        ppd_matches = _ppd_match_count(summary_context)
        reason_confidence = ppd_matches / max(1, len(PPD_TRIGGERS))
        reason_final = reason
        if ppd_matches >= 2 and reason_confidence >= 0.5:
            if _reason_is_generic(reason) or "ppd" in normalize_text(reason or ""):
                reason_final = "PPD mismatch"
        chunks.append(
            {
                "chunk_id": chunk_id,
                "ticket_id": ticket_id,
                "chunk_type": "ticket_summary",
                "text": summary_text,
                "metadata": {
                    "ticket_id": ticket_id,
                    "customer": customer,
                    "invoice": invoices,
                    "item_numbers": item_numbers,
                    "credit_numbers": credit_numbers,
                    "root_cause": root_cause,
                    "root_cause_confidence": root_cause_meta.get("root_cause_confidence"),
                    "root_cause_score": root_cause_meta.get("root_cause_score"),
                    "root_cause_rule_id": root_cause_meta.get("root_cause_rule_id"),
                    "root_cause_triggers": root_cause_meta.get("root_cause_triggers"),
                    "root_cause_ruleset_version": root_cause_meta.get(
                        "root_cause_ruleset_version"
                    ),
                    "total_credit": round(total_credit_sum, 2) if total_credit_sum else total_credit_sum,
                    "status": status,
                    "reason_for_credit": reason_final,
                    "reason_confidence": reason_confidence,
                },
            }
        )
        chunk_id += 1

        for entry in status_texts:
            text = normalize_text(f"ticket {ticket_id} status: {entry}")
            chunks.append(
                {
                    "chunk_id": chunk_id,
                    "ticket_id": ticket_id,
                    "chunk_type": "ticket_event",
                    "text": text,
                    "metadata": {
                        "ticket_id": ticket_id,
                        "event_type": "status",
                        "reason_for_credit": reason_final,
                        "item_numbers": item_numbers,
                    },
                }
            )
            chunk_id += 1

        for entry in note_texts_for_events:
            text = normalize_text(f"ticket {ticket_id} investigation note: {entry}")
            chunks.append(
                {
                    "chunk_id": chunk_id,
                    "ticket_id": ticket_id,
                    "chunk_type": "ticket_event",
                    "text": text,
                    "metadata": {
                        "ticket_id": ticket_id,
                        "event_type": "note",
                        "reason_for_credit": reason_final,
                        "item_numbers": item_numbers,
                    },
                }
            )
            chunk_id += 1

    return chunks


def main() -> None:
    _load_env()
    tickets = load_tickets()
    investigation_notes_map = load_investigation_notes()
    chunks = build_chunks(tickets, investigation_notes_map)
    if not chunks:
        raise RuntimeError("No chunks produced. Check ticket data source.")

    texts = [chunk["text"] for chunk in chunks]
    embeddings = embed_texts(texts)

    if not isinstance(embeddings, np.ndarray):
        raise ValueError("embed_texts() must return a numpy.ndarray.")

    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    embeddings = embeddings / norms

    store = get_rag_store(
        data_dir=Path(__file__).resolve().parents[1] / "rag_data",
        embedding_dim=embeddings.shape[1],
    )
    try:
        store.reset()
    except Exception:
        pass
    store.upsert_chunks(chunks, embeddings)
    sqlite_path = getattr(store, "sqlite_path", None)
    store.close()

    if sqlite_path:
        # Build ticket_lines table for line-level item numbers.
        conn = sqlite3.connect(sqlite_path)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ticket_lines (
                    ticket_id TEXT,
                    text TEXT
                )
                """
            )
            conn.execute("DELETE FROM ticket_lines")
            rows = []
            for ticket in tickets:
                ticket_id = ticket.get("Ticket Number") or ticket.get("ticket_id") or ticket.get("Record ID")
                if not ticket_id:
                    continue
                item_numbers = _extract_item_values(ticket)
                for item in item_numbers:
                    rows.append((ticket_id, f"item: {item}"))
            if rows:
                conn.executemany("INSERT INTO ticket_lines (ticket_id, text) VALUES (?, ?)", rows)
            conn.commit()
        finally:
            conn.close()
        print(f"Indexed {len(chunks)} chunks into {sqlite_path}.")
    else:
        print(f"Indexed {len(chunks)} chunks into Pinecone.")


if __name__ == "__main__":
    main()
