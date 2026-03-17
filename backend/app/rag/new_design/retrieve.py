from __future__ import annotations

from datetime import datetime
import re
from typing import Any

import numpy as np

from .models import QueryInfo, RetrievalChunk, SearchResult

AGING_QUERY_PATTERNS = [
    "more than 30 days",
    "over 30 days",
    "older than 30 days",
    "tickets older than",
    "took more than",
    "aging tickets",
]

PHRASE_BOOST_TERMS = [
    "loaded incorrectly",
    "wrong price",
    "priced wrong",
    "incorrect price",
    "price loaded",
]

ROOT_CAUSE_QUERY_TERMS: dict[str, list[str]] = {
    "ppd_mismatch": ["ppd", "non-ppd", "ppd mismatch"],
    "sub_price_mismatch": ["subbed", "substitute", "substitution", "price matched", "not price matched"],
    "freight_error": ["freight", "shipping", "handling", "delivery"],
    "price_discrepancy": ["pricing", "wrong price", "pricing error", "discrepancy"],
    "price_loaded_after_invoice": [
        "price loaded after invoice",
        "loaded after invoice",
        "price updated after invoice",
        "price changed after invoice",
        "price loaded after order",
        "loaded after order",
        "after invoice date",
        "after order date",
        "billed before updated pricing",
        "order placed before price loaded",
        "at the time the order was placed",
    ],
}

ROOT_CAUSE_ROUTE_ALIASES: dict[str, set[str]] = {
    "price_loaded_after_invoice": {"price_loaded_after_invoice", "post_change_invoice"},
}


def analyze_query(query: str) -> QueryInfo:
    q = str(query or "").strip()
    q_lower = q.lower()

    ticket_ids = [x.upper() for x in re.findall(r"\bR-\d+\b", q, flags=re.IGNORECASE)]
    invoice_ids = [x.upper() for x in re.findall(r"\bINV[-]?\d+[A-Z0-9-]*\b", q, flags=re.IGNORECASE)]

    tokens = re.findall(r"[A-Za-z0-9_-]+", q)
    item_candidates: list[str] = []
    for token in tokens:
        value = token.strip().upper()
        if re.match(r"^R-\d+$", value):
            continue
        if re.match(r"^INV[-]?\d+[A-Z0-9-]*$", value):
            continue
        if any(ch.isdigit() for ch in value) and len(value) >= 5 and (
            "-" in value or (re.search(r"[A-Z]", value) and re.search(r"\d", value))
        ):
            item_candidates.append(value)

    item_candidates = list(dict.fromkeys(item_candidates))

    if _is_aging_query(q):
        intent = "aging_lookup"
    elif ticket_ids:
        intent = "ticket_lookup"
    elif invoice_ids:
        intent = "invoice_lookup"
    elif item_candidates:
        intent = "item_lookup"
    else:
        intent = "semantic_lookup"

    return QueryInfo(
        raw_query=q,
        intent=intent,
        ticket_ids=ticket_ids,
        invoice_ids=invoice_ids,
        item_candidates=item_candidates,
        is_status_lookup=any(x in q_lower for x in ["status", "update", "resolved", "closed", "timeline"]),
        is_reason_lookup=any(x in q_lower for x in ["why", "reason", "investigation", "evidence"]),
    )


def _extract_query_tokens(query: str) -> set[str]:
    return set(re.findall(r"[A-Za-z0-9_-]+", str(query).upper()))


def _is_aging_query(query: str) -> bool:
    q = str(query or "").lower()
    return any(pattern in q for pattern in AGING_QUERY_PATTERNS)


def _to_dict(obj: Any) -> dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "__dict__"):
        return dict(obj.__dict__)
    result: dict[str, Any] = {}
    for name in dir(obj):
        if name.startswith("_"):
            continue
        try:
            result[name] = getattr(obj, name)
        except Exception:
            continue
    return result


def _parse_status_events(status_text: str) -> list[dict[str, Any]]:
    if not status_text or str(status_text).lower() == "nan":
        return []

    text = str(status_text)
    pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*\n?(.*?)(?=(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})|$)"
    matches = re.findall(pattern, text, flags=re.DOTALL)
    events: list[dict[str, Any]] = []
    closure_patterns = [
        r"\bupdated by the system\b",
        r"\bcredit number sent\b",
        r"\bcredit number verified\b",
        r"\bcr number verified\b",
        r"\bcredit processing completed\b",
        r"\bticket (?:is )?resolved\b",
        r"\bresolved and will be closed\b",
        r"\bautomatically closed\b",
        r"\bauto(?:matically)? closed\b",
        r"\bclosed by the system\b",
    ]

    for ts, body, _ in matches:
        raw = " ".join(body.split()).strip()
        body_clean = raw.lower()
        event_type = "other"
        if "open:" in body_clean or "not started" in body_clean:
            event_type = "entered"
        elif "on macro" in body_clean or "went through investigation" in body_clean:
            event_type = "investigation"
        elif any(re.search(p, body_clean) for p in closure_patterns):
            event_type = "credited"

        try:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        events.append({"timestamp": ts, "dt": dt, "event_type": event_type})

    return events


def _compute_aging_from_status_list(status_raw_list: list[str]) -> tuple[float | None, float | None]:
    events: list[dict[str, Any]] = []
    for status in status_raw_list or []:
        events.extend(_parse_status_events(status))

    if not events:
        return None, None

    events.sort(key=lambda item: item["dt"])
    entered = next((e for e in events if e["event_type"] == "entered"), None)
    investigation = next((e for e in events if e["event_type"] == "investigation"), None)
    credited = next((e for e in events if e["event_type"] == "credited"), None)

    if entered is None and events:
        # Prefer a non-terminal first event; if everything is terminal, keep None.
        entered = next((e for e in events if e["event_type"] != "credited"), None)

    entered_to_credited_days = None
    investigation_to_credited_days = None

    if entered and credited:
        entered_to_credited_days = round((credited["dt"] - entered["dt"]).total_seconds() / 86400, 2)
    if investigation and credited:
        investigation_to_credited_days = round((credited["dt"] - investigation["dt"]).total_seconds() / 86400, 2)

    return entered_to_credited_days, investigation_to_credited_days


def _extract_ticket_aging(ticket: Any) -> tuple[float | None, float | None]:
    data = _to_dict(ticket)

    entered = data.get("entered_to_credited_days")
    investigation = data.get("investigation_to_credited_days")
    if entered is not None or investigation is not None:
        return entered, investigation

    timeline = data.get("timeline_metrics")
    if isinstance(timeline, dict):
        return timeline.get("entered_to_credited_days"), timeline.get("investigation_to_credited_days")

    status_raw_list = data.get("status_raw_list")
    if isinstance(status_raw_list, list):
        return _compute_aging_from_status_list(status_raw_list)

    return None, None


def _extract_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value if v is not None]
    if value is None:
        return []
    return [str(value)]


def _extract_root_cause_ids_from_metadata(metadata: dict[str, Any]) -> list[str]:
    values = metadata.get("root_cause_ids") or []
    return [str(v).lower() for v in _extract_list(values)]


def _expand_root_cause_targets(targets: set[str]) -> set[str]:
    expanded = set(targets)
    for target in list(targets):
        expanded.update(ROOT_CAUSE_ROUTE_ALIASES.get(target, set()))
    return expanded


def _extract_root_cause_route_targets(query: str) -> set[str]:
    q = str(query or "").lower()
    targets: set[str] = set()
    for root_id, terms in ROOT_CAUSE_QUERY_TERMS.items():
        if any(term in q for term in terms):
            targets.add(root_id)
    return _expand_root_cause_targets(targets)


def _chunk_matches_root_cause_targets(chunk: RetrievalChunk, targets: set[str]) -> bool:
    metadata = chunk.metadata or {}
    root_ids = set(_extract_root_cause_ids_from_metadata(metadata))
    primary = str(metadata.get("root_cause_primary_id") or "").lower().strip()
    if primary:
        root_ids.add(primary)
    return bool(root_ids & targets)


def _resolve_runtime_canonical_tickets() -> dict[str, Any] | None:
    try:
        from . import service as service_module

        runtime_service = getattr(service_module, "_RUNTIME_SERVICE", None)
        if runtime_service is None:
            return None
        artifacts = getattr(runtime_service, "_artifacts", None)
        if artifacts is None:
            return None
        canonical_tickets = getattr(artifacts, "canonical_tickets", None)
        return canonical_tickets if isinstance(canonical_tickets, dict) else None
    except Exception:
        return None


def retrieve_aging_tickets(artifacts_or_tickets: Any, threshold_days: int = 30, top_k: int = 10) -> list[dict[str, Any]]:
    if isinstance(artifacts_or_tickets, dict):
        canonical_tickets = artifacts_or_tickets
    else:
        canonical_tickets = getattr(artifacts_or_tickets, "canonical_tickets", None)
    if not isinstance(canonical_tickets, dict):
        canonical_tickets = _resolve_runtime_canonical_tickets()
    if not isinstance(canonical_tickets, dict):
        return []

    aged: list[dict[str, Any]] = []
    for ticket_id, ticket in canonical_tickets.items():
        entered_days, investigation_days = _extract_ticket_aging(ticket)
        if entered_days is None or investigation_days is None:
            continue

        over_threshold = (
            entered_days > threshold_days
            or investigation_days > threshold_days
        )
        if not over_threshold:
            continue

        age_value = max(
            entered_days,
            investigation_days,
        )

        ticket_dict = _to_dict(ticket)
        root_cause_ids = _extract_list(
            ticket_dict.get("root_cause_ids")
            or ticket_dict.get("root_cause_labels")
            or ticket_dict.get("root_causes_all")
        )
        item_numbers = _extract_list(ticket_dict.get("item_numbers"))
        invoice_numbers = _extract_list(ticket_dict.get("invoice_numbers"))

        aged.append(
            {
                "ticket_id": str(ticket_id),
                "entered_to_credited_days": entered_days,
                "investigation_to_credited_days": investigation_days,
                "aging_days": float(age_value),
                "root_cause_ids": root_cause_ids,
                "item_numbers": item_numbers,
                "invoice_numbers": invoice_numbers,
            }
        )

    aged.sort(key=lambda item: (-item["aging_days"], item["ticket_id"]))
    return aged[:top_k]


def compute_exact_match_boost(query: str, chunk: RetrievalChunk) -> float:
    query_tokens = _extract_query_tokens(query)
    metadata = chunk.metadata or {}
    boost = 0.0

    ticket_id = str(metadata.get("ticket_id") or chunk.ticket_id or "").upper()
    if ticket_id and ticket_id in query_tokens:
        boost += 0.70

    invoice_number = str(metadata.get("invoice_number") or "").upper()
    if invoice_number and invoice_number in query_tokens:
        boost += 0.60

    item_number = str(metadata.get("item_number") or "").upper()
    if item_number and item_number in query_tokens:
        boost += 0.55

    combo_key = str(metadata.get("combo_key") or "").upper()
    if combo_key and combo_key in str(query).upper():
        boost += 0.70

    return boost


def compute_chunk_type_boost(query: str, chunk_type: str) -> float:
    q = str(query).lower()
    boost = 0.0

    if ("show me ticket" in q or "find ticket" in q or "ticket " in q) and chunk_type == "ticket_summary":
        boost += 0.30
    if ("line" in q or "invoice" in q or "item" in q) and chunk_type == "ticket_line_summary":
        boost += 0.20
    if (
        "why" in q
        or "investigation" in q
        or "reason" in q
        or "evidence" in q
    ) and chunk_type == "ticket_investigation_section":
        boost += 0.20

    return boost


def compute_root_cause_boost(query: str, metadata: dict[str, Any]) -> float:
    q = str(query).lower()
    root_cause_ids = _extract_root_cause_ids_from_metadata(metadata)
    primary_root = str(metadata.get("root_cause_primary_id", "")).lower()

    boost = 0.0

    if any(term in q for term in ROOT_CAUSE_QUERY_TERMS["ppd_mismatch"]):
        if "ppd_mismatch" in root_cause_ids:
            boost += 0.25
        if primary_root == "ppd_mismatch":
            boost += 0.10

    if any(term in q for term in ROOT_CAUSE_QUERY_TERMS["sub_price_mismatch"]):
        if "sub_price_mismatch" in root_cause_ids:
            boost += 0.25
        if primary_root == "sub_price_mismatch":
            boost += 0.10

    if any(term in q for term in ROOT_CAUSE_QUERY_TERMS["freight_error"]):
        if "freight_error" in root_cause_ids:
            boost += 0.25
        if primary_root == "freight_error":
            boost += 0.10

    if any(term in q for term in ROOT_CAUSE_QUERY_TERMS["price_discrepancy"]):
        if "price_discrepancy" in root_cause_ids:
            boost += 0.20
        if primary_root == "price_discrepancy":
            boost += 0.10

    target_timing_roots = ROOT_CAUSE_ROUTE_ALIASES["price_loaded_after_invoice"]
    if any(term in q for term in ROOT_CAUSE_QUERY_TERMS["price_loaded_after_invoice"]):
        # For timing-centric queries, prioritize tickets where this is the primary root cause.
        if primary_root in target_timing_roots:
            boost += 0.35
        elif any(root in root_cause_ids for root in target_timing_roots):
            boost += 0.10

    return boost


def compute_phrase_boost(query: str, chunk_text: str) -> float:
    q = str(query or "").lower()
    if not any(term in q for term in PHRASE_BOOST_TERMS):
        return 0.0

    text = str(chunk_text or "").lower()
    if any(term in text for term in PHRASE_BOOST_TERMS):
        return 2.0

    return 0.0


def route_candidate_chunks(query_info: QueryInfo, chunks: list[RetrievalChunk]) -> tuple[list[tuple[int, RetrievalChunk]], bool]:
    indexed = list(enumerate(chunks))
    filtered = indexed

    if query_info.ticket_ids:
        wanted = set(query_info.ticket_ids)
        filtered = [
            (idx, c)
            for idx, c in filtered
            if str((c.metadata or {}).get("ticket_id") or c.ticket_id or "").upper() in wanted
        ]

    if query_info.invoice_ids:
        wanted = set(query_info.invoice_ids)
        filtered = [
            (idx, c)
            for idx, c in filtered
            if str((c.metadata or {}).get("invoice_number") or "").upper() in wanted
            or any(str(v).upper() in wanted for v in (c.metadata or {}).get("invoice_numbers", []))
        ]

    if query_info.item_candidates and query_info.intent == "item_lookup":
        wanted = set(query_info.item_candidates)
        filtered = [
            (idx, c)
            for idx, c in filtered
            if str((c.metadata or {}).get("item_number") or "").upper() in wanted
            or any(str(v).upper() in wanted for v in (c.metadata or {}).get("item_numbers", []))
        ]

    if query_info.intent == "semantic_lookup":
        target_roots = _extract_root_cause_route_targets(query_info.raw_query)
        if target_roots:
            routed = [(idx, c) for idx, c in filtered if _chunk_matches_root_cause_targets(c, target_roots)]
            if routed:
                filtered = routed

    is_exact_intent = query_info.intent in {"ticket_lookup", "invoice_lookup", "item_lookup"}
    exact_not_found = is_exact_intent and not filtered

    if not filtered and not is_exact_intent:
        filtered = indexed

    return filtered, exact_not_found


def rerank_diversity(
    results: list[SearchResult],
    query_intent: str,
    max_chunks_per_ticket: int = 2,
    final_top_k: int = 5,
) -> list[SearchResult]:
    if query_intent != "semantic_lookup":
        return results[:final_top_k]

    selected: list[SearchResult] = []
    ticket_counts: dict[str, int] = {}

    for result in results:
        count = ticket_counts.get(result.ticket_id, 0)
        if count >= max_chunks_per_ticket:
            continue

        selected.append(result)
        ticket_counts[result.ticket_id] = count + 1
        if len(selected) >= final_top_k:
            break

    return selected


def routed_hybrid_search_real(
    query: str,
    chunks: list[RetrievalChunk],
    embeddings: np.ndarray,
    top_k: int = 5,
    initial_search_multiplier: int = 5,
    query_embedding: np.ndarray | None = None,
    canonical_tickets: dict[str, Any] | None = None,
) -> dict[str, Any]:
    query_info = analyze_query(query)

    if query_info.intent == "aging_lookup":
        aged = retrieve_aging_tickets(canonical_tickets, threshold_days=30, top_k=top_k)

        chunk_by_ticket: dict[str, RetrievalChunk] = {}
        for chunk in chunks:
            if chunk.ticket_id not in chunk_by_ticket and chunk.chunk_type == "ticket_summary":
                chunk_by_ticket[chunk.ticket_id] = chunk

        results: list[SearchResult] = []
        for row in aged:
            ticket_id = row["ticket_id"]
            chunk = chunk_by_ticket.get(ticket_id)
            if chunk is None:
                continue

            metadata = dict(chunk.metadata or {})
            metadata["entered_to_credited_days"] = row["entered_to_credited_days"]
            metadata["investigation_to_credited_days"] = row["investigation_to_credited_days"]
            metadata["aging_days"] = row["aging_days"]
            metadata["root_cause_ids"] = row["root_cause_ids"]
            metadata["item_numbers"] = row["item_numbers"]
            metadata["invoice_numbers"] = row["invoice_numbers"]

            results.append(
                SearchResult(
                    score=float(row["aging_days"]),
                    semantic_score=0.0,
                    exact_boost=0.0,
                    type_boost=0.0,
                    root_cause_boost=0.0,
                    chunk_type="ticket_summary",
                    ticket_id=ticket_id,
                    text=chunk.text,
                    metadata=metadata,
                    intent="aging_lookup",
                    chunk_id=chunk.chunk_id,
                )
            )

        return {"results": results, "query_info": query_info, "not_found": False, "intent": "aging_lookup"}

    candidates, exact_not_found = route_candidate_chunks(query_info, chunks)

    if exact_not_found:
        return {"results": [], "query_info": query_info, "not_found": True, "intent": query_info.intent}

    if not candidates:
        return {"results": [], "query_info": query_info, "not_found": False, "intent": query_info.intent}

    if query_embedding is None:
        from app.rag.embeddings import embed_texts

        query_embedding_vec = embed_texts([query])[0]
    else:
        query_embedding_vec = query_embedding
    filtered_indices = [idx for idx, _ in candidates]
    filtered_chunks = [chunk for _, chunk in candidates]

    semantic_scores = np.dot(embeddings[filtered_indices], query_embedding_vec)

    scored: list[SearchResult] = []
    for i, chunk in enumerate(filtered_chunks):
        semantic = float(semantic_scores[i])
        exact = compute_exact_match_boost(query, chunk)
        type_boost = compute_chunk_type_boost(query, chunk.chunk_type)
        phrase_boost = compute_phrase_boost(query, chunk.text)
        root_boost = compute_root_cause_boost(query, chunk.metadata or {})
        total_type_boost = type_boost + phrase_boost
        final = semantic + exact + total_type_boost + root_boost

        scored.append(
            SearchResult(
                score=final,
                semantic_score=semantic,
                exact_boost=exact,
                type_boost=total_type_boost,
                root_cause_boost=root_boost,
                chunk_type=chunk.chunk_type,
                ticket_id=chunk.ticket_id,
                text=chunk.text,
                metadata=chunk.metadata or {},
                intent=query_info.intent,
                chunk_id=chunk.chunk_id,
            )
        )

    scored.sort(key=lambda r: (-r.score, r.ticket_id, r.chunk_id))

    initial_k = top_k * initial_search_multiplier if query_info.intent == "semantic_lookup" else top_k
    scored = scored[:initial_k]
    final = rerank_diversity(scored, query_info.intent, max_chunks_per_ticket=2, final_top_k=top_k)

    return {"results": final, "query_info": query_info, "not_found": False, "intent": query_info.intent}


def search(query: str, artifacts, embeddings: np.ndarray, top_k: int = 5) -> dict[str, Any]:
    canonical_tickets = getattr(artifacts, "canonical_tickets", None)
    return routed_hybrid_search_real(
        query=query,
        chunks=artifacts.chunks,
        embeddings=embeddings,
        top_k=top_k,
        canonical_tickets=canonical_tickets,
    )
