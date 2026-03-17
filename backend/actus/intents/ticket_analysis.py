import os
import re
import pandas as pd

from app.rag.new_design.service import get_runtime_service
from actus.openrouter_client import openrouter_chat

DEFAULT_OPENROUTER_FALLBACK_MODEL = "google/gemini-3.1-flash-lite-preview"
DEFAULT_OPENROUTER_PRIMARY_MODEL = "openai/gpt-4o-mini"

INTENT_ALIASES = [
    "analyze ticket",
    "ticket analysis",
    "run ticket analysis",
    "analyze this ticket",
]


def _normalize_ticket_id(value: str) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("R-"):
        return text
    if text.startswith("R") and text[1:].isdigit():
        return f"R-{text[1:]}"
    if text.isdigit():
        return f"R-{text}"
    return text


def _extract_ticket_id(query: str) -> str | None:
    text = str(query or "").strip()

    explicit = re.search(r"\bR-?\d{4,7}\b", text, flags=re.IGNORECASE)
    if explicit:
        return _normalize_ticket_id(explicit.group(0))

    from_ticket = re.search(
        r"\bticket(?:\s*(?:id|number|#))?\s*[:\-]?\s*(\d{4,7})\b",
        text,
        flags=re.IGNORECASE,
    )
    if from_ticket:
        return _normalize_ticket_id(from_ticket.group(1))

    return None


def _is_explicit_analyze_query(query: str) -> bool:
    q_low = str(query or "").lower()
    return any(alias in q_low for alias in INTENT_ALIASES)


def _build_suggestions(ticket_id: str) -> list[dict[str, str]]:
    return [
        {
            "id": "ticket_status",
            "label": f"Ticket status ({ticket_id})",
            "prefix": f"ticket status {ticket_id}",
        },
        {
            "id": "investigation_notes",
            "label": f"Investigation notes ({ticket_id})",
            "prefix": f"investigation notes for ticket {ticket_id}",
        },
    ]


def _shorten_line(text: str, max_chars: int = 180) -> str:
    value = " ".join(str(text or "").split()).strip()
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 3].rstrip() + "..."


def _sanitize_highlight_text(text: str) -> str:
    value = str(text or "")
    # Remove keycap emojis like 1️⃣ 2️⃣ 3️⃣
    value = re.sub(r"[0-9#*]\uFE0F?\u20E3", " ", value)
    # Remove common circled-number glyphs
    value = re.sub(r"[\u2460-\u2473\u2776-\u277F\u24EA]", " ", value)
    # Remove plain numbered section prefixes before known headings
    value = re.sub(
        r"\b(?:[1-9]|10)\s+(?=(Order Details|Price Trace (?:Review|Analysis)|Order History (?:Review|Analysis)|Price History (?:Review|Verification)|Finding|Miscellaneous|Usage Review|Background)\b)",
        "",
        value,
        flags=re.IGNORECASE,
    )
    value = " ".join(value.split()).strip()
    return value


def _dedupe_lines(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = " ".join(value.lower().split())
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _extract_section(text: str, heading: str, stop_words: list[str]) -> str | None:
    stops = "|".join(re.escape(w) for w in stop_words)
    pattern = rf"(?:{heading})\s*:?\s*(.+?)\s*(?=(?:{stops})(?:\s*:)?\b|$)"
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    value = " ".join(match.group(1).split()).strip(" -")
    return value or None


def _heuristic_four_bullets(text: str) -> list[str]:
    body = _sanitize_highlight_text(text)
    if not body:
        return []

    bullets: list[str] = []
    section_specs: list[tuple[str, list[str], str]] = [
        (
            r"Finding",
            [
                "Miscellaneous",
                "Usage Review",
                "Background",
                "Price Trace Review",
                "Price Trace Analysis",
            ],
            "Finding",
        ),
        (
            r"Price Trace (?:Review|Analysis)",
            [
                "Order History Review",
                "Order History Analysis",
                "Price History Review",
                "Price History Verification",
                "Finding",
                "Miscellaneous",
            ],
            "Price Trace Review",
        ),
        (
            r"Order History (?:Review|Analysis)",
            [
                "Price History Review",
                "Price History Verification",
                "Finding",
                "Miscellaneous",
                "Usage Review",
            ],
            "Order History Review",
        ),
        (
            r"Price History (?:Review|Verification)",
            ["Finding", "Miscellaneous", "Usage Review", "Background"],
            "Price History Review",
        ),
        (
            r"Usage Review",
            ["Miscellaneous", "Background", "Finding"],
            "Usage Review",
        ),
        (
            r"Background",
            ["Order Details", "Price Trace Review", "Price Trace Analysis", "Order History Review", "Order History Analysis"],
            "Background",
        ),
    ]

    for heading_pattern, stop_words, label in section_specs:
        section = _extract_section(body, heading_pattern, stop_words)
        if not section:
            continue
        bullets.append(_shorten_line(f"{label}: {section}"))
        if len(bullets) >= 4:
            return _dedupe_lines(bullets)[:4]

    # Fallback: split into sentence-like pieces and keep first concise statements.
    pieces = re.split(r"(?<=[\.\!\?])\s+|(?:\s+-\s+)|(?:\s+•\s+)", body)
    for piece in pieces:
        clean = _shorten_line(piece)
        if len(clean) < 20:
            continue
        bullets.append(clean)
        if len(bullets) >= 4:
            break

    return _dedupe_lines(bullets)[:4]


def _normalize_model_bullets(raw: str) -> list[str]:
    text = _sanitize_highlight_text(str(raw or ""))
    if not text:
        return []

    lines: list[str] = []
    for line in text.splitlines():
        value = _sanitize_highlight_text(line.strip())
        if not value:
            continue
        value = re.sub(r"^[-*•\d\.\)\s]+", "", value).strip()
        if value:
            lines.append(_shorten_line(value))

    if not lines:
        lines = _heuristic_four_bullets(text)

    lines = _dedupe_lines(lines)
    return lines[:4]


def _highlight_quality_score(line: str) -> int:
    text = str(line or "").strip()
    low = text.lower()
    if not low:
        return -5

    strong_terms = [
        "finding",
        "price trace review",
        "order history review",
        "price history review",
        "usage review",
    ]
    weak_terms = [
        "invoice prices align",
        "matches invoice",
        "no substitutions",
        "no substitution",
        "no manual overrides",
        "no override",
        "loaded at",
        "updated to",
        "ppd",
        "price sheet",
        "billing",
    ]
    noise_terms = [
        "case file",
        "background:",
        "case number",
        "case title",
        "date opened",
        "status:",
        "invoice numbers",
        "item numbers",
        "notes on background",
    ]

    score = 0
    for term in strong_terms:
        if term in low:
            score += 2
    for term in weak_terms:
        if term in low:
            score += 1
    for term in noise_terms:
        if term in low:
            score -= 1

    if low.startswith("background:") or low.startswith("case file"):
        score -= 2
    if len(text) < 30:
        score -= 1
    return score


def _rank_highlight_bullets(model_lines: list[str], source_text: str) -> list[str]:
    model_lines = _dedupe_lines([_shorten_line(_sanitize_highlight_text(v)) for v in model_lines if v])
    heuristic_lines = _heuristic_four_bullets(source_text)
    candidates = _dedupe_lines(model_lines + heuristic_lines)

    if not candidates:
        return []

    scored: list[tuple[str, int, int, bool]] = []
    model_set = set(model_lines)
    for idx, line in enumerate(candidates):
        score = _highlight_quality_score(line)
        if score <= -2:
            continue
        scored.append((line, score, idx, line in model_set))

    if not scored:
        return heuristic_lines[:4]

    scored.sort(key=lambda row: (-row[1], 0 if row[3] else 1, row[2]))

    selected: list[str] = []
    for line, score, _, _ in scored:
        if score < 1:
            continue
        selected.append(line)
        if len(selected) == 4:
            return selected

    for line, score, _, _ in scored:
        if line in selected or score < 0:
            continue
        selected.append(line)
        if len(selected) == 4:
            return selected

    for line in heuristic_lines:
        if line in selected:
            continue
        selected.append(line)
        if len(selected) == 4:
            return selected

    return selected[:4]


def _looks_unsummarized(lines: list[str]) -> bool:
    if not lines:
        return True
    if len(lines) == 1 and len(lines[0]) > 240:
        return True
    if any("Case File" in line and len(line) > 200 for line in lines):
        return True
    return False


def _resolve_summary_models() -> tuple[str | None, str | None]:
    primary = (
        os.environ.get("ACTUS_OPENROUTER_HIGHLIGHTS_MODEL")
        or os.environ.get("ACTUS_OPENROUTER_SUMMARY_MODEL")
        or os.environ.get("ACTUS_OPENROUTER_MODEL")
    )
    fallback = (
        os.environ.get("ACTUS_OPENROUTER_HIGHLIGHTS_MODEL_FALLBACK")
        or os.environ.get("ACTUS_OPENROUTER_SUMMARY_MODEL_FALLBACK")
        or os.environ.get("ACTUS_OPENROUTER_MODEL_FALLBACK")
        or DEFAULT_OPENROUTER_FALLBACK_MODEL
    )
    if primary and fallback and primary == fallback:
        fallback = None
    return primary, fallback


def _resolve_primary_model_name(model_override: str | None) -> str:
    if model_override:
        return model_override
    return os.environ.get("ACTUS_OPENROUTER_MODEL", "").strip() or DEFAULT_OPENROUTER_PRIMARY_MODEL


def _summarize_highlights_bullets(ticket_id: str, highlights: list[str]) -> tuple[list[str], dict[str, str | None]]:
    summary_meta: dict[str, str | None] = {
        "provider": "heuristic",
        "model": None,
    }
    cleaned = [_sanitize_highlight_text(" ".join(str(h or "").split()).strip()) for h in highlights]
    cleaned = [h for h in cleaned if h]
    if not cleaned:
        return [], summary_meta

    source_text = "\n\n".join(cleaned)
    source_text = source_text[:6000]
    system_prompt = (
        "You summarize investigation highlights for credit operations. "
        "Return exactly 4 concise bullet lines, max 170 chars each. "
        "No intro text, no markdown headings, no extra commentary. "
        "Do not include emojis, keycap numbers, or numbered section markers. "
        "Prioritize findings and evidence (price trace, order history, price history, usage). "
        "Exclude metadata-only background details (case number/title, date opened, status, invoice/item lists)."
    )
    user_prompt = f"Ticket: {ticket_id}\n\nInvestigation content:\n{source_text}"
    primary_model, fallback_model = _resolve_summary_models()
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        raw = openrouter_chat(messages, model=primary_model) if primary_model else openrouter_chat(messages)
        summary_meta["provider"] = "openrouter_primary"
        summary_meta["model"] = _resolve_primary_model_name(primary_model)
    except Exception:
        if not fallback_model:
            return (
                _heuristic_four_bullets(source_text) or [_shorten_line(v) for v in cleaned[:4]],
                summary_meta,
            )
        try:
            raw = openrouter_chat(messages, model=fallback_model)
            summary_meta["provider"] = "openrouter_fallback"
            summary_meta["model"] = fallback_model
        except Exception:
            return (
                _heuristic_four_bullets(source_text) or [_shorten_line(v) for v in cleaned[:4]],
                summary_meta,
            )

    bullets = _rank_highlight_bullets(_normalize_model_bullets(str(raw or "")), source_text)
    if _looks_unsummarized(bullets):
        fallback = _heuristic_four_bullets(source_text)
        if fallback:
            ranked = _rank_highlight_bullets(fallback, source_text)
            if ranked:
                summary_meta["provider"] = "heuristic"
                summary_meta["model"] = None
                return ranked[:4], summary_meta
            summary_meta["provider"] = "heuristic"
            summary_meta["model"] = None
            return fallback[:4], summary_meta

    if not bullets:
        fallback = _heuristic_four_bullets(source_text) or [_shorten_line(v) for v in cleaned[:4]]
        ranked = _rank_highlight_bullets(fallback, source_text)
        summary_meta["provider"] = "heuristic"
        summary_meta["model"] = None
        return (ranked[:4] if ranked else fallback[:4]), summary_meta

    if len(bullets) < 4:
        extra = _heuristic_four_bullets(source_text)
        bullets = _dedupe_lines(bullets + extra)
    return bullets[:4], summary_meta


def intent_ticket_analysis(query: str, df: pd.DataFrame):
    """
    Handle explicit chat requests like:
      - "analyze ticket R-058284"
      - "run ticket analysis for ticket 058284"
    """
    if not _is_explicit_analyze_query(query):
        return None

    ticket_id = _extract_ticket_id(query)
    if not ticket_id:
        return (
            "Please provide a ticket id to analyze, for example: `analyze ticket R-058284`.",
            None,
            {},
        )

    try:
        service = get_runtime_service(refresh=False)
        analysis = service.analyze_ticket(ticket_id=ticket_id, threshold_days=30)
    except Exception as exc:
        return (
            f"I couldn't run ticket analysis for {ticket_id}: {exc}",
            None,
            {},
        )

    analysis_payload = dict(analysis)
    highlights, highlights_meta = _summarize_highlights_bullets(
        ticket_id=ticket_id,
        highlights=list(analysis.get("investigation_highlights") or []),
    )
    analysis_payload["investigation_highlights"] = highlights
    analysis_payload["investigation_highlights_source"] = highlights_meta.get("provider")
    analysis_payload["investigation_highlights_model"] = highlights_meta.get("model")
    text = (
        f"Ticket {ticket_id} analysis generated. "
        "Review the analysis card below and choose a suggested follow-up."
    )

    return (
        text,
        None,
        {
            "show_table": False,
            "ticket_analysis": analysis_payload,
            "suggestions": _build_suggestions(ticket_id),
        },
    )
