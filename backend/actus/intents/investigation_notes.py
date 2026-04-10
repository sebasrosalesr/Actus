import json
import html
import os
import re
from typing import Optional, Any
from urllib.parse import quote

import pandas as pd
from firebase_admin import db

from actus.openrouter_client import openrouter_chat

DEFAULT_OPENROUTER_FALLBACK_MODEL = "google/gemini-3.1-flash-lite-preview"
DEFAULT_OPENROUTER_PRIMARY_MODEL = "openai/gpt-4o-mini"

INTENT_ALIASES = [
    "investigation notes",
    "investigation note",
    "notes for ticket",
]


def _note_summary_enabled() -> bool:
    # Default ON so note summaries work out of the box.
    mode = os.environ.get("ACTUS_INV_NOTE_SUMMARY", "true").strip().lower()
    return mode in {"1", "true", "yes", "always", "on"}


def _resolve_note_summary_models() -> tuple[str | None, str | None]:
    primary = (
        os.environ.get("ACTUS_OPENROUTER_SUMMARY_MODEL")
        or os.environ.get("ACTUS_OPENROUTER_MODEL")
    )
    fallback = (
        os.environ.get("ACTUS_OPENROUTER_SUMMARY_MODEL_FALLBACK")
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


def _normalize(text: Optional[str]) -> str:
    if text is None:
        return ""
    return str(text).strip().upper()


def _normalize_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper()


def _extract_ticket(query: str) -> Optional[str]:
    match = re.search(r"\bR-\d{3,}\b", query, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(0).upper()


def _extract_combo_key(query: str) -> Optional[str]:
    match = re.search(r"\b(?:INV)?\d{5,}\|[A-Za-z0-9\-]+\b", query, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(0).upper()


def _extract_note_id(query: str) -> Optional[str]:
    match = re.search(r"note\s*id\s*[:=]?\s*([A-Za-z0-9\-]{6,})", query, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"\b-[A-Za-z0-9]{8,}\b", query)
    if match:
        return match.group(0)
    return None


def _load_investigation_notes() -> pd.DataFrame:
    ref = db.reference("investigation_notes")
    raw = ref.get() or {}
    rows = []
    for key, value in raw.items():
        if not isinstance(value, dict):
            continue
        rows.append(
            {
                "Note ID": value.get("note_id") or key,
                "Firebase Key": key,
                "Ticket Number": value.get("ticket_number"),
                "Combo Key": value.get("combo_key"),
                "Invoice Number": value.get("invoice_number"),
                "Item Number": value.get("item_number"),
                "Title": value.get("title"),
                "Body": value.get("body"),
                "Created At": value.get("created_at"),
                "Created By": value.get("created_by"),
                "Updated At": value.get("updated_at"),
                "Updated By": value.get("updated_by"),
            }
        )
    return pd.DataFrame(rows)


def _clean_note_body(raw: Optional[str]) -> str:
    if raw is None:
        return "No note body found."
    text = html.unescape(str(raw))
    text = text.replace("\u00a0", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n\n", text)
    text = re.sub(r"(?i)</li\s*>", "\n", text)
    text = re.sub(r"(?i)<li[^>]*>", "- ", text)
    text = re.sub(r"(?i)</ul\s*>", "\n", text)
    text = re.sub(r"(?i)<[^>]+>", "", text)
    text = re.sub(r"(?m)^[ \t]+", "", text)
    text = re.sub(r"(?<=\S)\n(?!- )(?=\S)", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip() or "No note body found."


def _compact_whitespace(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_note_lines(text: str, *, max_lines: int = 16) -> str:
    kept: list[str] = []
    current_section = ""
    section_counts: dict[str, int] = {}
    section_map = {
        "background": "background",
        "order details": "order_details",
        "price trace": "price_trace",
        "order history": "order_history",
        "price history": "price_history",
        "miscellaneous": "miscellaneous",
        "usage": "usage",
    }
    for raw_line in str(text or "").splitlines():
        line = _compact_whitespace(raw_line).lstrip("- ").strip()
        if not line:
            continue
        lowered = line.lower().rstrip(":")
        if lowered in section_map:
            current_section = section_map[lowered]
            continue
        if lowered.startswith(("case number:", "case title:", "date opened:", "status:")):
            continue
        if re.fullmatch(r"inv\d{5,}", line, flags=re.IGNORECASE):
            continue
        if re.fullmatch(r"inv\d{5,}\s*\|\s*\d+", line, flags=re.IGNORECASE):
            continue
        if current_section == "background":
            if lowered.startswith("notes on background:"):
                kept.append(f"Background: {line.split(':', 1)[1].strip()}")
            elif lowered.startswith("item number:"):
                kept.append(line)
            continue
        if current_section == "order_details":
            if lowered.startswith(("item:", "unit price:")):
                kept.append(f"Order details: {line}")
                section_counts[current_section] = section_counts.get(current_section, 0) + 1
            continue
        if current_section == "usage":
            continue
        keep_line = False
        if current_section == "price_trace":
            keep_line = any(term in lowered for term in ("currently loaded", "loaded at", "matches invoice price"))
        elif current_section == "order_history":
            keep_line = any(
                term in lowered
                for term in ("there are", "not individually reviewed", "no item substitutions", "no substitutions")
            )
        elif current_section == "price_history":
            keep_line = "$" in line or "updated to" in lowered
        elif current_section == "miscellaneous":
            keep_line = any(
                term in lowered
                for term in (
                    "price discrepancy",
                    "pricing summary",
                    "originally requested",
                    "original requested price",
                    "confirmed the correct price",
                    "correct price",
                    "billed price",
                    "incorrect price",
                    "conflicting pricing trail",
                    "communication error",
                    "approval",
                    "jeff",
                    "should have been matched",
                    "superseded",
                )
            )
        elif any(
            token in lowered
            for token in (
                "correct price",
                "billed price",
                "approval",
                "price discrepancy",
                "substitution",
                "price should have been matched",
            )
        ):
            keep_line = True

        if keep_line:
            label = current_section.replace("_", " ").title() if current_section else "Note"
            if section_counts.get(current_section, 0) < 5:
                kept.append(f"{label}: {line}")
                section_counts[current_section] = section_counts.get(current_section, 0) + 1
        if len(kept) >= max_lines:
            break
    return "\n".join(kept).strip()


def _sorted_notes(notes: pd.DataFrame) -> pd.DataFrame:
    frame = notes.copy()
    frame["Updated At"] = pd.to_datetime(frame.get("Updated At"), errors="coerce")
    frame["Created At"] = pd.to_datetime(frame.get("Created At"), errors="coerce")
    frame["Sort Time"] = frame["Updated At"].fillna(frame["Created At"])
    return frame.sort_values("Sort Time", ascending=False, na_position="last")


def _select_ticket_note_samples(notes: pd.DataFrame, *, max_notes: int = 4) -> list[dict[str, str]]:
    ranked = _sorted_notes(notes)
    samples: list[dict[str, str]] = []
    seen_bodies: set[str] = set()
    for _, row in ranked.iterrows():
        body = _clean_note_body(row.get("Body"))
        normalized_body = _normalize_note_lines(body)
        if not normalized_body or normalized_body == "No note body found.":
            continue
        body_key = normalized_body.lower()
        if body_key in seen_bodies:
            continue
        seen_bodies.add(body_key)
        samples.append(
            {
                "combo_key": str(row.get("Combo Key") or "").strip(),
                "invoice_number": str(row.get("Invoice Number") or "").strip(),
                "item_number": str(row.get("Item Number") or "").strip(),
                "title": str(row.get("Title") or "").strip(),
                "updated_at": str(row.get("Updated At") or row.get("Created At") or "").strip(),
                "body": normalized_body,
            }
        )
        if len(samples) >= max_notes:
            break
    return samples


def _summarize_note_body(body: str) -> tuple[Optional[list[str]], dict[str, str | None]]:
    summary_meta: dict[str, str | None] = {"source": None, "model": None}
    if not _note_summary_enabled():
        return None, summary_meta
    if not body.strip() or body.strip() == "No note body found.":
        return None, summary_meta
    max_chars_raw = os.environ.get("ACTUS_INV_NOTE_SUMMARY_MAX_CHARS", "6000").strip()
    try:
        max_chars = max(1000, int(max_chars_raw))
    except ValueError:
        max_chars = 6000
    summary_body = body if len(body) <= max_chars else body[:max_chars].rstrip() + "\n\n[Truncated for summary]"

    system_prompt = (
        "You summarize internal investigation notes for credit ops. "
        "Return 3-5 concise bullet points. "
        "Do not introduce facts. Capture key decisions or discrepancies. "
        "Respond with bullet lines only, no preamble."
    )
    try:
        primary_model, fallback_model = _resolve_note_summary_models()
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": summary_body},
        ]
        response = openrouter_chat(messages, model=primary_model) if primary_model else openrouter_chat(messages)
        summary_meta["source"] = "openrouter_primary"
        summary_meta["model"] = _resolve_primary_model_name(primary_model)
    except Exception:
        if not fallback_model:
            return None, summary_meta
        try:
            response = openrouter_chat(messages, model=fallback_model)
            summary_meta["source"] = "openrouter_fallback"
            summary_meta["model"] = fallback_model
        except Exception:
            return None, summary_meta

    bullets: list[str] = []
    for line in response.splitlines():
        cleaned = line.strip()
        if cleaned.startswith(("-", "•")):
            cleaned = cleaned.lstrip("-•").strip()
        if cleaned:
            bullets.append(cleaned)

    bullets = [b[:200] for b in bullets if b]
    if not bullets:
        return None, summary_meta
    return bullets[:5], summary_meta


def _summarize_ticket_note_samples(
    ticket: str,
    samples: list[dict[str, str]],
    *,
    total_notes: int,
) -> tuple[Optional[list[str]], dict[str, str | None]]:
    summary_meta: dict[str, str | None] = {"source": None, "model": None}
    if not _note_summary_enabled() or not samples:
        return None, summary_meta

    sample_payload: list[dict[str, str]] = []
    for item in samples:
        sample_payload.append(
            {
                "combo_key": item.get("combo_key") or "N/A",
                "invoice_number": item.get("invoice_number") or "N/A",
                "item_number": item.get("item_number") or "N/A",
                "title": item.get("title") or "N/A",
                "updated_at": item.get("updated_at") or "N/A",
                "body": item.get("body") or "No note body found.",
            }
        )

    system_prompt = (
        "You summarize investigation notes for a single credit ticket. "
        "Return exactly 2 concise bullet lines and nothing else. "
        "The first bullet must start with 'Likely issue:' and state the most likely pricing, substitution, or root-cause conclusion in one sentence. "
        "The second bullet must start with 'Why this is likely:' and explain the note evidence that supports that conclusion in one sentence. "
        "Only use facts present in the provided notes. "
        "Prioritize the actual pricing or root-cause conclusion and the evidence trail behind it. "
        "Ignore note IDs, command tokens, and repetitive invoice listings."
    )
    user_prompt = (
        f"Ticket: {ticket}\n"
        f"Total ticket notes available: {total_notes}\n"
        f"Unique note bodies reviewed: {len(sample_payload)}\n\n"
        f"{json.dumps(sample_payload)}"
    )

    try:
        primary_model, fallback_model = _resolve_note_summary_models()
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        response = openrouter_chat(messages, model=primary_model) if primary_model else openrouter_chat(messages)
        summary_meta["source"] = "openrouter_primary"
        summary_meta["model"] = _resolve_primary_model_name(primary_model)
    except Exception:
        if not fallback_model:
            return None, summary_meta
        try:
            response = openrouter_chat(messages, model=fallback_model)
            summary_meta["source"] = "openrouter_fallback"
            summary_meta["model"] = fallback_model
        except Exception:
            return None, summary_meta

    bullets = _coerce_ticket_summary_bullets(response.splitlines())
    if not bullets:
        return None, summary_meta
    return bullets[:2], summary_meta


def _coerce_ticket_summary_bullets(lines: list[str]) -> list[str]:
    cleaned_lines: list[str] = []
    for raw in lines:
        cleaned = str(raw or "").strip()
        if cleaned.startswith(("-", "•")):
            cleaned = cleaned.lstrip("-•").strip()
        if cleaned:
            cleaned_lines.append(cleaned[:240])

    if not cleaned_lines:
        return []

    likely_issue: str | None = None
    why_likely: str | None = None
    unlabeled: list[str] = []
    for line in cleaned_lines:
        lowered = line.lower()
        if lowered.startswith("likely issue:"):
            likely_issue = f"Likely issue: {line.split(':', 1)[1].strip()}"
        elif lowered.startswith("why this is likely:"):
            why_likely = f"Why this is likely: {line.split(':', 1)[1].strip()}"
        else:
            unlabeled.append(line)

    if likely_issue is None and unlabeled:
        likely_issue = f"Likely issue: {unlabeled.pop(0)}"
    if why_likely is None and unlabeled:
        why_likely = f"Why this is likely: {unlabeled.pop(0)}"

    out = [item for item in [likely_issue, why_likely] if item]
    return out[:2]


def _clean_ticket_evidence_line(value: str) -> str:
    text = str(value or "").strip()
    text = re.sub(r"^(item number|invoice number|title|background)\s*:\s*", "", text, flags=re.IGNORECASE)
    text = " ".join(text.split())
    if not text:
        return ""
    if text.upper() == text:
        text = text.lower()
    text = re.sub(r"\bwas subbed\b", "was substituted", text, flags=re.IGNORECASE)
    text = re.sub(r"\bsubbed\b", "substituted", text, flags=re.IGNORECASE)
    text = re.sub(r"\bpricing not matched\b", "pricing did not match", text, flags=re.IGNORECASE)
    text = re.sub(r"\bprice not matched\b", "price did not match", text, flags=re.IGNORECASE)
    text = re.sub(r"\bnot matched\b", "did not match", text, flags=re.IGNORECASE)
    return text.strip(" .")


def _extract_summary_item_number(samples: list[dict[str, str]], evidence_lines: list[str]) -> str:
    for sample in samples:
        item_number = str(sample.get("item_number") or "").strip()
        if item_number:
            return item_number
    item_line = next((line for line in evidence_lines if line.lower().startswith("item number:")), "")
    if item_line:
        return _clean_ticket_evidence_line(item_line)
    return ""


def _pick_primary_evidence_line(evidence_lines: list[str]) -> str:
    def score(line: str) -> tuple[int, int]:
        lowered = line.lower()
        keywords = [
            "price",
            "pricing",
            "match",
            "mismatch",
            "substituted",
            "subbed",
            "wrong",
            "incorrect",
            "billed",
            "ppd",
            "approval",
        ]
        return (sum(1 for keyword in keywords if keyword in lowered), len(line))

    candidates = [
        line
        for line in evidence_lines
        if _clean_ticket_evidence_line(line)
        and not line.lower().startswith(("item number:", "invoice number:", "title:"))
    ]
    if not candidates:
        return ""
    return max(candidates, key=score)


def _synthesize_ticket_summary_from_samples(
    samples: list[dict[str, str]],
    evidence_lines: list[str],
) -> list[str]:
    primary_line = _pick_primary_evidence_line(evidence_lines)
    cleaned = _clean_ticket_evidence_line(primary_line)
    if not cleaned:
        return []

    item_number = _extract_summary_item_number(samples, evidence_lines)
    likely_issue_text = ""
    substitution_match = re.match(
        r"(?P<sub_item>[A-Z0-9-]+)\s+was substituted(?:\s+and)?\s+pricing did not match",
        cleaned,
        flags=re.IGNORECASE,
    )
    if substitution_match and item_number:
        substituted_item = substitution_match.group("sub_item").upper()
        likely_issue_text = (
            f"item {item_number} appears tied to substituted item {substituted_item}, and the pricing did not match."
        )
    else:
        if item_number and item_number not in cleaned:
            likely_issue_text = f"item {item_number} investigation points to {cleaned}."
        else:
            likely_issue_text = f"{cleaned}."

    evidence_clause = cleaned.rstrip(".")
    if len(samples) > 1:
        why_text = (
            f"the reviewed notes explicitly reference {evidence_clause} across {len(samples)} unique note bodies."
        )
    else:
        why_text = f"the reviewed note explicitly states that {evidence_clause}."

    likely_issue = f"Likely issue: {likely_issue_text[0].upper()}{likely_issue_text[1:]}" if likely_issue_text else ""
    why_likely = f"Why this is likely: {why_text[0].upper()}{why_text[1:]}" if why_text else ""
    return [item for item in [likely_issue, why_likely] if item]


def _is_low_signal_ticket_summary(bullets: list[str]) -> bool:
    low_signal_prefixes = ("item number:", "invoice number:", "title:", "background:")
    for bullet in bullets:
        body = bullet.split(":", 1)[1].strip() if ":" in bullet else bullet.strip()
        if body.lower().startswith(low_signal_prefixes):
            return True
    return False


def _fallback_ticket_note_summary(
    samples: list[dict[str, str]],
) -> list[str]:
    if not samples:
        return []

    primary_body = samples[0].get("body") or ""
    bullets, _ = _summarize_note_body(primary_body)
    if bullets:
        coerced = _coerce_ticket_summary_bullets(bullets)
        if coerced and not _is_low_signal_ticket_summary(coerced):
            return coerced
        synthesized = _synthesize_ticket_summary_from_samples(samples, bullets)
        if synthesized:
            return synthesized
        if coerced:
            return coerced

    lines = [line.strip() for line in primary_body.splitlines() if line.strip()]
    synthesized = _synthesize_ticket_summary_from_samples(samples, lines)
    if synthesized:
        return synthesized
    out: list[str] = []
    for line in lines:
        if line.lower() in {"background:", "order details", "price trace", "order history", "price history", "miscellaneous", "usage"}:
            continue
        out.append(line[:200])
        if len(out) >= 2:
            break
    return _coerce_ticket_summary_bullets(out)


def intent_investigation_notes(query: str, df: pd.DataFrame):
    q_low = query.lower()
    if "investigation" not in q_low or "note" not in q_low:
        return None

    ticket = _extract_ticket(query)
    combo_key = _extract_combo_key(query)
    note_id = _extract_note_id(query)

    if not ticket and not combo_key and not note_id:
        return (
            "Which ticket should I pull investigation notes for? "
            "You can share a ticket number (e.g., R-053805) or a combo key.",
            None,
            {"follow_up": {"intent": "investigation_notes", "prefix": "investigation notes"}},
        )

    notes = _load_investigation_notes()
    if notes.empty:
        return "I couldn't find any investigation notes in Firebase."

    if ticket:
        notes = notes[_normalize_series(notes["Ticket Number"]) == ticket]

    notes_by_combo = None
    if combo_key:
        notes_by_combo = notes[_normalize_series(notes["Combo Key"]) == _normalize(combo_key)]
        notes = notes_by_combo

    note_id_mismatch = False
    if note_id:
        note_id_norm = _normalize(note_id)
        notes_by_id = notes[
            (_normalize_series(notes["Note ID"]) == note_id_norm)
            | (_normalize_series(notes["Firebase Key"]) == note_id_norm)
        ]
        if notes_by_id.empty and notes_by_combo is not None:
            note_id_mismatch = True
        else:
            notes = notes_by_id

    if notes.empty:
        parts = []
        if ticket:
            parts.append(f"ticket {ticket}")
        if combo_key:
            parts.append(f"combo {combo_key}")
        if note_id:
            parts.append(f"note id {note_id}")
        label = " / ".join(parts) if parts else "your request"
        return f"No investigation notes found for {label}."

    notes = notes.drop_duplicates(subset=["Note ID"])

    mismatch_prefix = ""
    if note_id_mismatch:
        mismatch_prefix = (
            f"Note ID `{note_id}` did not match any notes for combo **{combo_key}**. "
            "Showing notes for the combo instead.\n\n"
        )

    if note_id or combo_key:
        note = notes.iloc[0]
        body = _clean_note_body(note.get("Body"))
        summary_bullets, summary_meta = _summarize_note_body(body)
        message = (
            f"{mismatch_prefix}### Investigation note\n"
            f"- Ticket: **{note.get('Ticket Number') or 'N/A'}**\n"
            f"- Combo key: **{note.get('Combo Key') or 'N/A'}**\n"
            f"- Title: **{note.get('Title') or 'N/A'}**\n\n"
            f"**Note:**\n{body}"
        )
        meta = {"show_table": False}
        if summary_bullets:
            meta["note_summary"] = {
                "bullets": summary_bullets,
                "disclaimer": "Generated by LLM",
                "source": summary_meta.get("source"),
                "model": summary_meta.get("model"),
            }
        return (
            message,
            None,
            meta,
        )

    sorted_notes = _sorted_notes(notes)
    samples = _select_ticket_note_samples(sorted_notes)
    summary_bullets, summary_meta = _summarize_ticket_note_samples(
        ticket or "N/A",
        samples,
        total_notes=len(sorted_notes.index),
    )
    if not summary_bullets:
        summary_bullets = _fallback_ticket_note_summary(samples)

    message_lines: list[str] = []
    if mismatch_prefix:
        message_lines.append(mismatch_prefix.rstrip())
        message_lines.append("")
    if not summary_bullets:
        message_lines.append(f"Here are the investigation notes for ticket **{ticket or 'N/A'}**.")
        message_lines.append("")

    unique_count = len(samples)
    total_count = len(sorted_notes.index)
    message_lines.append(
        f"Reviewed **{unique_count}** unique note body/bodies across **{total_count}** ticket note(s)."
    )
    if samples:
        message_lines.append("")
        message_lines.append("Relevant notes reviewed:")
        for item in samples[:4]:
            combo = item.get("combo_key") or "N/A"
            command = f"Show investigation note for combo {combo}"
            action_token = f"ask:{quote(command)}|Open note"
            message_lines.append(f"- **{combo}** • `{action_token}`")
        remaining = max(total_count - len(samples[:4]), 0)
        if remaining:
            message_lines.append(f"- Plus **{remaining}** additional related note(s).")

    meta: dict[str, Any] = {"show_table": False}
    if summary_bullets:
        meta["note_summary"] = {
            "bullets": summary_bullets[:2],
            "disclaimer": "Generated by LLM" if summary_meta.get("source") else "Generated from note evidence",
            "source": summary_meta.get("source"),
            "model": summary_meta.get("model"),
            "ticket_level": True,
            "reviewed_note_bodies": unique_count,
            "total_ticket_notes": total_count,
        }

    return (
        "\n".join(message_lines).strip(),
        None,
        meta,
    )
