import html
import os
import re
from typing import Optional
from urllib.parse import quote

import pandas as pd
from firebase_admin import db

from actus.openrouter_client import openrouter_chat

INTENT_ALIASES = [
    "investigation notes",
    "investigation note",
    "notes for ticket",
]


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


def _summarize_note_body(body: str) -> Optional[list[str]]:
    mode = os.environ.get("ACTUS_INV_NOTE_SUMMARY", "").strip().lower()
    if mode not in {"1", "true", "yes", "always"}:
        return None
    if not body.strip() or body.strip() == "No note body found.":
        return None

    system_prompt = (
        "You summarize internal investigation notes for credit ops. "
        "Return 3-5 concise bullet points. "
        "Do not introduce facts. Capture key decisions or discrepancies. "
        "Respond with bullet lines only, no preamble."
    )
    try:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": body},
        ]
        response = openrouter_chat(messages)
    except Exception:
        fallback_model = os.environ.get("ACTUS_OPENROUTER_MODEL_FALLBACK")
        if not fallback_model:
            return None
        try:
            response = openrouter_chat(messages, model=fallback_model)
        except Exception:
            return None

    bullets: list[str] = []
    for line in response.splitlines():
        cleaned = line.strip()
        if cleaned.startswith(("-", "•")):
            cleaned = cleaned.lstrip("-•").strip()
        if cleaned:
            bullets.append(cleaned)

    bullets = [b[:200] for b in bullets if b]
    if not bullets:
        return None
    return bullets[:5]


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
        summary_bullets = _summarize_note_body(body)
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
            }
        return (
            message,
            None,
            meta,
        )

    preview_cols = [
        "Ticket Number",
        "Combo Key",
        "Invoice Number",
        "Item Number",
        "Title",
        "Updated At",
        "Updated By",
        "Note ID",
    ]
    preview_cols = [col for col in preview_cols if col in notes.columns]
    notes = notes.copy()
    notes["Updated At"] = pd.to_datetime(notes.get("Updated At"), errors="coerce")
    notes["Created At"] = pd.to_datetime(notes.get("Created At"), errors="coerce")
    notes["Sort Time"] = notes["Updated At"].fillna(notes["Created At"])
    notes = notes.sort_values("Sort Time", ascending=False, na_position="last")

    preview = notes[preview_cols].head(10)
    items: list[str] = []
    for _, row in preview.iterrows():
        combo = row.get("Combo Key") or "N/A"
        title = row.get("Title") or "N/A"
        updated = row.get("Updated At") or "N/A"
        note_id_value = row.get("Note ID") or "N/A"
        command = f"Show investigation note for combo {combo}"
        action_token = f"ask:{quote(command)}|Open note"
        items.append(
            f"- **{combo}** — {title} (Updated: {updated}) • Note ID: `{note_id_value}` • `{action_token}`"
        )

    ticket_label = f"ticket **{ticket}**" if ticket else "your request"
    message = (
        f"{mismatch_prefix}Here are the investigation notes for {ticket_label}.\n\n"
        "Available notes:\n"
        + "\n".join(items)
    )
    return (
        message,
        None,
        {
            "show_table": False,
        },
    )
