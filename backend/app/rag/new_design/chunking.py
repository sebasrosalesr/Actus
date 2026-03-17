from __future__ import annotations

import html
import re

from .models import InvestigationChunk, InvestigationNote


DEFAULT_SECTION_HEADERS = [
    "Order Details",
    "Price Trace",
    "Order History",
    "Price History",
    "Miscellaneous",
    "Usage",
    "Margin Review",
    "Margin Review (contextual)",
]


def clean_investigation_html(text: str | None) -> str:
    value = html.unescape(str(text or ""))
    if not value:
        return ""

    # Preserve list items as line-level bullets before stripping tags.
    value = re.sub(r"<\s*li\b[^>]*>", "\n- ", value, flags=re.IGNORECASE)
    value = re.sub(r"<\s*/\s*li\s*>", "\n", value, flags=re.IGNORECASE)
    value = re.sub(r"<\s*br\s*/?\s*>", "\n", value, flags=re.IGNORECASE)
    value = re.sub(r"<\s*/\s*(p|div|ul|ol|tr|section|h\d)\s*>", "\n", value, flags=re.IGNORECASE)
    value = re.sub(r"<[^>]+>", " ", value)

    value = value.replace("\r", "\n")
    value = re.sub(r"\n\s*\n+", "\n\n", value)
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r" *\n *", "\n", value)
    return value.strip()


def split_investigation_sections(clean_text: str, section_headers: list[str] | None = None) -> list[dict[str, str]]:
    headers = section_headers or DEFAULT_SECTION_HEADERS
    normalized_map = {h.lower(): h for h in headers}

    sections: list[dict[str, str]] = []
    current_section: str | None = None
    current_lines: list[str] = []

    for raw in str(clean_text or "").split("\n"):
        line = raw.strip()
        if not line:
            continue

        matched = normalized_map.get(line.lower())
        if matched:
            if current_section and current_lines:
                sections.append(
                    {
                        "section_name": current_section,
                        "section_text": "\n".join(current_lines).strip(),
                    }
                )
            current_section = matched
            current_lines = []
            continue

        current_lines.append(line)

    if current_section and current_lines:
        sections.append(
            {
                "section_name": current_section,
                "section_text": "\n".join(current_lines).strip(),
            }
        )

    return sections


def split_fallback_body(clean_text: str, max_chars: int = 700) -> list[str]:
    text = str(clean_text or "").strip()
    if not text:
        return []

    paragraphs = [p.strip() for p in re.split(r"\n\s*\n+", text) if p.strip()]
    if len(paragraphs) > 1:
        parts: list[str] = []
        current = ""
        for p in paragraphs:
            if len(current) + len(p) + 2 <= max_chars:
                current = f"{current}\n\n{p}".strip()
            else:
                if current:
                    parts.append(current)
                current = p
        if current:
            parts.append(current)
        return parts

    sentences = re.split(r"(?<=[.!?])\s+", text)
    parts = []
    current = ""
    for sentence in sentences:
        value = sentence.strip()
        if not value:
            continue
        if len(current) + len(value) + 1 <= max_chars:
            current = f"{current} {value}".strip()
        else:
            if current:
                parts.append(current)
            current = value
    if current:
        parts.append(current)
    return parts


def build_investigation_section_chunks(
    note: InvestigationNote,
    fallback_max_chars: int = 700,
    section_headers: list[str] | None = None,
) -> list[InvestigationChunk]:
    sections = split_investigation_sections(note.body_clean, section_headers=section_headers)
    chunks: list[InvestigationChunk] = []

    if sections:
        for i, sec in enumerate(sections, start=1):
            chunks.append(
                InvestigationChunk(
                    chunk_id=f"{note.ticket_id}::{note.note_id}::{i}",
                    chunk_index=i,
                    chunk_type="ticket_investigation_section",
                    section_name=sec["section_name"],
                    chunk_text=sec["section_text"],
                    ticket_id=note.ticket_id,
                    invoice_number=note.invoice_number,
                    item_number=note.item_number,
                    combo_key=note.combo_key,
                    note_id=note.note_id,
                    title=note.title,
                    source_node=note.source_node,
                    created_at=note.created_at,
                )
            )
        return chunks

    fallback = split_fallback_body(note.body_clean, max_chars=fallback_max_chars)
    for i, part in enumerate(fallback, start=1):
        chunks.append(
            InvestigationChunk(
                chunk_id=f"{note.ticket_id}::{note.note_id}::{i}",
                chunk_index=i,
                chunk_type="ticket_investigation_section",
                section_name="Full Note",
                chunk_text=part,
                ticket_id=note.ticket_id,
                invoice_number=note.invoice_number,
                item_number=note.item_number,
                combo_key=note.combo_key,
                note_id=note.note_id,
                title=note.title,
                source_node=note.source_node,
                created_at=note.created_at,
            )
        )

    return chunks
