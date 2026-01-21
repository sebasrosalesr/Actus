from __future__ import annotations

import re
from typing import Any, Optional


def normalize_text(text: str) -> str:
    return " ".join(str(text or "").lower().strip().split())


def normalize_chunk_type(row: dict[str, Any]) -> str:
    meta = row.get("metadata") or {}
    event_type = (meta.get("event_type") or "").lower().strip()
    raw = (row.get("chunk_type") or "").lower().strip()

    if raw in ("ticket_summary", "summary"):
        return "summary"
    if event_type in ("status", "note"):
        return event_type
    return "event"


def extract_invoice_ids(text: str) -> set[str]:
    """
    Extract invoice IDs from normalized ticket text.

    IMPORTANT:
    - Only trust explicit "invoice:" fields (most reliable)
    - Fallback to inv* tokens ONLY when they look like your real invoice format
      (e.g., INV + 7-10 digits). Adjust the digit range if needed.
    """
    text = text or ""
    matches: set[str] = set()

    # 1) Primary: explicit invoice field
    for value in re.findall(r"\binvoice:\s*([^\s|]+)", text, flags=re.IGNORECASE):
        v = value.strip().strip(",.;:[](){}")
        if v:
            matches.add(v.upper())

    # 2) Fallback: strict INV pattern (prevents overcounting)
    # Example: INV12760893, INV14514558
    for value in re.findall(r"\bINV\d{7,12}\b", text, flags=re.IGNORECASE):
        matches.add(value.upper())

    return matches


def extract_customer(text: str) -> Optional[str]:
    text = text or ""
    match = re.search(r"\bcustomer:\s*([^\s|]+)", text, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).strip().strip(",.;:")


_ITEM_PATTERNS = [
    r"\bitem(?:_number)?\s*:\s*([A-Za-z0-9][A-Za-z0-9\-/]*[A-Za-z0-9])\b",
    r"\bitem\s+no\.?\s*:\s*([A-Za-z0-9][A-Za-z0-9\-/]*[A-Za-z0-9])\b",
    r"\bitem\s+([0-9]{3,}(?:[-/][A-Za-z0-9]+)*)\b",
    r"\b([0-9]{3}-[0-9]{3,})\b",
    r"\b([0-9]{3,}-[A-Za-z]{1,4}[0-9]{2,})\b",
    r"\b([0-9]{7})\b",
]


def extract_item_numbers(text: str) -> set[str]:
    text = text or ""
    found: set[str] = set()
    for pat in _ITEM_PATTERNS:
        for m in re.findall(pat, text, flags=re.IGNORECASE):
            val = (m or "").strip().strip(",.;:()[]{}")
            if not val:
                continue
            val = re.sub(r"\s+", "", val).upper()
            if val.startswith("INV"):
                continue
            found.add(val)
    return found
