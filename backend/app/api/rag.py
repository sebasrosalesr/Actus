from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import traceback
import re
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.rag.next_action_engine import evaluate_next_action, evaluate_next_action_with_trace
from app.rag.new_design.service import get_runtime_service
from app.rag.store import get_rag_store

_ITEM_PATTERNS = [
    r"\bitem(?:_number)?\s*:\s*([A-Za-z0-9][A-Za-z0-9\-/]*[A-Za-z0-9])\b",
    r"\bitem\s+no\.?\s*:\s*([A-Za-z0-9][A-Za-z0-9\-/]*[A-Za-z0-9])\b",
    r"\bitem\s+([0-9]{3,}(?:[-/][A-Za-z0-9]+)*)\b",
    r"\b([0-9]{3}-[0-9]{3,})\b",
    r"\b([0-9]{3,}-[A-Za-z]{1,4}[0-9]{2,})\b",
    r"\b([0-9]{7})\b",
]


def extract_invoice_ids(text: str) -> set[str]:
    text = text or ""
    matches: set[str] = set()
    for value in re.findall(r"\binvoice:\s*([^\s|]+)", text, flags=re.IGNORECASE):
        v = value.strip().strip(",.;:[](){}")
        if v:
            matches.add(v.upper())
    for value in re.findall(r"\bINV\d{7,12}\b", text, flags=re.IGNORECASE):
        matches.add(value.upper())
    return matches


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


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _safe_text(value: Any) -> str:
    return ("" if value is None else str(value)).strip()


def _norm(value: Any) -> str:
    return " ".join(_safe_text(value).lower().split())


def _invoice_ids_from_value(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, (list, tuple, set)):
        found: set[str] = set()
        for item in value:
            found |= _invoice_ids_from_value(item)
        return found
    if isinstance(value, dict):
        found: set[str] = set()
        for item in value.values():
            found |= _invoice_ids_from_value(item)
        return found
    text = str(value).strip()
    if not text:
        return set()
    extracted = extract_invoice_ids(text)
    if extracted:
        return extracted
    return {text.upper()}


def _invoice_ids_from_meta(meta: dict[str, Any]) -> set[str]:
    invoice_ids: set[str] = set()
    for key in ("invoice", "invoice_number", "inv", "invoice_ids"):
        invoice_ids |= _invoice_ids_from_value(meta.get(key))
    return invoice_ids


def _parse_iso_dt(text: str) -> Optional[datetime]:
    value = _safe_text(text)
    if not value:
        return None
    try:
        if "T" in value and value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        try:
            dt = datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
            return dt
        except Exception:
            return None


def _age_days(last_ts: Optional[datetime]) -> Optional[float]:
    if not last_ts:
        return None
    delta = _now_utc() - last_ts
    return delta.total_seconds() / 86400.0


def _money_from_text(text: str) -> Optional[float]:
    value = _norm(text)
    match = re.search(r"\btotal[_\s]*credit[:\s]+([0-9]+(?:\.[0-9]+)?)", value)
    if match:
        try:
            return float(match.group(1))
        except Exception:
            return None
    return None


def _format_date_human(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    try:
        parsed = _parse_iso_dt(date_str)
        if not parsed:
            cleaned = re.sub(r"(\d{1,2})(st|nd|rd|th)", r"\\1", str(date_str))
            for fmt in ("%B %d, %Y", "%b %d, %Y"):
                try:
                    parsed = datetime.strptime(cleaned, fmt).replace(tzinfo=timezone.utc)
                    break
                except Exception:
                    parsed = None
            if not parsed:
                return date_str
        return parsed.strftime("%b %d, %Y")
    except Exception:
        return date_str


def _build_closure_note(semantic: Dict[str, Any]) -> Optional[str]:
    credit_number = semantic.get("credit_number")
    credit_numbers = semantic.get("credit_numbers") or []
    credit_amount = semantic.get("credit_amount")
    credit_date = _format_date_human(semantic.get("credit_date"))
    closed_date = _format_date_human(semantic.get("closed_date"))
    corrected_date = _format_date_human(semantic.get("pricing_corrected_date"))

    if not any([credit_number, credit_date, closed_date, isinstance(credit_amount, (int, float))]):
        return None

    parts: list[str] = []
    head_bits: list[str] = []
    if isinstance(credit_numbers, list) and len(credit_numbers) > 1:
        total = len(credit_numbers)
        shown = [str(v) for v in credit_numbers[:3]]
        suffix = f" (+{total - len(shown)} more)" if total > len(shown) else ""
        head_bits.append(f"Credit numbers provided ({total} total): {', '.join(shown)}{suffix}")
    elif credit_number:
        head_bits.append(f"Credit {credit_number}")
    else:
        head_bits.append("Credit")
    if isinstance(credit_amount, (int, float)):
        head_bits.append(f"for ${credit_amount:.2f}")
    head = " ".join(head_bits).strip()
    if head or credit_date:
        issued = f"was issued on {credit_date}" if credit_date else "was issued"
        if isinstance(credit_numbers, list) and len(credit_numbers) > 1:
            issued = f"were issued on {credit_date}" if credit_date else "were issued"
        parts.append(f"{head + ' ' if head else ''}{issued}.".strip())
    if corrected_date:
        parts.append(f"Pricing was corrected on {corrected_date}.")
    if closed_date:
        parts.append(f"Ticket was formally closed on {closed_date}.")
    parts.append("No further action required.")
    return " ".join(parts).replace("  ", " ").strip()


def _is_terminal_decision(
    decision: ActionDecision,
    *,
    context: Dict[str, Any],
) -> bool:
    tag = decision.action_tag
    rule_id = decision.action_rule_id
    next_action = (decision.next_action or "").lower()
    has_cr = bool((context.get("flags") or {}).get("has_cr_number"))
    has_credit_completed = bool((context.get("flags") or {}).get("has_credit_completed"))

    if tag == "completed":
        return True
    if isinstance(rule_id, str) and rule_id.startswith("completed_"):
        return True
    if has_credit_completed:
        return True
    if has_cr and "completed" in next_action:
        return True
    return False


def _score_band(score: Optional[float]) -> str:
    if score is None:
        return "medium"
    if score >= 0.78:
        return "high"
    if score >= 0.55:
        return "medium"
    return "low"


def _suppress_trace_if_terminal(
    trace: Optional[List[Dict[str, Any]]],
    *,
    terminal: bool,
) -> Optional[List[Dict[str, Any]]]:
    if not trace or not terminal:
        return trace

    filtered = [
        item
        for item in trace
        if not str(item.get("id", "")).startswith("summary_")
        and str(item.get("id", "")) != "catch_all"
    ]

    kept = []
    for item in filtered:
        if not item.get("matched"):
            continue
        if item.get("id") == "completed_cr_number":
            kept.append(item)
            continue
        action = item.get("action") or {}
        if action.get("tag") == "completed":
            kept.append(item)
            continue
        priority = item.get("priority")
        if isinstance(priority, int) and priority >= 950:
            kept.append(item)

    if kept:
        return kept

    completed = next((i for i in filtered if i.get("id") == "completed_cr_number"), None)
    if completed:
        return [completed]

    first_matched = next((i for i in filtered if i.get("matched")), None)
    return [first_matched] if first_matched else []


def _extract_last_status_ts(snippets: List[Dict[str, Any]]) -> Optional[datetime]:
    best: Optional[datetime] = None
    for snippet in snippets or []:
        text = _safe_text(snippet.get("text"))
        for match in re.finditer(
            r"\[(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})\]", text
        ):
            dt = _parse_iso_dt(match.group(1))
            if dt and (best is None or dt > best):
                best = dt
    return best


def _extract_cr_number(text: str) -> Optional[str]:
    value = _norm(text)
    if not value:
        return None
    match = re.search(
        r"\b(?:credit request(?:\s+no\.?|\s+number)|cr\s*(?:no\.?|number))\b\s*[:#]?\s*([a-z0-9_-]+)",
        value,
    )
    if not match:
        rtn_match = re.search(r"\brtn[a-z0-9_-]{6,}\b", value)
        if not rtn_match:
            return None
        cr_value = rtn_match.group(0).strip().lower()
    else:
        cr_value = match.group(1).strip().lower()
    if cr_value in {
        "0",
        "none",
        "n/a",
        "na",
        "null",
        "undefined",
        "no",
        "number",
        "provided",
        "sent",
        "sentout",
        "verified",
        "processing",
        "completed",
        "added",
        "will",
    }:
        return None
    return cr_value


def _extract_cr_numbers(text: str) -> List[str]:
    value = _norm(text)
    if not value:
        return []
    found: set[str] = set()
    for match in re.finditer(
        r"\b(?:credit request(?:\s+no\.?|\s+number)|cr\s*(?:no\.?|number))\b\s*[:#]?\s*([a-z0-9_-]+)",
        value,
    ):
        candidate = match.group(1).strip().lower()
        if candidate and _extract_cr_number(f"cr {candidate}"):
            found.add(candidate.upper())
    for match in re.finditer(r"\brtn[a-z0-9_-]{6,}\b", value):
        found.add(match.group(0).strip().upper())
    return sorted(found)


def _extract_semantic_dates(text: str) -> Dict[str, Optional[str]]:
    credit_date = None
    closed_date = None
    pricing_corrected_date = None
    human_date_re = re.compile(
        r"\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|sept|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:st|nd|rd|th)?(?:,)?\s+\d{4}\b",
        re.IGNORECASE,
    )
    seen_ts: set[str] = set()
    closed_date_from_human = False

    def _find_human_date(pattern: str, *, flags: int = re.IGNORECASE) -> Optional[str]:
        match = re.search(pattern, text or "", flags=flags)
        if not match:
            return None
        return match.group(1)

    def _process_ts(ts: str, window: str) -> None:
        nonlocal credit_date, closed_date, pricing_corrected_date
        window_low = window.lower()

        credit_strong = bool(
            re.search(r"\bcr number verified\b|\bcredit processing completed\b", window_low)
        )
        credit_soft = bool(
            re.search(
                r"\bcredit\b.*\b(issued|processed|completed|provided|verified)\b|\bcredit number provided\b|\bcr number\b",
                window_low,
            )
        )
        closed_soft = bool(
            re.search(r"\b(closed|resolved|auto(?:matically)? closed)\b", window_low)
        )
        closed_future = bool(
            re.search(r"\bwill be closed\b|\bwill close\b|\bin \d+\s+days\b", window_low)
        )
        pricing_soft = bool(
            re.search(
                r"\b(price|pricing)\b.*\b(updated|corrected|fixed|adjusted|reverted)\b|\bupdated to ppd\b",
                window_low,
            )
        )

        if credit_strong:
            credit_date = ts
        elif credit_soft and credit_date is None:
            credit_date = ts

        if pricing_soft and pricing_corrected_date is None:
            pricing_corrected_date = ts

        if closed_soft and not closed_future and closed_date is None and not closed_date_from_human:
            if "closed on" in window_low:
                return
            closed_date = ts

    if text:
        credit_match = _find_human_date(
            r"(?:credit numbers?\s+sent(?:\s+out)?|credit number provided|cr number verified|credit processing completed|credit issued)[^\n]{0,160}?\b("
            + human_date_re.pattern
            + r")",
            flags=re.IGNORECASE,
        )
        if credit_match:
            credit_date = credit_match

        closed_match = _find_human_date(
            r"(?:ticket\s+officially\s+closed\s+on|ticket\s+closed\s+on|closed\s+on|resolved\s+on)[^\n]{0,80}?\b("
            + human_date_re.pattern
            + r")",
            flags=re.IGNORECASE,
        )
        if closed_match:
            closed_date = closed_match
            closed_date_from_human = True

        pricing_match = _find_human_date(
            r"(?:price|pricing)[^\n]{0,120}?\b(?:updated|corrected|fixed|adjusted|reverted)[^\n]{0,40}?\b("
            + human_date_re.pattern
            + r")",
            flags=re.IGNORECASE,
        )
        if pricing_match:
            pricing_corrected_date = pricing_match

    for match in re.finditer(r"\[(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})\]", text or ""):
        ts = match.group(1)
        if ts in seen_ts:
            continue
        seen_ts.add(ts)
        window_start = max(0, match.start() - 120)
        window_end = min(len(text), match.end() + 120)
        window = (text[window_start:window_end] or "")
        _process_ts(ts, window)

    for match in re.finditer(r"\b(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})\b", text or ""):
        ts = match.group(1)
        if ts in seen_ts:
            continue
        seen_ts.add(ts)
        window_start = max(0, match.start() - 120)
        window_end = min(len(text), match.end() + 120)
        window = (text[window_start:window_end] or "")
        _process_ts(ts, window)

    if text:
        lowered = text.lower()
        for pat, target in (
            (
                r"\bcredit\b.*\b(issued|processed|completed|provided|verified|sent)\b|\bcredit number sent out\b",
                "credit",
            ),
            (r"\b(closed|resolved|auto(?:matically)? closed)\b", "closed"),
            (
                r"\b(price|pricing)\b.*\b(updated|corrected|fixed|adjusted|reverted)\b|\bupdated to ppd\b",
                "pricing",
            ),
        ):
            for m in re.finditer(pat, lowered):
                if (target == "credit" and credit_date) or (
                    target == "closed" and closed_date
                ) or (target == "pricing" and pricing_corrected_date):
                    break
                window_start = max(0, m.start() - 80)
                window_end = min(len(text), m.end() + 120)
                window = text[window_start:window_end]
                human_match = human_date_re.search(window)
                if not human_match:
                    continue
                date_str = human_match.group(0)
                if target == "credit" and (credit_date is None or "sent out" in window.lower()):
                    credit_date = date_str
                elif target == "closed":
                    if re.search(r"\bwill be closed\b|\bwill close\b|\bin \d+\s+days\b", window.lower()):
                        continue
                    closed_date = date_str
                    closed_date_from_human = True
                elif target == "pricing" and pricing_corrected_date is None:
                    pricing_corrected_date = date_str

    return {
        "credit_date": credit_date,
        "closed_date": closed_date,
        "pricing_corrected_date": pricing_corrected_date,
    }


@dataclass
class ActionDecision:
    next_action: str
    action_confidence: str
    action_reason_codes: List[str]
    action_tag: Optional[str] = None
    action_rule_id: Optional[str] = None


def _detect_signals(text: str) -> Dict[str, bool]:
    value = _norm(text)
    return {
        "ppd": bool(re.search(r"\bppd\b", value)),
        "non_ppd": bool(re.search(r"\bnon[-\s]?ppd\b", value)),
        "contract": bool(re.search(r"\bcontract\b|\bnon[-\s]?contract\b", value)),
        "price_sheet": bool(re.search(r"\bprice\s*sheet\b|\bps\b", value)),
        "uom": bool(re.search(r"\buom\b|\bcs\b|\bpk\b|\bbox\b|\bea\b", value)),
        "loaded_incorrectly": bool(
            re.search(r"\bloaded incorrectly\b|\bloaded wrong\b", value)
        ),
        "priced_wrong": bool(
            re.search(r"\bpriced wrong\b|\bwrong price\b|\bpricing was incorrect\b", value)
        ),
        "should_be": bool(re.search(r"\bshould be\b|\bshould have\b", value)),
        "no_documentation": bool(
            re.search(r"\bno documentation\b|\bdocument not found\b", value)
        ),
        "approved": bool(re.search(r"\bapproved\b", value)),
        "declined": bool(re.search(r"\bdeclined\b|\bno credit warranted\b", value)),
        "waiting": bool(
            re.search(r"\bwaiting on\b|\bawaiting\b|\bpending\b|\bneed to hear back\b", value)
        ),
        "in_process": bool(
            re.search(r"\bin process\b|\bon macro\b|\binvestigation\b|\bwent through investigation\b", value)
        ),
        "credit_completed": bool(
            re.search(
                r"\bcredit number provided\b|\bcredit numbers? sent(?:\s+out)?\b|\bcr number verified\b|\bcredit processing completed\b",
                value,
            )
        ),
    }


def _has_analysis_signals(text: str) -> bool:
    value = _norm(text)
    patterns = [
        r"\bmargin\b",
        r"\brebate\b",
        r"\btarget\s*margin\b",
        r"\bguidance\b",
        r"\bpriced?\s+at\b",
        r"\bprice[-\s]*match\b",
        r"\bdoes\s+not\s+.*pricing\s+error\b",
        r"\bvalidated\b|\bconfirmed\b|\banalysis\b",
        r"\bdiscrepanc(y|ies)\b",
        r"\bper\s+[a-z]+\b",
    ]
    hits = 0
    for pat in patterns:
        if re.search(pat, value):
            hits += 1
    return hits >= 2


def _has_explicit_investigation(snippets: List[Dict[str, Any]], combined_text: str) -> bool:
    for snippet in snippets or []:
        chunk_type = _norm(snippet.get("chunk_type"))
        if chunk_type in {"investigation_note", "note"}:
            return True
        meta = snippet.get("metadata") or {}
        event_type = _norm(meta.get("event_type"))
        if event_type in {"investigation_note", "note"}:
            return True
        if "investigation note:" in _norm(snippet.get("text")):
            return True
    return "investigation note:" in _norm(combined_text)


def _build_action_context(
    *,
    snippets: List[Dict[str, Any]],
    reason_for_credit: Optional[str] = None,
    investigation_note_body: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    reason_txt = _norm(reason_for_credit)
    combined_snips = "\n".join(_safe_text(s.get("text")) for s in (snippets or []))
    note_text = _safe_text(investigation_note_body)
    signals = _detect_signals("\n".join([reason_txt, combined_snips, note_text]))

    has_analysis = _has_analysis_signals(combined_snips)
    has_explicit_investigation = _has_explicit_investigation(snippets, combined_snips)
    has_investigation = has_explicit_investigation or has_analysis

    cr_numbers = _extract_cr_numbers(combined_snips)
    if metadata:
        meta_cr = metadata.get("credit_numbers")
        if isinstance(meta_cr, (list, tuple, set)):
            cr_numbers = sorted(set([str(v).strip().upper() for v in meta_cr if v]))
    cr_number = cr_numbers[0] if cr_numbers else None
    if not cr_numbers and metadata:
        for key in ("credit_request_no", "credit_request_number", "cr_number", "cr_no"):
            value = metadata.get(key)
            if value:
                cr_number = _extract_cr_number(f"cr {value}")
                if cr_number:
                    cr_numbers = [cr_number.upper()]
                    break

    last_ts = _extract_last_status_ts(snippets or [])
    age = _age_days(last_ts)
    credit_amt = _money_from_text(combined_snips)
    semantic_dates = _extract_semantic_dates(combined_snips)

    return {
        "signals": signals,
        "flags": {
            "has_snippets": bool(combined_snips.strip()),
            "has_investigation_note": bool(note_text.strip()),
            "has_analysis": has_analysis,
            "has_explicit_investigation": has_explicit_investigation,
            "has_investigation": has_investigation,
            "has_cr_number": bool(cr_numbers),
            "has_credit_completed": bool(signals.get("credit_completed")),
            "credit_amount_missing": credit_amt is None,
            "always": True,
        },
        "fields": {
            "age_days": age,
            "credit_amount": credit_amt,
        },
        "semantic": {
            "credit_number": cr_number.upper() if isinstance(cr_number, str) else cr_number,
            "credit_numbers": cr_numbers,
            "credit_amount": credit_amt,
            "credit_date": semantic_dates.get("credit_date"),
            "closed_date": semantic_dates.get("closed_date"),
            "pricing_corrected_date": semantic_dates.get("pricing_corrected_date"),
        },
    }


def _fallback_action_decision() -> ActionDecision:
    return ActionDecision(
        next_action="Needs investigation note. Add a short note (what happened / evidence / next step) "
        "or follow up with requestor for missing details.",
        action_confidence="low",
        action_reason_codes=["tier_c_minimal"],
    )


def decide_next_action(
    ticket_id: str,
    *,
    snippets: List[Dict[str, Any]],
    reason_for_credit: Optional[str] = None,
    investigation_note_body: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> ActionDecision:
    context = _build_action_context(
        snippets=snippets,
        reason_for_credit=reason_for_credit,
        investigation_note_body=investigation_note_body,
        metadata=metadata,
    )
    decision = evaluate_next_action(context)
    if decision:
        return ActionDecision(
            next_action=decision.next_action,
            action_confidence=decision.action_confidence,
            action_reason_codes=decision.action_reason_codes,
            action_tag=decision.action_tag,
            action_rule_id=decision.rule_id,
        )

    return _fallback_action_decision()


def enrich_next_action(result: Dict[str, Any]) -> Dict[str, Any]:
    snippets = result.get("snippets") or []
    inv_body = result.get("investigation_note_body")
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else None
    context = _build_action_context(
        snippets=snippets,
        reason_for_credit=result.get("reason_for_credit"),
        investigation_note_body=inv_body,
        metadata=metadata,
    )
    decision = decide_next_action(
        result.get("ticket_id") or "",
        snippets=snippets,
        reason_for_credit=result.get("reason_for_credit"),
        investigation_note_body=inv_body,
        metadata=metadata,
    )
    result["next_action"] = decision.next_action
    result["action_confidence"] = decision.action_confidence
    result["action_reason_codes"] = decision.action_reason_codes
    if decision.action_rule_id:
        result["action_rule_id"] = decision.action_rule_id
    if decision.action_tag:
        result["action_tag"] = decision.action_tag
    elif "cr_number_present" in decision.action_reason_codes:
        result["action_tag"] = "completed"

    terminal = _is_terminal_decision(decision, context=context)
    semantic = {
        "credit_number": (context.get("semantic") or {}).get("credit_number"),
        "credit_numbers": (context.get("semantic") or {}).get("credit_numbers"),
        "credit_amount": (context.get("semantic") or {}).get("credit_amount"),
        "credit_date": (context.get("semantic") or {}).get("credit_date"),
        "closed_date": (context.get("semantic") or {}).get("closed_date"),
        "pricing_corrected_date": (context.get("semantic") or {}).get("pricing_corrected_date"),
        "verified_by": (context.get("semantic") or {}).get("verified_by"),
    }
    closure_note = _build_closure_note(semantic) if terminal else None
    resolution = None
    if terminal:
        resolution = {
            "status": "completed",
            "method": "credit_issued" if semantic.get("credit_number") else "credit_number_present",
            "credit_number": semantic.get("credit_number"),
            "credit_numbers": semantic.get("credit_numbers"),
            "credit_date": semantic.get("credit_date"),
            "amount": semantic.get("credit_amount"),
            "closed_date": semantic.get("closed_date"),
            "pricing_corrected_date": semantic.get("pricing_corrected_date"),
            "verified_by": semantic.get("verified_by") or "cs_note",
        }

    result["ui"] = {
        "score_label": "Decision Confidence Match",
        "score_band": _score_band(result.get("score")),
        "score_value": result.get("score"),
    }
    result["meta"] = {
        "terminal_decision": terminal,
        "closure_note": closure_note,
        "resolution": resolution,
    }
    return result


router = APIRouter()

class TicketRefsResponse(BaseModel):
    ticket_id: str
    invoice_ids: list[str]
    item_numbers: list[str]


class NextActionTraceRequest(BaseModel):
    ticket_id: Optional[str] = None
    snippets: List[Dict[str, Any]] = []
    reason_for_credit: Optional[str] = None
    investigation_note_body: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class NewDesignSearchRequest(BaseModel):
    query: str
    top_k: int = 5
    refresh: bool = False


class NewDesignAnswerRequest(BaseModel):
    query: str
    top_k: int = 10
    max_tickets_in_answer: int = 5
    refresh: bool = False


class NewDesignRefreshRequest(BaseModel):
    index: bool = False


class NewDesignItemAnalysisRequest(BaseModel):
    item_number: str
    refresh: bool = False


class NewDesignTicketAnalysisRequest(BaseModel):
    ticket_id: str
    threshold_days: int = 30
    refresh: bool = False


@router.get("/rag/health")
def rag_health() -> dict[str, Any]:
    store = get_rag_store()
    try:
        return {
            "provider": store.provider_name(),
            "has_data": store.has_data(),
            "stats": store.stats(),
        }
    finally:
        try:
            store.close()
        except Exception:
            pass


@router.post("/rag/search")
def rag_search() -> dict[str, Any]:
    raise HTTPException(
        status_code=410,
        detail="Legacy /rag/search was removed. Use /rag/new/search.",
    )


@router.get("/rag/ticket/{ticket_id}/refs", response_model=TicketRefsResponse)
def rag_ticket_refs(ticket_id: str):
    store = get_rag_store()
    try:
        rows = store.get_ticket_chunks(ticket_id) or []
        invoice_ids: set[str] = set()
        item_numbers: set[str] = set()

        for row in rows:
            txt = (row.get("text") or "")
            invoice_ids |= extract_invoice_ids(txt)
            item_numbers |= extract_item_numbers(txt)
            meta = row.get("metadata") or {}
            if isinstance(meta, dict):
                invoice_ids |= _invoice_ids_from_meta(meta)
                for key in (
                    "item",
                    "item_number",
                    "item_num",
                    "itemnumber",
                    "item_id",
                    "itemcode",
                    "sku",
                    "product",
                    "item_no",
                ):
                    value = meta.get(key)
                    if value:
                        item_numbers |= extract_item_numbers(str(value))
                for value in meta.values():
                    if isinstance(value, (list, dict)):
                        item_numbers |= extract_item_numbers(str(value))

        if not item_numbers:
            line_texts = store.get_ticket_line_texts(ticket_id)
            for txt in line_texts:
                item_numbers |= extract_item_numbers(txt)

        return {
            "ticket_id": ticket_id,
            "invoice_ids": sorted(invoice_ids),
            "item_numbers": sorted(item_numbers),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        try:
            store.close()
        except Exception:
            pass


@router.post("/rag/next-action/trace")
def rag_next_action_trace(payload: NextActionTraceRequest) -> dict[str, Any]:
    context = _build_action_context(
        snippets=payload.snippets or [],
        reason_for_credit=payload.reason_for_credit,
        investigation_note_body=payload.investigation_note_body,
        metadata=payload.metadata,
    )
    decision, trace = evaluate_next_action_with_trace(context)
    if decision:
        final = ActionDecision(
            next_action=decision.next_action,
            action_confidence=decision.action_confidence,
            action_reason_codes=decision.action_reason_codes,
            action_tag=decision.action_tag,
            action_rule_id=decision.rule_id,
        )
    else:
        final = _fallback_action_decision()
    return {
        "ticket_id": payload.ticket_id,
        "decision": {
            "next_action": final.next_action,
            "action_confidence": final.action_confidence,
            "action_reason_codes": final.action_reason_codes,
            "action_tag": final.action_tag,
            "action_rule_id": final.action_rule_id,
        },
        "context": context,
        "trace": _suppress_trace_if_terminal(
            trace, terminal=_is_terminal_decision(final, context=context)
        ),
    }


@router.post("/rag/new/search")
def rag_new_search(payload: NewDesignSearchRequest) -> dict[str, Any]:
    try:
        service = get_runtime_service(refresh=payload.refresh)
        return service.search(payload.query, top_k=payload.top_k)
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}")


@router.post("/rag/new/answer")
def rag_new_answer(payload: NewDesignAnswerRequest) -> dict[str, Any]:
    try:
        service = get_runtime_service(refresh=payload.refresh)
        return service.answer(
            payload.query,
            top_k=payload.top_k,
            max_tickets_in_answer=payload.max_tickets_in_answer,
        )
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}")


@router.post("/rag/new/refresh")
def rag_new_refresh(payload: Optional[NewDesignRefreshRequest] = None) -> dict[str, Any]:
    try:
        request_payload = payload or NewDesignRefreshRequest()
        service = get_runtime_service(refresh=False)
        refresh_info = service.refresh_from_firebase()

        response: dict[str, Any] = {
            "refreshed": True,
            "ready": service.is_ready,
            "chunk_count": service.chunk_count,
            "refresh_info": refresh_info,
        }

        if request_payload.index:
            response["index_info"] = service.index_current()

        return response
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}")


@router.post("/rag/new/item-analysis")
def rag_new_item_analysis(payload: NewDesignItemAnalysisRequest) -> dict[str, Any]:
    try:
        item_number = (payload.item_number or "").strip()
        if not item_number:
            raise HTTPException(status_code=400, detail="item_number is required.")
        service = get_runtime_service(refresh=payload.refresh)
        return service.analyze_item(item_number)
    except HTTPException:
        raise
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}")


@router.post("/rag/new/ticket-analysis")
def rag_new_ticket_analysis(payload: NewDesignTicketAnalysisRequest) -> dict[str, Any]:
    try:
        ticket_id = (payload.ticket_id or "").strip()
        if not ticket_id:
            raise HTTPException(status_code=400, detail="ticket_id is required.")

        threshold_days = int(payload.threshold_days or 30)
        if threshold_days <= 0:
            raise HTTPException(status_code=400, detail="threshold_days must be > 0.")

        service = get_runtime_service(refresh=payload.refresh)
        return service.analyze_ticket(ticket_id=ticket_id, threshold_days=threshold_days)
    except HTTPException:
        raise
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}")
