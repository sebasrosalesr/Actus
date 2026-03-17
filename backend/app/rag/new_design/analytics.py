from __future__ import annotations

from collections import Counter
from datetime import datetime
import re
from typing import Any

from .models import CanonicalTicket, TicketLine


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


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


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None

    candidates = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%fZ",
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def _extract_datetimes_from_text(value: Any) -> list[datetime]:
    text = str(value or "")
    if not text:
        return []

    matches = re.findall(r"\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2})?", text)
    out: list[datetime] = []
    for match in matches:
        dt = _parse_datetime(match)
        if dt is not None:
            out.append(dt)
    return out


def _iter_ticket_lines(ticket: CanonicalTicket | dict[str, Any]) -> list[TicketLine | dict[str, Any]]:
    if isinstance(ticket, CanonicalTicket):
        out: list[TicketLine | dict[str, Any]] = []
        for line_list in ticket.line_map.values():
            out.extend(line_list)
        return out

    out: list[TicketLine | dict[str, Any]] = []
    line_map = ticket.get("line_map", {}) if isinstance(ticket, dict) else {}
    if isinstance(line_map, dict):
        for line_value in line_map.values():
            if isinstance(line_value, list):
                out.extend(line_value)
            else:
                out.append(line_value)
    return out


def _line_get(line: TicketLine | dict[str, Any], key: str, default: Any = None) -> Any:
    if isinstance(line, dict):
        return line.get(key, default)
    return getattr(line, key, default)


def _ticket_get(ticket: CanonicalTicket | dict[str, Any], key: str, default: Any = None) -> Any:
    if isinstance(ticket, dict):
        return ticket.get(key, default)
    return getattr(ticket, key, default)


def _ticket_account_prefixes(ticket: CanonicalTicket | dict[str, Any]) -> list[str]:
    prefixes = _ticket_get(ticket, "account_prefixes", []) or []
    if prefixes:
        return [str(p).strip().upper() for p in prefixes if str(p).strip()]

    out: list[str] = []
    for customer in _ticket_get(ticket, "customer_numbers", []) or []:
        value = str(customer).strip().upper()
        prefix = "".join(ch for ch in value if ch.isalpha())
        if prefix:
            out.append(prefix)
    return out


def analyze_item_actus(item_number: str, canonical_tickets: dict[str, CanonicalTicket | dict[str, Any]]) -> dict[str, Any]:
    item_norm = _norm_text(item_number)
    if not item_norm:
        return {"item_number": item_number, "answer": "Item number is required."}

    tickets: set[str] = set()
    invoices: set[str] = set()
    sales_reps = Counter()
    account_prefixes = Counter()
    root_causes = Counter()
    root_causes_all = Counter()
    dates: list[datetime] = []

    total_credit = 0.0
    line_count = 0

    for ticket_id, ticket in canonical_tickets.items():
        matched_lines: list[TicketLine | dict[str, Any]] = []
        for line in _iter_ticket_lines(ticket):
            if _norm_text(_line_get(line, "item_number")) != item_norm:
                continue
            matched_lines.append(line)

        if not matched_lines:
            continue

        ticket_id_str = str(ticket_id).strip().upper()
        tickets.add(ticket_id_str)

        for line in matched_lines:
            line_count += 1

            invoice = _line_get(line, "invoice_number")
            if invoice:
                invoices.add(str(invoice).strip().upper())

            total_credit += _safe_float(_line_get(line, "credit_request_total"))

            primary = str(_line_get(line, "root_cause_primary_id", "")).strip()
            if primary and primary.lower() != "unidentified":
                root_causes[primary] += 1
            else:
                for root in _line_get(line, "root_cause_ids", []) or []:
                    root_text = str(root).strip()
                    if root_text and root_text.lower() != "unidentified":
                        root_causes[root_text] += 1
                        break

            # Count all attached root causes (line-level), useful for mixed-cause items.
            all_ids: set[str] = set()
            for root in _line_get(line, "root_cause_ids", []) or []:
                root_text = str(root).strip()
                if root_text and root_text.lower() != "unidentified":
                    all_ids.add(root_text)
            if not all_ids and primary and primary.lower() != "unidentified":
                all_ids.add(primary)
            for root_id in all_ids:
                root_causes_all[root_id] += 1

            for note in _line_get(line, "investigation_notes", []) or []:
                created = _parse_datetime(_line_get(note, "created_at"))
                updated = _parse_datetime(_line_get(note, "updated_at"))
                if created is not None:
                    dates.append(created)
                if updated is not None:
                    dates.append(updated)

        for rep in _ticket_get(ticket, "sales_reps", []) or []:
            rep_text = str(rep).strip().upper()
            if rep_text:
                sales_reps[rep_text] += 1

        for prefix in _ticket_account_prefixes(ticket):
            account_prefixes[prefix] += 1

        for status in _ticket_get(ticket, "status_raw_list", []) or []:
            dates.extend(_extract_datetimes_from_text(status))

    if not tickets:
        return {
            "item_number": item_number,
            "ticket_count": 0,
            "invoice_count": 0,
            "line_count": 0,
            "total_credit": 0.0,
            "root_cause_counts": {},
            "root_cause_counts_all": {},
            "sales_rep_counts": {},
            "account_prefix_counts": {},
            "tickets": [],
            "invoices": [],
            "first_seen": None,
            "last_seen": None,
            "answer": f"No credit activity found for item {item_number}.",
        }

    first_seen = min(dates).strftime("%Y-%m-%d") if dates else "unknown"
    last_seen = max(dates).strftime("%Y-%m-%d") if dates else "unknown"

    root_summary = (
        "\n".join(f"- {root} ({count})" for root, count in root_causes.most_common(3))
        if root_causes
        else "- none attached"
    )
    rep_summary = ", ".join([rep for rep, _ in sales_reps.most_common(3)]) if sales_reps else "none attached"
    prefix_summary = (
        ", ".join([prefix for prefix, _ in account_prefixes.most_common(3)])
        if account_prefixes
        else "none attached"
    )
    ticket_preview = ", ".join(sorted(tickets)[:3])
    invoice_preview = ", ".join(sorted(invoices)[:3])

    answer = (
        f"Item {item_number} analysis\n\n"
        f"This item appears in {len(tickets)} credit tickets across {line_count} invoice lines.\n"
        f"Total credited amount is ${total_credit:,.2f}.\n\n"
        f"Most common root causes:\n{root_summary}\n\n"
        f"Sales reps involved:\n{rep_summary}\n\n"
        f"Account prefixes affected:\n{prefix_summary}\n\n"
        f"First observed activity: {first_seen}\n"
        f"Most recent activity: {last_seen}\n\n"
        f"Tickets involved:\n{ticket_preview} +...\n\n"
        f"Invoices involved:\n{invoice_preview} +..."
    )

    return {
        "item_number": item_number,
        "ticket_count": len(tickets),
        "invoice_count": len(invoices),
        "line_count": line_count,
        "total_credit": total_credit,
        "root_cause_counts": dict(root_causes),
        "root_cause_counts_all": dict(root_causes_all),
        "sales_rep_counts": dict(sales_reps),
        "account_prefix_counts": dict(account_prefixes),
        "tickets": sorted(tickets),
        "invoices": sorted(invoices),
        "first_seen": first_seen,
        "last_seen": last_seen,
        "answer": answer,
    }


_STATUS_EVENT_PATTERN = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*\n?(.*?)(?=(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})|$)",
    flags=re.DOTALL,
)

_CREDIT_NUMBER_PATTERN = re.compile(r"\bRTN[A-Z0-9_-]{4,}\b", flags=re.IGNORECASE)

_CREDIT_OR_CLOSURE_PATTERNS = [
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


def _normalize_ticket_id(ticket_id: str) -> str:
    value = (ticket_id or "").strip().upper()
    if value.startswith("R"):
        if value.startswith("R-"):
            return value
        if len(value) > 1 and value[1:].isdigit():
            return f"R-{value[1:]}"
    return value


def _format_list_preview(values: list[Any], max_items: int = 6) -> str:
    cleaned = [str(v).strip().upper() for v in values if str(v).strip()]
    if not cleaned:
        return "none"
    if len(cleaned) <= max_items:
        return ", ".join(cleaned)
    shown = ", ".join(cleaned[:max_items])
    return f"{shown} +{len(cleaned) - max_items} more"


def _clean_answer_text(value: Any, max_len: int = 180) -> str:
    text = " ".join(str(value or "").split()).strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 3].rstrip() + "..."


def _extract_investigation_highlights(
    ticket: CanonicalTicket | dict[str, Any],
    max_highlights: int = 2,
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for line in _iter_ticket_lines(ticket):
        for note in _line_get(line, "investigation_notes", []) or []:
            note_title = _line_get(note, "title")
            note_body = _line_get(note, "body_clean") or _line_get(note, "body_raw")
            if not note_body:
                continue
            text = str(note_body).strip()
            if note_title:
                text = f"{note_title}: {text}"
            cleaned = " ".join(text.split()).strip()
            key = cleaned.lower()
            if not cleaned or key in seen:
                continue
            seen.add(key)
            out.append(cleaned)
            if len(out) >= max_highlights:
                return out

        for chunk in _line_get(line, "investigation_chunks", []) or []:
            text = _line_get(chunk, "chunk_text")
            cleaned = " ".join(str(text or "").split()).strip()
            key = cleaned.lower()
            if not cleaned or key in seen:
                continue
            seen.add(key)
            out.append(cleaned)
            if len(out) >= max_highlights:
                return out

    return out


def _status_indicates_credit_or_closure(text: str) -> bool:
    value = str(text or "").lower()
    if not value:
        return False
    return any(re.search(pattern, value) for pattern in _CREDIT_OR_CLOSURE_PATTERNS)


def _has_credit_number(values: list[Any]) -> bool:
    for value in values or []:
        if _is_valid_credit_value(value):
            return True
    return False


def _is_valid_credit_value(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if text.lower() in {"nan", "none", "null", "na", "n/a", "0"}:
        return False
    # Most values are RTN-like, but keep non-empty non-placeholder values as valid.
    return bool(_CREDIT_NUMBER_PATTERN.search(text) or text)


def parse_status_events(status_text: Any) -> list[dict[str, Any]]:
    if status_text is None:
        return []
    text = str(status_text).strip()
    if not text or text.lower() == "nan":
        return []

    events: list[dict[str, Any]] = []
    for ts, body, _ in re.findall(_STATUS_EVENT_PATTERN, text):
        dt = _parse_datetime(ts)
        if dt is None:
            continue

        raw = " ".join(str(body).split()).strip()
        body_clean = raw.lower()

        event_type = "other"
        if "open:" in body_clean or "not started" in body_clean:
            event_type = "entered"
        elif "wip" in body_clean:
            event_type = "wip"
        elif "on macro" in body_clean or "went through investigation" in body_clean:
            event_type = "investigation"
        elif "submitted to billing" in body_clean:
            event_type = "submitted_to_billing"
        elif _status_indicates_credit_or_closure(body_clean):
            event_type = "credited"

        events.append(
            {
                "timestamp": ts,
                "dt": dt,
                "event_type": event_type,
                "raw_text": raw,
            }
        )

    return events


def compute_ticket_timeline_metrics_from_status_list(
    status_raw_list: list[Any],
    threshold_days: int = 30,
    now_dt: datetime | None = None,
    has_credit_number: bool = False,
    pending_only: bool = False,
) -> dict[str, Any]:
    if now_dt is None:
        now_dt = datetime.now()

    all_events: list[dict[str, Any]] = []
    for status_text in status_raw_list or []:
        all_events.extend(parse_status_events(status_text))

    all_events = sorted(all_events, key=lambda x: x["dt"])

    events_for_pending = [e for e in all_events if e["event_type"] != "credited"] if pending_only else all_events
    entered = next((e for e in events_for_pending if e["event_type"] == "entered"), None)
    investigation = next((e for e in events_for_pending if e["event_type"] == "investigation"), None)
    submitted = next((e for e in events_for_pending if e["event_type"] == "submitted_to_billing"), None)
    credited = None if pending_only else next((e for e in all_events if e["event_type"] == "credited"), None)
    last_event = events_for_pending[-1] if events_for_pending else (all_events[-1] if all_events else None)

    credited_inferred_from_rtn = False
    if credited is None and has_credit_number and (not pending_only) and last_event is not None:
        credited = {
            "timestamp": last_event["timestamp"],
            "dt": last_event["dt"],
            "event_type": "credited",
            "raw_text": "Inferred credited from RTN/CR number evidence.",
        }
        credited_inferred_from_rtn = True

    # Fallback: many status timelines do not explicitly include "open/not started".
    # Use earliest non-terminal status event as entry anchor when available.
    if entered is None and events_for_pending:
        non_terminal_entered = next((e for e in events_for_pending if e["event_type"] != "credited"), None)
        if non_terminal_entered is not None:
            entered = non_terminal_entered
        elif credited is None:
            entered = events_for_pending[0]

    metrics: dict[str, Any] = {
        "timeline_events": [
            {
                "timestamp": event["timestamp"],
                "event_type": event["event_type"],
                "raw_text": event["raw_text"],
            }
            for event in events_for_pending
        ],
        "entered_timestamp": entered["timestamp"] if entered else None,
        "investigation_timestamp": investigation["timestamp"] if investigation else None,
        "submitted_to_billing_timestamp": submitted["timestamp"] if submitted else None,
        "credited_timestamp": credited["timestamp"] if credited else None,
        "last_status_timestamp": last_event["timestamp"] if last_event else None,
        "last_status_event_type": last_event["event_type"] if last_event else None,
        "last_status_raw_text": last_event["raw_text"] if last_event else None,
        "is_credited": (credited is not None or has_credit_number) and not pending_only,
        "credited_inferred_from_rtn": credited_inferred_from_rtn,
        "pending_only": pending_only,
        "entered_to_credited_days": None,
        "investigation_to_credited_days": None,
        "submitted_to_credited_days": None,
        "days_open": None,
        "days_pending_billing_to_credit": None,
        "entered_to_credited_over_30_days": False,
        "investigation_to_credited_over_30_days": False,
    }

    if entered and credited:
        days = (credited["dt"] - entered["dt"]).total_seconds() / 86400.0
        metrics["entered_to_credited_days"] = round(days, 2)
        metrics["entered_to_credited_over_30_days"] = days > threshold_days

    if investigation and credited:
        days = (credited["dt"] - investigation["dt"]).total_seconds() / 86400.0
        metrics["investigation_to_credited_days"] = round(days, 2)
        metrics["investigation_to_credited_over_30_days"] = days > threshold_days

    if submitted and credited:
        days = (credited["dt"] - submitted["dt"]).total_seconds() / 86400.0
        metrics["submitted_to_credited_days"] = round(days, 2)

    if entered and not metrics["is_credited"]:
        metrics["days_open"] = round((now_dt - entered["dt"]).total_seconds() / 86400.0, 2)

    if submitted and not metrics["is_credited"]:
        metrics["days_pending_billing_to_credit"] = round(
            (now_dt - submitted["dt"]).total_seconds() / 86400.0, 2
        )

    return metrics


def analyze_ticket_actus(
    ticket_id: str,
    canonical_tickets: dict[str, CanonicalTicket | dict[str, Any]],
    threshold_days: int = 30,
) -> dict[str, Any]:
    normalized_ticket_id = _normalize_ticket_id(ticket_id)
    ticket = canonical_tickets.get(normalized_ticket_id)

    if not ticket:
        return {
            "ticket_id": normalized_ticket_id or ticket_id,
            "answer": f"Ticket {normalized_ticket_id or ticket_id} was not found.",
        }

    primary_root = str(_ticket_get(ticket, "root_cause_primary_id", "unidentified") or "unidentified")
    all_roots = [str(v).strip() for v in (_ticket_get(ticket, "root_cause_ids", []) or []) if str(v).strip()]
    supporting_roots = [
        root for root in all_roots if root != primary_root and root.lower() != "unidentified"
    ]

    lines = _iter_ticket_lines(ticket)
    line_credit_total = round(
        sum(_safe_float(_line_get(line, "credit_request_total")) for line in lines),
        2,
    )
    credit_totals = _ticket_get(ticket, "credit_request_totals", []) or []
    summary_credit_total = round(sum(_safe_float(v) for v in credit_totals), 2)
    total_credit = line_credit_total if line_credit_total else summary_credit_total

    sales_reps = [
        str(v).strip().upper()
        for v in (_ticket_get(ticket, "sales_reps", []) or [])
        if str(v).strip()
    ]
    account_prefixes = _ticket_account_prefixes(ticket)

    invoice_numbers = [
        str(v).strip().upper()
        for v in (_ticket_get(ticket, "invoice_numbers", []) or [])
        if str(v).strip()
    ]
    item_numbers = [
        str(v).strip().upper()
        for v in (_ticket_get(ticket, "item_numbers", []) or [])
        if str(v).strip()
    ]
    line_count = len(lines)

    credited_line_count = 0
    credited_line_exposure = 0.0
    for line in lines:
        amount = _safe_float(_line_get(line, "credit_request_total"))
        if _is_valid_credit_value(_line_get(line, "credit_number")):
            credited_line_count += 1
            credited_line_exposure += amount

    # Fallback for pre-patch in-memory artifacts that don't carry line-level credit_number.
    ticket_credit_numbers = _ticket_get(ticket, "credit_numbers", []) or []
    if credited_line_count == 0 and line_count > 0:
        inferred_credit_count = min(sum(1 for v in ticket_credit_numbers if _is_valid_credit_value(v)), line_count)
        if inferred_credit_count > 0:
            credited_line_count = inferred_credit_count
            if line_count > 0:
                credited_line_exposure = round(total_credit * (credited_line_count / line_count), 2)

    pending_line_count = max(line_count - credited_line_count, 0)
    pending_line_exposure = round(max(total_credit - credited_line_exposure, 0.0), 2)
    credited_line_exposure = round(credited_line_exposure, 2)

    is_partially_credited = credited_line_count > 0 and pending_line_count > 0
    is_fully_credited = credited_line_count > 0 and pending_line_count == 0

    timeline = compute_ticket_timeline_metrics_from_status_list(
        _ticket_get(ticket, "status_raw_list", []) or [],
        threshold_days=threshold_days,
        has_credit_number=is_fully_credited or _has_credit_number(ticket_credit_numbers),
        pending_only=is_partially_credited,
    )
    entered_days = timeline.get("entered_to_credited_days")
    investigation_days = timeline.get("investigation_to_credited_days")
    submitted_to_credited_days = timeline.get("submitted_to_credited_days")
    is_credited = bool(timeline.get("is_credited"))
    if is_partially_credited:
        is_credited = False
    days_open = timeline.get("days_open")
    last_status_timestamp = timeline.get("last_status_timestamp")
    last_status_event_type = timeline.get("last_status_event_type")
    submitted_ts = timeline.get("submitted_to_billing_timestamp")
    days_pending_billing = timeline.get("days_pending_billing_to_credit")

    threshold_exceeded = False
    if isinstance(entered_days, (int, float)) and entered_days > threshold_days:
        threshold_exceeded = True
    if isinstance(investigation_days, (int, float)) and investigation_days > threshold_days:
        threshold_exceeded = True

    highlights = _extract_investigation_highlights(ticket, max_highlights=2)

    parts: list[str] = []
    parts.append(f"Ticket {normalized_ticket_id}")
    parts.append(f"Sales Reps: {_format_list_preview(sales_reps, max_items=6)}")
    parts.append(f"Account Prefixes: {_format_list_preview(account_prefixes, max_items=6)}")
    parts.append("")

    if supporting_roots:
        parts.append(
            f"Primary root cause: {primary_root}. Supporting causes: {', '.join(supporting_roots)}."
        )
    else:
        parts.append(f"Primary root cause: {primary_root}.")

    parts.append(f"Total credited amount is ${total_credit:,.2f} across {line_count} invoice lines.")
    if is_partially_credited:
        parts.append(
            f"Credit coverage is partially credited: {credited_line_count}/{line_count} lines have CR/RTN "
            f"(${credited_line_exposure:,.2f}) and {pending_line_count} line(s) remain pending "
            f"(${pending_line_exposure:,.2f})."
        )
    elif is_fully_credited:
        parts.append(
            f"Credit coverage is fully credited: {credited_line_count}/{line_count} lines have CR/RTN."
        )
    else:
        parts.append("Credit coverage is open: no credited lines detected yet.")

    if is_credited:
        if isinstance(entered_days, (int, float)) and isinstance(investigation_days, (int, float)):
            threshold_text = "exceeding" if threshold_exceeded else "not exceeding"
            parts.append(
                f"It took {entered_days:.2f} days from entry to credited and "
                f"{investigation_days:.2f} days from investigation to credited, "
                f"{threshold_text} the {threshold_days}-day aging threshold."
            )
        elif isinstance(entered_days, (int, float)):
            threshold_text = "exceeding" if entered_days > threshold_days else "not exceeding"
            parts.append(
                f"It took {entered_days:.2f} days from entry to credited, "
                f"{threshold_text} the {threshold_days}-day aging threshold."
            )
    else:
        pending_prefix = "The pending portion is still open. " if is_partially_credited else "The ticket is still open. "
        if submitted_ts and isinstance(days_pending_billing, (int, float)):
            open_text = ""
            if isinstance(days_open, (int, float)):
                open_text = f"It has been open for {days_open:.2f} days. "
            parts.append(
                f"{pending_prefix}{open_text}"
                f"It has been in billing since {submitted_ts}, pending to be credited for "
                f"{days_pending_billing:.2f} days."
            )
        elif last_status_timestamp and last_status_event_type:
            if isinstance(days_open, (int, float)):
                parts.append(
                    f"{pending_prefix}It has been open for {days_open:.2f} days. "
                    f"The last recorded status was {last_status_event_type} on {last_status_timestamp}."
                )
            else:
                parts.append(
                    f"{pending_prefix}"
                    f"The last recorded status was {last_status_event_type} on {last_status_timestamp}."
                )

    if highlights:
        highlight_text = " ".join(_clean_answer_text(h, max_len=180) for h in highlights[:2])
        parts.append(f"Key investigation highlights: {highlight_text}")

    parts.append(f"Invoices involved: {_format_list_preview(invoice_numbers, max_items=6)}")
    parts.append(f"Items involved: {_format_list_preview(item_numbers, max_items=6)}")

    answer = "\n".join(parts)

    return {
        "ticket_id": normalized_ticket_id,
        "primary_root_cause": primary_root,
        "supporting_root_causes": supporting_roots,
        "sales_reps": sales_reps,
        "account_prefixes": account_prefixes,
        "credit_total": total_credit,
        "line_count": line_count,
        "entered_to_credited_days": entered_days,
        "investigation_to_credited_days": investigation_days,
        "submitted_to_credited_days": submitted_to_credited_days,
        "days_open": days_open,
        "days_pending_billing_to_credit": days_pending_billing,
        "threshold_exceeded": threshold_exceeded,
        "is_credited": is_credited,
        "is_partially_credited": is_partially_credited,
        "credited_line_count": credited_line_count,
        "pending_line_count": pending_line_count,
        "credited_line_exposure": credited_line_exposure,
        "pending_line_exposure": pending_line_exposure,
        "last_status_timestamp": last_status_timestamp,
        "last_status_event_type": last_status_event_type,
        "invoice_numbers": invoice_numbers,
        "item_numbers": item_numbers,
        "investigation_highlights": highlights,
        "timeline_metrics": timeline,
        "answer": answer,
    }
