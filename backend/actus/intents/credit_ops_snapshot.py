import re
from collections import Counter
from functools import lru_cache
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from dateutil import parser

from app.rag.new_design.service import get_runtime_service
from app.rag.new_design.root_cause import load_root_cause_rules
from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "credit ops snapshot",
    "operations snapshot",
]

INDY_TZ = ZoneInfo("America/Indiana/Indianapolis")

ROOT_CAUSE_ALIASES = {
    "freight should not of been charged": "Freight should not have been charged",
}


TIMESTAMP_RE = re.compile(
    r"(?:\[(?P<bracketed>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\]|"
    r"(?P<plain>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}))"
)


def _parse_status_entries(status_text: str) -> list[tuple[str, str]]:
    if not status_text:
        return []
    matches = list(TIMESTAMP_RE.finditer(status_text))
    if not matches:
        return []
    entries = []
    for idx, match in enumerate(matches):
        ts = match.group("bracketed") or match.group("plain")
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(status_text)
        message = status_text[start:end].strip()
        message = re.sub(r"\s+", " ", message)
        entries.append((ts, message))
    return entries


def _extract_last_status(status_text: str) -> Tuple[Optional[str], str]:
    entries = _parse_status_entries(status_text)
    if not entries:
        return None, ""
    return entries[-1]


def _parse_window(query: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None, str | None]:
    q_low = query.lower()
    now = pd.Timestamp.now(tz=INDY_TZ)
    today = now.normalize()
    quarter_start_month = ((today.month - 1) // 3) * 3 + 1
    quarter_start = today.replace(month=quarter_start_month, day=1)

    m = re.search(r"\bbetween\s+(.+?)\s+(?:and|to)\s+(.+)", q_low)
    if m:
        raw_start = m.group(1).strip()
        raw_end = m.group(2).strip()
        try:
            dt_start = parser.parse(raw_start, fuzzy=True)
            dt_end = parser.parse(raw_end, fuzzy=True)
            start = pd.Timestamp(dt_start).tz_localize(INDY_TZ) if dt_start.tzinfo is None else pd.Timestamp(dt_start).tz_convert(INDY_TZ)
            end = pd.Timestamp(dt_end).tz_localize(INDY_TZ) if dt_end.tzinfo is None else pd.Timestamp(dt_end).tz_convert(INDY_TZ)
            return start.normalize(), end, f"between {start.date()} and {end.date()}"
        except Exception:
            pass

    m = re.search(r"\bfrom\s+(.+?)\s+to\s+(.+)", q_low)
    if m:
        raw_start = m.group(1).strip()
        raw_end = m.group(2).strip()
        try:
            dt_start = parser.parse(raw_start, fuzzy=True)
            dt_end = parser.parse(raw_end, fuzzy=True)
            start = pd.Timestamp(dt_start).tz_localize(INDY_TZ) if dt_start.tzinfo is None else pd.Timestamp(dt_start).tz_convert(INDY_TZ)
            end = pd.Timestamp(dt_end).tz_localize(INDY_TZ) if dt_end.tzinfo is None else pd.Timestamp(dt_end).tz_convert(INDY_TZ)
            return start.normalize(), end, f"from {start.date()} to {end.date()}"
        except Exception:
            pass

    m = re.search(r"\bsince\s+(.+)", q_low)
    if m:
        raw = m.group(1).strip()
        try:
            dt = parser.parse(raw, fuzzy=True)
            start = pd.Timestamp(dt).tz_localize(INDY_TZ) if dt.tzinfo is None else pd.Timestamp(dt).tz_convert(INDY_TZ)
            return start.normalize(), now, f"since {start.date()}"
        except Exception:
            pass

    if "today" in q_low:
        return today, now, "today"
    if "yesterday" in q_low:
        start = today - pd.Timedelta(days=1)
        end = today - pd.Timedelta(seconds=1)
        return start, end, "yesterday"

    m = re.search(r"last\s+(\d+)\s+day", q_low)
    if m:
        days = int(m.group(1))
        start = today - pd.Timedelta(days=days)
        return start, now, f"last {days} days"

    m = re.search(r"last\s+(\d+)\s+week", q_low)
    if m:
        weeks = int(m.group(1))
        start = today - pd.Timedelta(days=weeks * 7)
        return start, now, f"last {weeks} weeks"

    m = re.search(r"last\s+(\d+)\s+month", q_low)
    if m:
        months = int(m.group(1))
        start = today - pd.DateOffset(months=months)
        return start, now, f"last {months} months"

    m = re.search(r"(?:last|past)\s+(\d+)\s+week", q_low)
    if m:
        weeks = int(m.group(1))
        start = today - pd.Timedelta(days=weeks * 7)
        return start, now, f"last {weeks} weeks"

    m = re.search(r"(?:last|past)\s+(\d+)\s+month", q_low)
    if m:
        months = int(m.group(1))
        start = today - pd.DateOffset(months=months)
        return start, now, f"last {months} months"

    m = re.search(r"(?:from\s+)?(\d+)\s+months?\s+ago", q_low)
    if m:
        months = int(m.group(1))
        start = today - pd.DateOffset(months=months)
        return start, now, f"from {months} months ago"

    m = re.search(r"(?:from\s+)?(\d+)\s+weeks?(?:\s+ago)?", q_low)
    if m:
        weeks = int(m.group(1))
        start = today - pd.Timedelta(days=weeks * 7)
        label = "from {} weeks ago".format(weeks) if "ago" in m.group(0) else f"from {weeks} weeks"
        return start, now, label

    if "last week" in q_low:
        start = today - pd.Timedelta(days=7)
        return start, now, "last 7 days"

    if "last month" in q_low:
        start = today - pd.Timedelta(days=30)
        return start, now, "last 30 days"

    if "this week" in q_low or "current week" in q_low:
        start = today - pd.Timedelta(days=today.weekday())
        return start, now, "current week" if "current week" in q_low else "this week"

    if "this month" in q_low or "current month" in q_low:
        start = today.replace(day=1)
        return start, now, "current month" if "current month" in q_low else "this month"

    m = re.search(r"(?:last|past)\s+(\d+)\s+quarter", q_low)
    if m:
        quarters = int(m.group(1))
        start = today - pd.DateOffset(months=quarters * 3)
        return start, now, f"last {quarters} quarters"

    if "this quarter" in q_low or "current quarter" in q_low:
        return quarter_start, now, "current quarter" if "current quarter" in q_low else "this quarter"

    if "this year" in q_low or "current year" in q_low:
        start = today.replace(month=1, day=1)
        return start, now, "current year" if "current year" in q_low else "this year"

    m = re.search(r"\bon\s+([A-Za-z]{3,}\s+\d{1,2}(?:st|nd|rd|th)?)\b", q_low)
    if m:
        raw = m.group(1).strip()
        try:
            dt = parser.parse(raw, fuzzy=True)
            start = pd.Timestamp(dt).tz_localize(INDY_TZ) if dt.tzinfo is None else pd.Timestamp(dt).tz_convert(INDY_TZ)
            end = start + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            return start.normalize(), end, f"on {start.date()}"
        except Exception:
            pass

    return None, None, None


@lru_cache(maxsize=1)
def _root_cause_label_maps() -> tuple[dict[str, str], dict[str, str]]:
    by_id: dict[str, str] = {}
    by_label: dict[str, str] = {}
    try:
        for rule in load_root_cause_rules():
            rule_id = str(rule.id or "").strip().lower()
            label = str(rule.label or rule.id or "").strip()
            if not rule_id or not label:
                continue
            by_id[rule_id] = label
            by_label[re.sub(r"\s+", " ", label.lower())] = label
    except Exception:
        return {}, {}
    return by_id, by_label


def _normalize_root_cause(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    text = re.sub(r"[.]+$", "", text)
    text = re.sub(r"\s+", " ", text)
    if text in ROOT_CAUSE_ALIASES:
        return ROOT_CAUSE_ALIASES[text]

    by_id, by_label = _root_cause_label_maps()
    return by_id.get(text) or by_label.get(text)


def _localize_indy(series: pd.Series) -> pd.Series:
    if series.empty:
        return series
    try:
        tz = series.dt.tz
    except Exception:
        return series
    if tz is None:
        return series.dt.tz_localize(INDY_TZ)
    return series.dt.tz_convert(INDY_TZ)


def _normalize_ticket_id(value: object) -> str | None:
    ticket_id = str(value or "").strip().upper()
    if not ticket_id:
        return None
    if ticket_id.startswith("R") and not ticket_id.startswith("R-"):
        if ticket_id.startswith("R "):
            ticket_id = "R-" + ticket_id[2:]
        else:
            ticket_id = "R-" + ticket_id[1:]
    return ticket_id


def _norm_upper_text(value: object) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _canonical_value(payload: object, key: str, default: object = None) -> object:
    if isinstance(payload, dict):
        return payload.get(key, default)
    return getattr(payload, key, default)


def _iter_canonical_ticket_lines(ticket: object) -> list[object]:
    line_map = _canonical_value(ticket, "line_map", {}) or {}
    if not isinstance(line_map, dict):
        return []

    out: list[object] = []
    for line_group in line_map.values():
        if isinstance(line_group, list):
            out.extend(line_group)
        elif line_group is not None:
            out.append(line_group)
    return out


def _extract_root_causes_from_canonical_ticket(ticket: object) -> tuple[str | None, list[str]]:
    primary = _normalize_root_cause(
        _canonical_value(ticket, "root_cause_primary_label")
        or _canonical_value(ticket, "root_cause_primary_id")
    )
    if primary and primary.lower() == "unidentified":
        primary = None

    all_values = (
        _canonical_value(ticket, "root_cause_labels")
        or _canonical_value(ticket, "root_cause_ids")
        or []
    )
    if isinstance(all_values, str):
        all_values = [all_values]

    seen: list[str] = []
    for value in all_values:
        normalized = _normalize_root_cause(value)
        if not normalized or normalized.lower() == "unidentified" or normalized in seen:
            continue
        seen.append(normalized)

    if primary and primary not in seen:
        seen.insert(0, primary)
    return primary, seen


def _extract_root_causes_from_lines(lines: list[object]) -> tuple[str | None, list[str]]:
    primary_counts: Counter[str] = Counter()
    all_counts: Counter[str] = Counter()
    first_seen: dict[str, int] = {}

    def _remember(cause: str) -> None:
        if cause not in first_seen:
            first_seen[cause] = len(first_seen)

    for line in lines:
        primary = _normalize_root_cause(
            _canonical_value(line, "root_cause_primary_label")
            or _canonical_value(line, "root_cause_primary_id")
        )
        if primary and primary.lower() != "unidentified":
            primary_counts[primary] += 1
            _remember(primary)

        all_values = (
            _canonical_value(line, "root_cause_labels")
            or _canonical_value(line, "root_cause_ids")
            or []
        )
        if isinstance(all_values, str):
            all_values = [all_values]

        added_any = False
        for value in all_values:
            normalized = _normalize_root_cause(value)
            if not normalized or normalized.lower() == "unidentified":
                continue
            all_counts[normalized] += 1
            _remember(normalized)
            added_any = True

        if not added_any and primary and primary.lower() != "unidentified":
            all_counts[primary] += 1

    if not all_counts and not primary_counts:
        return None, []

    ordered_all = sorted(
        all_counts.keys(),
        key=lambda cause: (-all_counts[cause], first_seen[cause], cause),
    )
    primary = None
    if primary_counts:
        primary = sorted(
            primary_counts.keys(),
            key=lambda cause: (
                -primary_counts[cause],
                -all_counts.get(cause, 0),
                first_seen[cause],
                cause,
            ),
        )[0]
    elif ordered_all:
        primary = ordered_all[0]

    if primary and primary not in ordered_all:
        ordered_all.insert(0, primary)
    return primary, ordered_all


def _select_ticket_lines(
    ticket: object,
    *,
    invoice_number: str | None,
    item_number: str | None,
) -> list[object]:
    if not invoice_number and not item_number:
        return []

    matched: list[object] = []
    for line in _iter_canonical_ticket_lines(ticket):
        line_invoice = _norm_upper_text(_canonical_value(line, "invoice_number"))
        line_item = _norm_upper_text(_canonical_value(line, "item_number"))
        if invoice_number and line_invoice != invoice_number:
            continue
        if item_number and line_item != item_number:
            continue
        matched.append(line)
    return matched


def _extract_root_causes(rows: list[dict]) -> tuple[str | None, list[str]]:
    primary = None
    seen: list[str] = []
    for row in rows:
        meta = row.get("metadata") or {}
        if not isinstance(meta, dict):
            continue

        root_cause = (
            _normalize_root_cause(meta.get("root_cause"))
            or _normalize_root_cause(meta.get("root_cause_primary_label"))
            or _normalize_root_cause(meta.get("root_cause_primary_id"))
        )

        root_causes_all = (
            meta.get("root_causes_all")
            or meta.get("root_cause_labels")
            or meta.get("root_cause_ids")
        )
        if isinstance(root_causes_all, str):
            root_causes_all = [root_causes_all]
        if isinstance(root_causes_all, list):
            for item in root_causes_all:
                normalized = _normalize_root_cause(item)
                if normalized and normalized not in seen:
                    seen.append(str(normalized))
        if root_cause:
            root_cause = str(root_cause)
            if root_cause not in seen:
                seen.append(root_cause)
        kind = (row.get("chunk_type") or meta.get("chunk_type") or meta.get("event_type") or "").lower()
        if kind in {"summary", "ticket_summary"}:
            primary = root_cause or (seen[0] if seen else None)
            break
        if primary is None:
            primary = root_cause or (seen[0] if seen else None)
    return primary, seen


def _lookup_root_causes_from_store(ticket_series: pd.Series) -> pd.DataFrame:
    ticket_map: dict[str, list[int]] = {}
    for idx, raw in ticket_series.items():
        ticket_id = _normalize_ticket_id(raw)
        if not ticket_id:
            continue
        ticket_map.setdefault(ticket_id, []).append(idx)

    results = pd.DataFrame(
        {
            "Root Causes (Primary)": [None] * len(ticket_series),
            "Root Causes (All)": [None] * len(ticket_series),
            "Root Cause Mixed": [False] * len(ticket_series),
        },
        index=ticket_series.index,
    )
    if not ticket_map:
        return results

    store = None
    try:
        from app.rag.store import get_rag_store

        store = get_rag_store()
    except Exception:
        return results

    root_map: dict[str, tuple[str | None, list[str]]] = {}
    for ticket_id in ticket_map:
        try:
            rows = store.get_ticket_chunks(ticket_id)
        except Exception:
            continue
        primary, all_causes = _extract_root_causes(rows)
        if primary or all_causes:
            root_map[ticket_id] = (primary, all_causes)

    try:
        store.close()
    except Exception:
        pass

    for idx, raw in ticket_series.items():
        ticket_id = _normalize_ticket_id(raw)
        if not ticket_id:
            continue
        primary, all_causes = root_map.get(ticket_id, (None, []))
        results.at[idx, "Root Causes (Primary)"] = primary
        if all_causes:
            results.at[idx, "Root Causes (All)"] = ", ".join(all_causes)
            results.at[idx, "Root Cause Mixed"] = len(set(all_causes)) > 1

    return results


def _lookup_root_causes(
    ticket_series: pd.Series,
    invoice_series: pd.Series | None = None,
    item_series: pd.Series | None = None,
) -> pd.DataFrame:
    ticket_ids = {
        ticket_id
        for raw in ticket_series.tolist()
        if (ticket_id := _normalize_ticket_id(raw))
    }

    try:
        service = get_runtime_service(refresh=False, search_ready=False)
        canonical_tickets = service.get_canonical_tickets(required_ticket_ids=ticket_ids)
    except Exception:
        return _lookup_root_causes_from_store(ticket_series)

    if not isinstance(canonical_tickets, dict):
        return _lookup_root_causes_from_store(ticket_series)

    if ticket_ids and not ticket_ids.intersection(canonical_tickets.keys()):
        return _lookup_root_causes_from_store(ticket_series)

    results = pd.DataFrame(
        {
            "Root Causes (Primary)": [None] * len(ticket_series),
            "Root Causes (All)": [None] * len(ticket_series),
            "Root Cause Mixed": [False] * len(ticket_series),
        },
        index=ticket_series.index,
    )

    ticket_level_map: dict[str, tuple[str | None, list[str]]] = {}
    scoped_map: dict[tuple[str, str | None, str | None], tuple[str | None, list[str]]] = {}

    for ticket_id in ticket_ids:
        ticket = canonical_tickets.get(ticket_id)
        if ticket is None:
            continue
        ticket_level_map[ticket_id] = _extract_root_causes_from_canonical_ticket(ticket)

    for idx, raw in ticket_series.items():
        ticket_id = _normalize_ticket_id(raw)
        if not ticket_id:
            continue

        ticket = canonical_tickets.get(ticket_id)
        if ticket is None:
            continue

        invoice_number = None
        if invoice_series is not None and idx in invoice_series.index:
            invoice_number = _norm_upper_text(invoice_series.at[idx])

        item_number = None
        if item_series is not None and idx in item_series.index:
            item_number = _norm_upper_text(item_series.at[idx])

        cache_key = (ticket_id, invoice_number, item_number)
        if cache_key not in scoped_map:
            matched_lines = _select_ticket_lines(
                ticket,
                invoice_number=invoice_number,
                item_number=item_number,
            )
            if matched_lines:
                scoped_map[cache_key] = _extract_root_causes_from_lines(matched_lines)
            else:
                scoped_map[cache_key] = ticket_level_map.get(ticket_id, (None, []))

        primary, all_causes = scoped_map[cache_key]
        results.at[idx, "Root Causes (Primary)"] = primary
        if all_causes:
            results.at[idx, "Root Causes (All)"] = ", ".join(all_causes)
            results.at[idx, "Root Cause Mixed"] = len(set(all_causes)) > 1

    return results


def intent_credit_ops_snapshot(query: str, df: pd.DataFrame):
    q_low = query.lower()
    if not any(term in q_low for term in ["ops snapshot", "operations snapshot", "credit ops", "credito ops"]):
        return None

    df_use = df.copy()
    df_use["Date"] = pd.to_datetime(df_use.get("Date"), errors="coerce")

    start_window, end_window, window_label = _parse_window(query)
    if not (start_window and end_window):
        return (
            "What date range should I use for the credit ops snapshot? "
            "Try: today, yesterday, last 7 days, last month, or a custom range.",
            None,
            {"follow_up": {"intent": "credit_ops_snapshot", "prefix": "credit ops snapshot"}},
        )

    df_use["Date"] = _localize_indy(df_use["Date"])
    if getattr(df_use["Date"].dt, "tz", None) is None:
        start_cmp = start_window.tz_localize(None)
        end_cmp = end_window.tz_localize(None)
    else:
        start_cmp = start_window
        end_cmp = end_window

    df_use = df_use[
        df_use["Date"].notna() &
        (df_use["Date"] >= start_cmp) &
        (df_use["Date"] <= end_cmp)
    ].copy()

    cr = df_use.get("RTN_CR_No", pd.Series(index=df_use.index, dtype="object"))
    cr_clean = cr.astype(str).str.strip()
    df_use["CR_without_number"] = (
        cr.isna() |
        (cr_clean == "") |
        (cr_clean.str.lower().isin(["nan", "none", "null", "not assigned", "credit request no.:"]))
    )

    status_series = (
        df_use.get("Status", pd.Series(index=df_use.index, dtype="object"))
        .fillna("")
        .astype(str)
    )
    last_entries = status_series.apply(_extract_last_status)
    df_use["Last_Status_Time_Str"] = last_entries.apply(lambda item: item[0])
    df_use["Last_Status_Message"] = last_entries.apply(lambda item: item[1])
    df_use["Last_Status_Time"] = pd.to_datetime(
        df_use["Last_Status_Time_Str"], errors="coerce", utc=True
    )

    msg = df_use["Last_Status_Message"].fillna("").astype(str).str.lower()
    submitted_patterns = [
        r"\bsubmitted\b",
        r"\bsubmitted\b.*\bbilling\b",
        r"\bbilling\b",
        r"\bpending\b.*\bcr\b",
        r"\bawaiting\b.*\bcr\b",
        r"\bwaiting\b.*\bcr\b",
        r"\bcr\s*number\b",
        r"\bcredit\s*number\b",
    ]
    wip_patterns = [
        r"\bwip\b",
        r"\bin\s*progress\b",
        r"\bopen\b",
        r"\bnot\s*started\b",
        r"\bworking\b",
        r"\bprocessing\b",
        r"\bin\s*review\b",
        r"\breviewing\b",
    ]
    credited_patterns = [
        r"\bcredited\b",
        r"\bclosed\b",
        r"\bcompleted\b",
        r"\bdone\b",
        r"\bresolved\b",
        r"\bfinish(ed)?\b",
    ]

    def matches_any(text_series, patterns):
        pat = "(?:" + "|".join(patterns) + ")"
        return text_series.str.contains(pat, regex=True, na=False)

    is_submitted = matches_any(msg, submitted_patterns)
    is_credited = matches_any(msg, credited_patterns)
    is_wip = matches_any(msg, wip_patterns)

    df_use["Last_Status_Category"] = np.select(
        [is_credited, is_submitted, is_wip],
        ["Credited", "Submitted", "WIP"],
        default="WIP",
    )
    df_use.loc[
        (~df_use["CR_without_number"]) &
        (df_use["Last_Status_Category"].isin(["Submitted", "WIP"])),
        "Last_Status_Category",
    ] = "Credited"

    if "Root Causes" not in df_use.columns:
        df_use["Root Causes"] = None
    if "Root Causes (All)" not in df_use.columns:
        df_use["Root Causes (All)"] = None
    if "Root Cause Mixed" not in df_use.columns:
        df_use["Root Cause Mixed"] = False

    if "root_cause" in df_use.columns:
        df_use["Root Causes"] = df_use["Root Causes"].fillna(df_use["root_cause"])
    if "Root Cause" in df_use.columns:
        df_use["Root Causes"] = df_use["Root Causes"].fillna(df_use["Root Cause"])

    ticket_series = df_use.get("Ticket Number", pd.Series(index=df_use.index, dtype="object"))
    if not ticket_series.empty:
        rag_root_causes = _lookup_root_causes(
            ticket_series,
            df_use.get("Invoice Number"),
            df_use.get("Item Number"),
        )
        df_use["Root Causes"] = df_use["Root Causes"].fillna(rag_root_causes["Root Causes (Primary)"])
        df_use["Root Causes (All)"] = df_use["Root Causes (All)"].fillna(rag_root_causes["Root Causes (All)"])
        df_use["Root Cause Mixed"] = df_use["Root Cause Mixed"] | rag_root_causes["Root Cause Mixed"].fillna(False)
    df_use["Root Causes"] = df_use["Root Causes"].apply(_normalize_root_cause)

    cols = [
        "Date",
        "Ticket Number",
        "Customer Number",
        "Invoice Number",
        "Item Number",
        "QTY",
        "Unit Price",
        "Extended Price",
        "Corrected Unit Price",
        "Credit Request Total",
        "Credit Type",
        "Issue Type",
        "Reason for Credit",
        "Root Causes",
        "Root Causes (All)",
        "Root Cause Mixed",
        "Requested By",
        "Sales Rep",
        "CR_without_number",
        "Last_Status_Category",
        "Last_Status_Time",
        "Last_Status_Message",
        "RTN_CR_No",
        "Type",
    ]
    for col in cols:
        if col not in df_use.columns:
            df_use[col] = None

    out = (
        df_use[cols]
        .sort_values(["CR_without_number", "Last_Status_Time"], ascending=[False, False])
        .reset_index(drop=True)
    )

    date_series = pd.to_datetime(out.get("Date"), errors="coerce")
    try:
        date_series = date_series.dt.tz_localize(None)
    except Exception:
        pass
    out["Date"] = date_series.dt.strftime("%Y-%m-%d").fillna("")

    if out.empty:
        return f"I don't see any records in the window ({window_label})."

    credit_totals = pd.to_numeric(out.get("Credit Request Total"), errors="coerce").fillna(0.0)
    total_credited = format_money(credit_totals.sum())

    root_cause_series = (
        out.get("Root Causes", pd.Series(index=out.index, dtype="object"))
        .fillna("")
        .astype(str)
        .str.strip()
    )
    root_cause_summary = (
        pd.DataFrame({
            "root_cause": root_cause_series,
            "credit_total": credit_totals,
        })
        .loc[lambda frame: frame["root_cause"].ne("")]
        .groupby("root_cause", dropna=False)
        .agg(
            credit_total=("credit_total", "sum"),
            record_count=("root_cause", "size"),
        )
        .sort_values(["credit_total", "record_count"], ascending=[False, False])
    )
    primary_root_cause = (
        str(root_cause_summary.index[0])
        if not root_cause_summary.empty
        else "Unspecified"
    )

    preview = out.head(200).copy()
    message = "\n".join([
        "Credit ops snapshot:",
        f"- Window used: **{window_label}**",
        f"- Records found: **{len(out):,}**",
        f"- Total credited: **{total_credited}**",
        f"- Primary root cause: **{primary_root_cause}**",
        "",
        "Here is a preview of the results.",
    ])

    return (
        message,
        preview,
        {
            "show_table": True,
            "csv_filename": "credit_ops_snapshot.csv",
            "csv_rows": out,
            "csv_row_count": len(out),
            "columns": cols,
        },
    )
