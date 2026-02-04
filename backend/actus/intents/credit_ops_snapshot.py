import re
from zoneinfo import ZoneInfo
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from dateutil import parser

from app.rag.store import get_rag_store

INTENT_ALIASES = [
    "credit ops snapshot",
    "operations snapshot",
]

INDY_TZ = ZoneInfo("America/Indiana/Indianapolis")

ROOT_CAUSE_LABELS = [
    "Item should be PPD",
    "Item not price matched when subbing",
    "Freight should not of been charged",
    "Item invoiced after price change",
    "Price discrepancy",
]
ROOT_CAUSE_CANONICAL = {
    re.sub(r"\s+", " ", label.strip().lower()): label
    for label in ROOT_CAUSE_LABELS
}
ROOT_CAUSE_ALIASES = {
    "freight should not have been charged": "Freight should not of been charged",
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

    if "this week" in q_low:
        start = today - pd.Timedelta(days=today.weekday())
        return start, now, "this week"

    if "this month" in q_low:
        start = today.replace(day=1)
        return start, now, "this month"

    if "this year" in q_low:
        start = today.replace(month=1, day=1)
        return start, now, "this year"

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


def _normalize_root_cause(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    text = re.sub(r"[.]+$", "", text)
    text = re.sub(r"\s+", " ", text)
    return ROOT_CAUSE_ALIASES.get(text) or ROOT_CAUSE_CANONICAL.get(text)


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


def _extract_root_cause(rows: list[dict]) -> str | None:
    best = None
    for row in rows:
        meta = row.get("metadata") or {}
        if not isinstance(meta, dict):
            continue
        root_cause = _normalize_root_cause(meta.get("root_cause"))
        if not root_cause:
            continue
        kind = (row.get("chunk_type") or meta.get("chunk_type") or meta.get("event_type") or "").lower()
        if kind in {"summary", "ticket_summary"}:
            return str(root_cause)
        if best is None:
            best = str(root_cause)
    return best


def _lookup_root_causes(ticket_series: pd.Series) -> pd.Series:
    ticket_map: dict[str, list[int]] = {}
    for idx, raw in ticket_series.items():
        ticket_id = _normalize_ticket_id(raw)
        if not ticket_id:
            continue
        ticket_map.setdefault(ticket_id, []).append(idx)

    results = pd.Series([None] * len(ticket_series), index=ticket_series.index, dtype="object")
    if not ticket_map:
        return results

    store = None
    try:
        store = get_rag_store()
    except Exception:
        return results

    root_map: dict[str, str] = {}
    for ticket_id in ticket_map:
        try:
            rows = store.get_ticket_chunks(ticket_id)
        except Exception:
            continue
        root_cause = _extract_root_cause(rows)
        if root_cause:
            root_map[ticket_id] = root_cause

    try:
        store.close()
    except Exception:
        pass

    for idx, raw in ticket_series.items():
        ticket_id = _normalize_ticket_id(raw)
        if not ticket_id:
            continue
        results.at[idx] = root_map.get(ticket_id)

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

    if "root_cause" in df_use.columns:
        df_use["Root Causes"] = df_use["Root Causes"].fillna(df_use["root_cause"])
    if "Root Cause" in df_use.columns:
        df_use["Root Causes"] = df_use["Root Causes"].fillna(df_use["Root Cause"])

    ticket_series = df_use.get("Ticket Number", pd.Series(index=df_use.index, dtype="object"))
    if not ticket_series.empty:
        rag_root_causes = _lookup_root_causes(ticket_series)
        df_use["Root Causes"] = df_use["Root Causes"].fillna(rag_root_causes)
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

    preview = out.head(200).copy()
    message = "\n".join([
        "Credit ops snapshot:",
        f"- Window used: **{window_label}**",
        f"- Records found: **{len(out):,}**",
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
