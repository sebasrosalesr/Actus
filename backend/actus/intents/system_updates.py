import re
from typing import Any, Optional, Tuple

import pandas as pd

from actus.intents.credit_ops_snapshot import _parse_window
from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "system updates",
    "system updated",
    "system-updated records",
    "rtn updates",
    "system rtn updates",
]


TIMESTAMP_RE = re.compile(
    r"(?:\[(?P<bracketed>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\]|"
    r"(?P<plain>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}))"
)

_MANUAL_RTN_PATTERNS = (
    r"\bcredit numbers? provided\b",
    r"\bcredit numbers? sent(?:\s+out)?\b",
    r"\bcredit number sent\b",
    r"\bcredited manually\b",
    r"\bclosed\b.*\bcredit number\b",
    r"\bresolved\b.*\bcredit number\b",
)

_MANUAL_CLOSURE_PATTERNS = (
    r"\bcredit request received and approved\b",
    r"\bticket resolved\b",
    r"\bofficially closed\b",
    r"\bresolved and closed\b",
    r"\bresolved and will be closed\b",
    r"\bwill be closed automatically\b",
    r"\bcredit no(?:\.| number)?\s*&\s*reason\b",
)

_SYSTEM_VERIFICATION_PATTERNS = (
    r"\bcredit number verified\b",
    r"\bcr number verified\b",
    r"\bcredit processing completed\b",
    r"\bticket automatically closed by the system\b",
)

_SYSTEM_BACKFILL_PATTERNS = (
    r"\bbackfilled date\b",
)

_RESET_AFTER_TERMINAL_PATTERNS = (
    r"\bno credit number\b",
    r"\bnot available on the billing master\b",
    r"\bsubmitted to billing\b",
    r"\bthis credit was missed\b",
    r"\bwill be added to the macro\b",
)

_RESOLVED_ON_RE = re.compile(
    r"\bresolved on\s+([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,\s+\d{4})",
    re.IGNORECASE,
)
_CLOSED_ON_RE = re.compile(
    r"\bclosed on\s+([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,\s+\d{4})",
    re.IGNORECASE,
)

_MANUAL_EVENT_PRIORITY = {
    "manual_credit_number": 1,
    "manual_closure": 2,
}

_SYSTEM_EVENT_PRIORITY = {
    "system_update": 1,
}


def _has_value(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .replace({"nan": "", "none": "", "n/a": "", "na": ""})
        .ne("")
    )


def _naive_ts(value: pd.Timestamp | None) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        return ts.tz_localize(None)
    return ts


def _window_range_label(start: pd.Timestamp | None, end: pd.Timestamp | None) -> str | None:
    start_ts = _naive_ts(start)
    end_ts = _naive_ts(end)
    if start_ts is None:
        return None
    if end_ts is None:
        end_ts = pd.Timestamp.today().normalize()
    return f"{start_ts.date()} → {end_ts.date()}"


def _preview_suggestion(start: pd.Timestamp | None, end: pd.Timestamp | None) -> dict[str, str] | None:
    start_ts = _naive_ts(start)
    end_ts = _naive_ts(end)
    if start_ts is None:
        return None
    if end_ts is None:
        end_ts = pd.Timestamp.today().normalize()
    start_text = start_ts.strftime("%Y-%m-%d")
    end_text = end_ts.strftime("%Y-%m-%d")
    window = f"{start_text} → {end_text}"
    return {
        "id": "system_updates",
        "label": f"System RTN updates preview ({window})",
        "prefix": f"system rtn updates analysis from {start_text} to {end_text}",
    }


def _unique_non_empty(values: list[str], *, limit: int | None = None) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        cleaned = str(value or "").strip()
        if not cleaned:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
        if limit is not None and len(out) >= limit:
            break
    return out


def _record_key_series(frame: pd.DataFrame) -> pd.Series:
    key_cols = [
        column
        for column in [
            "Ticket Number",
            "Invoice Number",
            "Item Number",
            "Customer Number",
            "RTN_CR_No",
        ]
        if column in frame.columns
    ]
    if not key_cols:
        return pd.Series(frame.index.astype(str), index=frame.index, dtype="object")
    return frame[key_cols].apply(
        lambda row: "||".join("" if pd.isna(value) else str(value) for value in row),
        axis=1,
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
    if not status_text:
        return None, ""
    entries = _parse_status_entries(status_text)
    if not entries:
        return None, ""
    return entries[-1]


def _parse_embedded_status_date(value: str) -> Optional[str]:
    cleaned = re.sub(r"(\d)(st|nd|rd|th)", r"\1", str(value or "").strip(), flags=re.IGNORECASE)
    parsed = pd.to_datetime(cleaned, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).date().isoformat()


def _extract_referenced_dates(message: str) -> tuple[Optional[str], Optional[str]]:
    resolved_match = _RESOLVED_ON_RE.search(message or "")
    closed_match = _CLOSED_ON_RE.search(message or "")
    resolved_date = _parse_embedded_status_date(resolved_match.group(1)) if resolved_match else None
    closed_date = _parse_embedded_status_date(closed_match.group(1)) if closed_match else None
    return resolved_date, closed_date


def _classify_status_entry(message: str) -> Optional[str]:
    msg_low = str(message or "").lower().strip()
    if not msg_low:
        return None
    if "updated by the system" in msg_low:
        return "system_update"
    if any(re.search(pattern, msg_low) for pattern in _SYSTEM_BACKFILL_PATTERNS):
        return None
    if any(re.search(pattern, msg_low) for pattern in _SYSTEM_VERIFICATION_PATTERNS):
        return None
    if any(re.search(pattern, msg_low) for pattern in _MANUAL_CLOSURE_PATTERNS):
        return "manual_closure"
    if any(re.search(pattern, msg_low) for pattern in _MANUAL_RTN_PATTERNS):
        return "manual_credit_number"
    return None


def _is_reset_after_terminal(message: str) -> bool:
    msg_low = str(message or "").lower().strip()
    if not msg_low:
        return False
    return any(re.search(pattern, msg_low) for pattern in _RESET_AFTER_TERMINAL_PATTERNS)


def _classified_status_entries(
    status_text: str,
) -> list[tuple[pd.Timestamp, int, str, str, str, Optional[str], Optional[str]]]:
    entries = _parse_status_entries(status_text)
    classified: list[tuple[pd.Timestamp, int, str, str, str, Optional[str], Optional[str]]] = []
    for ts_text, msg in entries:
        event_type = _classify_status_entry(msg)
        if not event_type:
            continue
        ts = pd.to_datetime(ts_text, errors="coerce")
        if pd.isna(ts):
            continue
        if event_type.startswith("manual"):
            priority = _MANUAL_EVENT_PRIORITY.get(event_type, 0)
        else:
            priority = _SYSTEM_EVENT_PRIORITY.get(event_type, 0)
        resolved_date, closed_date = _extract_referenced_dates(msg)
        classified.append((pd.Timestamp(ts), priority, ts_text, msg.strip(), event_type, resolved_date, closed_date))
    return classified


def _extract_best_status_event(
    status_text: str,
    *,
    allowed_types: tuple[str, ...],
    stop_after_manual_terminal: bool = False,
) -> tuple[Optional[str], str, str, Optional[str], Optional[str]]:
    if not status_text:
        return None, "", "", None, None
    entries = _classified_status_entries(status_text)
    if not entries:
        return None, "", "", None, None

    terminal_manual_ts: Optional[pd.Timestamp] = None
    if stop_after_manual_terminal:
        manual_terminal_entries = [item for item in entries if item[4] in {"manual_credit_number", "manual_closure"}]
        if manual_terminal_entries:
            terminal_manual_ts = max(item[0] for item in manual_terminal_entries)

    candidates: list[tuple[pd.Timestamp, int, str, str, str, Optional[str], Optional[str]]] = []
    for item in entries:
        if item[4] not in allowed_types:
            continue
        if terminal_manual_ts is not None and item[0] > terminal_manual_ts:
            continue
        candidates.append(item)

    if not candidates:
        return None, "", "", None, None

    candidates.sort(key=lambda item: (item[0], item[1]))
    _ts, _priority, ts_text, message, event_type, resolved_date, closed_date = candidates[-1]
    return ts_text, message, event_type, resolved_date, closed_date


def _extract_system_update(status_text: str) -> tuple[Optional[str], str, str, Optional[str], Optional[str]]:
    if not status_text:
        return None, "", "", None, None
    entries = _parse_status_entries(status_text)
    if not entries:
        return None, "", "", None, None

    terminal_manual_active = False
    best_candidate: tuple[Optional[str], str, str, Optional[str], Optional[str]] = (None, "", "", None, None)

    for ts_text, msg in entries:
        event_type = _classify_status_entry(msg)
        if _is_reset_after_terminal(msg):
            terminal_manual_active = False
            continue
        if event_type in {"manual_credit_number", "manual_closure"}:
            terminal_manual_active = True
            continue
        if event_type != "system_update":
            continue
        if terminal_manual_active:
            continue
        best_candidate = (ts_text, msg.strip(), event_type, None, None)

    return best_candidate


def _extract_manual_rtn_update(status_text: str) -> tuple[Optional[str], str, str, Optional[str], Optional[str]]:
    return _extract_best_status_event(
        status_text,
        allowed_types=("manual_credit_number", "manual_closure"),
    )


def _reopened_after_manual_terminal(status_text: str) -> bool:
    entries = _parse_status_entries(status_text)
    if not entries:
        return False

    terminal_manual_active = False
    for _ts_text, msg in entries:
        if _is_reset_after_terminal(msg):
            if terminal_manual_active:
                return True
            continue
        event_type = _classify_status_entry(msg)
        if event_type in {"manual_credit_number", "manual_closure"}:
            terminal_manual_active = True
    return False


def _primary_update_source(
    manual_time: pd.Timestamp | None,
    system_time: pd.Timestamp | None,
    *,
    reopened_after_terminal: bool,
) -> str:
    manual_ts = _naive_ts(manual_time)
    system_ts = _naive_ts(system_time)

    if manual_ts is not None and system_ts is not None:
        if reopened_after_terminal and system_ts > manual_ts:
            return "system"
        return "system" if system_ts <= manual_ts else "manual"
    if system_ts is not None:
        return "system"
    if manual_ts is not None:
        return "manual"
    return ""


def _outlier_mask(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.dropna().empty:
        return pd.Series(False, index=series.index)

    q1 = numeric.quantile(0.25)
    q3 = numeric.quantile(0.75)
    iqr = q3 - q1
    if pd.notna(iqr) and iqr > 0:
        cutoff = q3 + 1.5 * iqr
        return numeric > cutoff

    std = float(numeric.std() or 0.0)
    mean = float(numeric.mean() or 0.0)
    if std <= 0:
        return pd.Series(False, index=series.index)
    return numeric > (mean + 2.0 * std)


def _source_summary(frame: pd.DataFrame, source: str) -> dict[str, Any]:
    subset = frame[frame["Update Source"].eq(source)].copy()
    if subset.empty:
        return {
            "record_count": 0,
            "credit_total": 0.0,
            "avg_days_to_update": 0.0,
            "median_days_to_update": 0.0,
            "outlier_count": 0,
            "outlier_ticket_ids": [],
            "batch_dates": 0,
            "batched_dates": 0,
            "batched_records": 0,
            "batched_credit_total": 0.0,
            "largest_batch_count": 0,
            "largest_batch_date": "N/A",
            "largest_batch_credit_total": 0.0,
        }

    credit_total = float(pd.to_numeric(subset["Credit Request Total"], errors="coerce").fillna(0.0).sum())
    valid_days = pd.to_numeric(subset["Days To RTN Update"], errors="coerce").dropna()
    avg_days = float(valid_days.mean() or 0.0) if not valid_days.empty else 0.0
    median_days = float(valid_days.median() or 0.0) if not valid_days.empty else 0.0

    subset["RTN Update Outlier"] = _outlier_mask(subset["Days To RTN Update"]).fillna(False)
    outliers = subset[subset["RTN Update Outlier"]].copy()
    outlier_tickets = (
        outliers.get("Ticket Number", pd.Series(index=outliers.index, dtype="object"))
        .fillna("")
        .astype(str)
        .str.strip()
    )
    outlier_ticket_ids = _unique_non_empty(outlier_tickets.tolist(), limit=5)
    outlier_ticket_count = len(_unique_non_empty(outlier_tickets.tolist()))

    batch_counts = subset.groupby("Update Batch Date").size().sort_index(ascending=False)
    batch_totals = (
        subset.groupby("Update Batch Date")["Credit Request Total"]
        .sum()
        .sort_index(ascending=False)
    )
    batched_dates = int((batch_counts > 1).sum()) if not batch_counts.empty else 0
    batched_records = int(batch_counts[batch_counts > 1].sum()) if not batch_counts.empty else 0
    batched_credit_total = float(batch_totals[batch_counts > 1].sum()) if not batch_totals.empty else 0.0
    largest_batch_count = int(batch_counts.max()) if not batch_counts.empty else 0
    largest_batch_date = str(batch_counts.idxmax()) if not batch_counts.empty else "N/A"
    largest_batch_credit_total = (
        float(batch_totals.loc[batch_counts.idxmax()] or 0.0) if not batch_counts.empty else 0.0
    )

    subset["Batch Update Count"] = subset["Update Batch Date"].map(batch_counts).fillna(1).astype(int)
    subset["Batch Credit Total"] = (
        subset["Update Batch Date"].map(batch_totals).fillna(subset["Credit Request Total"]).astype(float)
    )

    return {
        "record_count": int(len(subset.index)),
        "credit_total": credit_total,
        "avg_days_to_update": avg_days,
        "median_days_to_update": median_days,
        "outlier_count": outlier_ticket_count,
        "outlier_ticket_ids": outlier_ticket_ids,
        "batch_dates": int(len(batch_counts.index)),
        "batched_dates": batched_dates,
        "batched_records": batched_records,
        "batched_credit_total": batched_credit_total,
        "largest_batch_count": largest_batch_count,
        "largest_batch_date": largest_batch_date,
        "largest_batch_credit_total": largest_batch_credit_total,
        "rows": subset,
    }


def intent_system_updates(query: str, df: pd.DataFrame):
    q_low = query.lower()
    if "system" not in q_low and "rtn" not in q_low:
        return None
    if not any(term in q_low for term in ["status", "updated", "update", "billing", "sync", "analysis", "rtn"]):
        return None

    if "Status" not in df.columns:
        return "I can't check system RTN updates because `Status` is missing."
    if "RTN_CR_No" not in df.columns:
        return "I can't check system RTN updates because `RTN_CR_No` is missing."

    df_use = df.copy()
    status_series = df_use.get("Status", pd.Series(index=df_use.index, dtype="object")).fillna("").astype(str)

    system_updates = status_series.apply(_extract_system_update)
    manual_updates = status_series.apply(_extract_manual_rtn_update)
    last_statuses = status_series.apply(_extract_last_status)
    df_use["System_Update_Time_Str"] = system_updates.apply(lambda item: item[0])
    df_use["System_Update_Message"] = system_updates.apply(lambda item: item[1])
    df_use["System_Update_Type"] = system_updates.apply(lambda item: item[2])
    df_use["System_Resolved_Date"] = system_updates.apply(lambda item: item[3])
    df_use["System_Closed_Date"] = system_updates.apply(lambda item: item[4])
    df_use["System_Update_Time"] = pd.to_datetime(df_use["System_Update_Time_Str"], errors="coerce")
    df_use["Manual_Update_Time_Str"] = manual_updates.apply(lambda item: item[0])
    df_use["Manual_Update_Message"] = manual_updates.apply(lambda item: item[1])
    df_use["Manual_Update_Type"] = manual_updates.apply(lambda item: item[2])
    df_use["Manual_Resolved_Date"] = manual_updates.apply(lambda item: item[3])
    df_use["Manual_Closed_Date"] = manual_updates.apply(lambda item: item[4])
    df_use["Manual_Update_Time"] = pd.to_datetime(df_use["Manual_Update_Time_Str"], errors="coerce")
    df_use["Reopened_After_Terminal"] = status_series.apply(_reopened_after_manual_terminal)
    df_use["Primary_Update_Source"] = df_use.apply(
        lambda row: _primary_update_source(
            row.get("Manual_Update_Time"),
            row.get("System_Update_Time"),
            reopened_after_terminal=bool(row.get("Reopened_After_Terminal")),
        ),
        axis=1,
    )
    df_use["Last_Status_Time_Str"] = last_statuses.apply(lambda item: item[0])
    df_use["Last_Status_Message"] = last_statuses.apply(lambda item: item[1])
    df_use["Last_Status_Time"] = pd.to_datetime(df_use["Last_Status_Time_Str"], errors="coerce")

    rtn_mask = _has_value(df_use["RTN_CR_No"])
    system_rows = df_use[rtn_mask & df_use["System_Update_Time"].notna()].copy()
    system_rows["Update Source"] = "system"
    system_rows["Update Event Time"] = system_rows["System_Update_Time"]
    system_rows["Update Event Message"] = system_rows["System_Update_Message"]
    system_rows["Update Event Type"] = system_rows["System_Update_Type"]
    system_rows["Mentioned Resolved Date"] = system_rows["System_Resolved_Date"]
    system_rows["Mentioned Closed Date"] = system_rows["System_Closed_Date"]
    system_rows["Primary Update Source"] = system_rows["Primary_Update_Source"]
    system_rows["Reopened After Terminal"] = system_rows["Reopened_After_Terminal"]

    manual_rows = df_use[rtn_mask & df_use["Manual_Update_Time"].notna()].copy()
    manual_rows["Update Source"] = "manual"
    manual_rows["Update Event Time"] = manual_rows["Manual_Update_Time"]
    manual_rows["Update Event Message"] = manual_rows["Manual_Update_Message"]
    manual_rows["Update Event Type"] = manual_rows["Manual_Update_Type"]
    manual_rows["Mentioned Resolved Date"] = manual_rows["Manual_Resolved_Date"]
    manual_rows["Mentioned Closed Date"] = manual_rows["Manual_Closed_Date"]
    manual_rows["Primary Update Source"] = manual_rows["Primary_Update_Source"]
    manual_rows["Reopened After Terminal"] = manual_rows["Reopened_After_Terminal"]

    covered_idx = set(system_rows.index) | set(manual_rows.index)
    fallback_mask = rtn_mask & ~df_use.index.isin(covered_idx)
    fallback_rows = df_use[fallback_mask].copy()
    if not fallback_rows.empty:
        fallback_time = fallback_rows["Last_Status_Time"].where(
            fallback_rows["Last_Status_Time"].notna(),
            other=pd.to_datetime(fallback_rows.get("Date"), errors="coerce"),
        )
        fallback_rows = fallback_rows[fallback_time.notna()].copy()
        if not fallback_rows.empty:
            fallback_time = fallback_time[fallback_rows.index]
            fallback_rows["Update Source"] = "unknown"
            fallback_rows["Update Event Time"] = fallback_time
            fallback_rows["Update Event Message"] = fallback_rows["Last_Status_Message"].fillna("")
            fallback_rows["Update Event Type"] = ""
            fallback_rows["Mentioned Resolved Date"] = None
            fallback_rows["Mentioned Closed Date"] = None
            fallback_rows["Primary Update Source"] = fallback_rows["Primary_Update_Source"]
            fallback_rows["Reopened After Terminal"] = fallback_rows["Reopened_After_Terminal"]

    filtered = pd.concat(
        [system_rows, manual_rows, fallback_rows if not fallback_rows.empty else pd.DataFrame()],
        ignore_index=True,
        sort=False,
    )
    if not filtered.empty:
        dedupe_subset = [
            column
            for column in [
                "Ticket Number",
                "Invoice Number",
                "Item Number",
                "RTN_CR_No",
                "Update Source",
                "Update Event Time",
                "Update Event Message",
            ]
            if column in filtered.columns
        ]
        filtered = filtered.drop_duplicates(subset=dedupe_subset, keep="last")
        filtered = filtered[filtered["Update Event Time"].notna()].copy()

    start, end, window_label = _parse_window(query)
    start_naive = _naive_ts(start)
    end_naive = _naive_ts(end)
    if start_naive is not None and end_naive is not None:
        filtered = filtered[filtered["Update Event Time"].between(start_naive, end_naive)].copy()
    elif start_naive is not None:
        filtered = filtered[filtered["Update Event Time"] >= start_naive].copy()

    if filtered.empty:
        label = window_label or "that window"
        return f"I don't see any RTN updates with a credit number for {label}."

    filtered["Credit Request Total"] = pd.to_numeric(filtered.get("Credit Request Total"), errors="coerce").fillna(0.0)
    if "Date" in filtered.columns:
        filtered["Date"] = pd.to_datetime(filtered["Date"], errors="coerce")
        filtered["Days To RTN Update"] = (
            filtered["Update Event Time"] - filtered["Date"]
        ).dt.total_seconds() / 86400.0
    else:
        filtered["Date"] = pd.NaT
        filtered["Days To RTN Update"] = pd.NA

    filtered["Update Batch Date"] = filtered["Update Event Time"].dt.date
    filtered["RTN Update Outlier"] = False
    filtered["_Record_Key"] = _record_key_series(filtered)
    source_counts = filtered.groupby("_Record_Key")["Update Source"].nunique()
    filtered["_Update_Source_Count"] = filtered["_Record_Key"].map(source_counts).fillna(1).astype(int)
    filtered["Update Mix Status"] = filtered.apply(
        lambda row: (
            "mixed"
            if int(row["_Update_Source_Count"] or 0) > 1
            else ("system_only" if str(row.get("Update Source") or "").strip().lower() == "system" else "manual_only")
        ),
        axis=1,
    )

    system_summary = _source_summary(filtered, "system")
    manual_summary = _source_summary(filtered, "manual")
    unknown_count = int((filtered.get("Update Source", pd.Series(dtype="object")) == "unknown").sum())

    if isinstance(system_summary.get("rows"), pd.DataFrame) and not system_summary["rows"].empty:
        filtered.loc[system_summary["rows"].index, "Batch Update Count"] = system_summary["rows"]["Batch Update Count"]
        filtered.loc[system_summary["rows"].index, "Batch Credit Total"] = system_summary["rows"]["Batch Credit Total"]
        filtered.loc[system_summary["rows"].index, "RTN Update Outlier"] = system_summary["rows"]["RTN Update Outlier"]
    if isinstance(manual_summary.get("rows"), pd.DataFrame) and not manual_summary["rows"].empty:
        filtered.loc[manual_summary["rows"].index, "Batch Update Count"] = manual_summary["rows"]["Batch Update Count"]
        filtered.loc[manual_summary["rows"].index, "Batch Credit Total"] = manual_summary["rows"]["Batch Credit Total"]
        filtered.loc[manual_summary["rows"].index, "RTN Update Outlier"] = manual_summary["rows"]["RTN Update Outlier"]

    if "Batch Update Count" not in filtered.columns:
        filtered["Batch Update Count"] = 1
    if "Batch Credit Total" not in filtered.columns:
        filtered["Batch Credit Total"] = filtered["Credit Request Total"]
    filtered["Batch Update Count"] = pd.to_numeric(filtered["Batch Update Count"], errors="coerce").fillna(1).astype(int)
    filtered["Batch Credit Total"] = pd.to_numeric(filtered["Batch Credit Total"], errors="coerce").fillna(filtered["Credit Request Total"]).astype(float)
    filtered["RTN Update Outlier"] = filtered["RTN Update Outlier"].fillna(False).astype(bool)
    filtered["Days To System Credit"] = filtered["Days To RTN Update"]
    filtered["System Update Outlier"] = filtered["RTN Update Outlier"]
    filtered = filtered.drop(columns=["_Record_Key", "_Update_Source_Count"], errors="ignore")

    filtered = filtered.sort_values(
        ["RTN Update Outlier", "Days To RTN Update", "Update Event Time"],
        ascending=[False, False, False],
    )

    cols = [
        "Date",
        "Ticket Number",
        "Invoice Number",
        "Item Number",
        "Customer Number",
        "Credit Request Total",
        "RTN_CR_No",
        "Update Source",
        "Update Mix Status",
        "Update Event Type",
        "Primary Update Source",
        "Reopened After Terminal",
        "Update Event Time",
        "Mentioned Resolved Date",
        "Mentioned Closed Date",
        "Days To RTN Update",
        "RTN Update Outlier",
        "Batch Update Count",
        "Batch Credit Total",
        "Last_Status_Time",
        "Update Event Message",
        "Days To System Credit",
        "System Update Outlier",
        "Status",
    ]
    for col in cols:
        if col not in filtered.columns:
            filtered[col] = None
    preview = filtered[cols].head(200).copy()

    if start_naive is not None:
        resolved_window = _window_range_label(start_naive, end_naive) or "the selected window"
    else:
        min_update = filtered["Update Event Time"].min()
        max_update = filtered["Update Event Time"].max()
        resolved_window = f"{min_update.date()} → {max_update.date()}" if pd.notna(min_update) and pd.notna(max_update) else "all available updates"

    suggestions = []
    preview_suggestion = _preview_suggestion(start_naive, end_naive)
    if preview_suggestion is not None:
        suggestions.append(preview_suggestion)

    system_outlier_line = "- System outlier tickets: **0**"
    if system_summary["outlier_ticket_ids"]:
        system_outlier_line = (
            f"- System outlier tickets: **{system_summary['outlier_count']}** "
            f"({', '.join(system_summary['outlier_ticket_ids'])})"
        )

    unknown_lines: list[str] = []
    if unknown_count > 0:
        unknown_lines = [
            "",
            f"RTN/CR records with no parseable status event (using last status timestamp as event time): **{unknown_count}**",
        ]

    manual_lines: list[str] = []
    if manual_summary["record_count"] > 0:
        manual_outlier_line = "- Manual outlier tickets: **0**"
        if manual_summary["outlier_ticket_ids"]:
            manual_outlier_line = (
                f"- Manual outlier tickets: **{manual_summary['outlier_count']}** "
                f"({', '.join(manual_summary['outlier_ticket_ids'])})"
            )
        manual_lines = [
            "",
            "Manual RTN / closure updates captured in status history:",
            f"- Manual records with RTN/CR: **{manual_summary['record_count']:,}** / **{format_money(manual_summary['credit_total'])}**",
            f"- Avg days from entry to manual RTN update: **{manual_summary['avg_days_to_update']:.1f}**",
            f"- Median days from entry to manual RTN update: **{manual_summary['median_days_to_update']:.1f}**",
            manual_outlier_line,
            f"- Manual batch dates: **{manual_summary['batch_dates']}** total, **{manual_summary['batched_dates']}** with multi-record batches affecting **{manual_summary['batched_records']}** record(s) / **{format_money(manual_summary['batched_credit_total'])}**",
            f"- Largest manual batch: **{manual_summary['largest_batch_count']}** record(s) on **{manual_summary['largest_batch_date']}** / **{format_money(manual_summary['largest_batch_credit_total'])}**",
        ]

    message = "\n".join(
        [
            "System RTN updates analysis:",
            f"- Window used: **{resolved_window}**",
            f"- System-updated records with RTN/CR: **{system_summary['record_count']:,}** / **{format_money(system_summary['credit_total'])}**",
            f"- Avg days from entry to system credit: **{system_summary['avg_days_to_update']:.1f}**",
            f"- Median days from entry to system credit: **{system_summary['median_days_to_update']:.1f}**",
            system_outlier_line,
            f"- Batch update dates: **{system_summary['batch_dates']}** total, **{system_summary['batched_dates']}** with multi-record batches affecting **{system_summary['batched_records']}** record(s) / **{format_money(system_summary['batched_credit_total'])}**",
            f"- Largest batch: **{system_summary['largest_batch_count']}** record(s) on **{system_summary['largest_batch_date']}** / **{format_money(system_summary['largest_batch_credit_total'])}**",
            *manual_lines,
            *unknown_lines,
            "",
            "Here is a preview of the results.",
        ]
    )

    return (
        message,
        preview,
        {
            "show_table": True,
            "suggestions": suggestions,
            "csv_filename": "system_rtn_updates_analysis.csv",
            "csv_rows": filtered[cols],
            "csv_row_count": len(filtered),
            "columns": cols,
            "system_updates_summary": {
                "window": resolved_window,
                "total_records": int(system_summary["record_count"]),
                "credit_total": system_summary["credit_total"],
                "avg_days_to_system_credit": system_summary["avg_days_to_update"],
                "median_days_to_system_credit": system_summary["median_days_to_update"],
                "outlier_count": system_summary["outlier_count"],
                "outlier_ticket_ids": system_summary["outlier_ticket_ids"],
                "batch_dates": system_summary["batch_dates"],
                "batched_dates": system_summary["batched_dates"],
                "batched_records": system_summary["batched_records"],
                "batched_credit_total": system_summary["batched_credit_total"],
                "largest_batch_count": system_summary["largest_batch_count"],
                "largest_batch_date": system_summary["largest_batch_date"],
                "largest_batch_credit_total": system_summary["largest_batch_credit_total"],
                "manual_record_count": int(manual_summary["record_count"]),
                "manual_credit_total": manual_summary["credit_total"],
                "manual_avg_days_to_update": manual_summary["avg_days_to_update"],
                "manual_median_days_to_update": manual_summary["median_days_to_update"],
                "manual_outlier_count": manual_summary["outlier_count"],
                "manual_outlier_ticket_ids": manual_summary["outlier_ticket_ids"],
                "manual_batch_dates": manual_summary["batch_dates"],
                "manual_batched_dates": manual_summary["batched_dates"],
                "manual_batched_records": manual_summary["batched_records"],
                "manual_batched_credit_total": manual_summary["batched_credit_total"],
                "manual_largest_batch_count": manual_summary["largest_batch_count"],
                "manual_largest_batch_date": manual_summary["largest_batch_date"],
                "manual_largest_batch_credit_total": manual_summary["largest_batch_credit_total"],
                "unknown_record_count": unknown_count,
                "preview_total_records": int(len(filtered)),
            },
        },
    )
