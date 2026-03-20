import re
from typing import Optional, Tuple
import pandas as pd

from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "system updates",
    "system updated",
    "system-updated records",
]


TIMESTAMP_RE = re.compile(
    r"(?:\[(?P<bracketed>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\]|"
    r"(?P<plain>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}))"
)


def _has_value(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .replace({"nan": "", "none": "", "n/a": "", "na": ""})
        .ne("")
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


def _extract_system_update(status_text: str) -> Tuple[Optional[str], str]:
    if not status_text:
        return None, ""
    entries = _parse_status_entries(status_text)
    if not entries:
        return None, ""
    for ts, msg in reversed(entries):
        if "updated by the system" in msg.lower():
            return ts, msg.strip()
    return None, ""


def intent_system_updates(query: str, df: pd.DataFrame):
    """
    Find records where the last status was updated by the system
    and the record has an RTN/CR number.
    """
    q_low = query.lower()
    if "system" not in q_low:
        return None
    if not any(term in q_low for term in ["status", "updated", "update", "billing", "sync"]):
        return None

    if "Status" not in df.columns:
        return "I can't check system updates because `Status` is missing."
    if "RTN_CR_No" not in df.columns:
        return "I can't check system updates because `RTN_CR_No` is missing."

    df_use = df.copy()
    status_series = (
        df_use.get("Status", pd.Series(index=df_use.index, dtype="object"))
        .fillna("")
        .astype(str)
    )

    system_updates = status_series.apply(_extract_system_update)
    df_use["Last_Status_Time_Str"] = system_updates.apply(lambda item: item[0])
    df_use["Last_Status_Message"] = system_updates.apply(lambda item: item[1])
    df_use["Last_Status_Time"] = pd.to_datetime(
        df_use["Last_Status_Time_Str"], errors="coerce"
    )

    system_mask = status_series.str.contains("updated by the system", case=False, na=False)
    rtn_mask = _has_value(df_use["RTN_CR_No"])

    filtered = df_use[system_mask & rtn_mask].copy()
    filtered = filtered[filtered["Last_Status_Time"].notna()].copy()
    filtered = filtered.sort_values("Last_Status_Time", ascending=False)

    if filtered.empty:
        return "I don't see any records with a system-updated last status and an RTN/CR number."

    filtered["Credit Request Total"] = pd.to_numeric(
        filtered.get("Credit Request Total"), errors="coerce"
    ).fillna(0.0)

    if "Date" in filtered.columns:
        filtered["Date"] = pd.to_datetime(filtered["Date"], errors="coerce")
        filtered["Days Since Created"] = (
            filtered["Last_Status_Time"] - filtered["Date"]
        ).dt.days
    else:
        filtered["Days Since Created"] = None

    detail_df = filtered.copy()
    update_counts = (
        detail_df["Last_Status_Time"]
        .dt.date.value_counts()
        .sort_index(ascending=False)
    )
    batches = []
    for date_value, count in update_counts.items():
        if pd.isna(date_value):
            continue
        batch_amount = float(
            detail_df.loc[
                detail_df["Last_Status_Time"].dt.date == date_value,
                "Credit Request Total",
            ].sum()
        )
        batches.append({
            "date": str(date_value),
            "count": int(count),
            "credit_total": batch_amount,
            "credit_total_display": format_money(batch_amount),
        })
    recent_limit = 3
    batch_note = ""
    if batches:
        shown = min(recent_limit, len(batches))
        if len(batches) > recent_limit:
            batch_note = (
                f"- Update batches: showing the {shown} most recent of **{len(batches):,}** date(s) below.\n"
            )
        else:
            batch_note = f"- Update batches: showing all **{len(batches):,}** date(s) below.\n"
    cols = [
        "Date",
        "Ticket Number",
        "Invoice Number",
        "Item Number",
        "Customer Number",
        "Credit Request Total",
        "RTN_CR_No",
        "Last_Status_Time",
        "Days Since Created",
        "Last_Status_Message",
        "Status",
    ]
    for col in cols:
        if col not in detail_df.columns:
            detail_df[col] = None
    filtered = detail_df[cols]
    preview = filtered.head(200).copy()
    message = (
        "System update snapshot — last status updated by the system with RTN/CR present.\n"
        f"- Records found: **{len(filtered):,}**\n"
        f"{batch_note}\nHere is a preview of the results."
    )

    return (
        message,
        preview,
        {
            "show_table": True,
            "csv_filename": "system_updated_with_rtn.csv",
            "csv_rows": filtered,
            "csv_row_count": len(filtered),
            "columns": cols,
            "system_updates_summary": {
                "total_records": int(len(filtered)),
                "total_update_dates": int(len(batches)),
                "recent_limit": recent_limit,
                "batches": batches,
            },
        },
    )
