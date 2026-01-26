import re
from datetime import timedelta
import pandas as pd

from actus.intents._time_reasoning import (
    enrich_time_reasoning,
    summarize_time_reasoning,
)
from actus.utils.df_cleaning import coerce_date
from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "stalled tickets",
    "stale tickets",
    "not updated",
]


def _ensure_update_timestamp(dv: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure dv has an 'Update Timestamp' column, derived from Status if needed.
    Status format is expected to have [YYYY-MM-DD HH:MM:SS] inside.
    """
    dv = dv.copy()

    if "Update Timestamp" in dv.columns:
        dv["Update Timestamp"] = pd.to_datetime(
            dv["Update Timestamp"], errors="coerce"
        )
        return dv

    if "Status" not in dv.columns:
        dv["Update Timestamp"] = pd.NaT
        return dv

    dv["Status"] = dv["Status"].astype(str)

    ts_pattern = r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]"
    all_ts = dv["Status"].str.findall(ts_pattern)
    dv["Update Timestamp"] = all_ts.apply(
        lambda xs: xs[-1] if isinstance(xs, list) and xs else None
    )
    dv["Update Timestamp"] = pd.to_datetime(dv["Update Timestamp"], errors="coerce")
    return dv


def _has_rtn(series: pd.Series) -> pd.Series:
    """
    True = row HAS a credit number (RTN_CR_No).
    """
    s = series.astype(str).str.strip()
    return (s != "") & ~s.str.upper().isin(["NAN", "NONE", "NULL", "NA"])


def _latest_status(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "N/A"
    matches = list(
        re.finditer(r"(?:\[)?(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]?", text)
    )
    if matches:
        end = matches[-1].end()
        cleaned = text[end:].strip()
        return cleaned or "N/A"
    return text


def intent_stalled_tickets(
    query: str, df: pd.DataFrame
) -> tuple[str, pd.DataFrame, dict] | str | None:
    """
    Handle queries like:
      - "SkyBar, which tickets are stalled?"
      - "Show credits that haven't been updated in 7 days"
      - "Which tickets have no recent updates?"

    Logic:
      - Only tickets WITHOUT RTN_CR_No (still open)
      - Compute days since last Update Timestamp
      - Default threshold = 7 days (can parse '14 days', '30 days', etc.)
    """
    q_low = query.lower()

    # Intent detection
    keywords = [
        "stalled",
        "no recent update",
        "no updates",
        "not updated",
        "haven't been updated",
        "haven’t been updated",
        "no movement",
    ]
    if not any(k in q_low for k in keywords):
        return None
    if "ticket" not in q_low and "credit" not in q_low:
        return None

    # Threshold for "stalled"
    m = re.search(r"(\d+)\s+day", q_low)
    stalled_days = int(m.group(1)) if m else 7

    dv = df.copy()

    # Ensure Update Timestamp exists
    dv = _ensure_update_timestamp(dv)

    if "Update Timestamp" not in dv.columns:
        return (
            "I can't detect stalled tickets because I don't see an "
            "`Update Timestamp` column or a `Status` column with timestamps."
        )

    # Optional Date -> Days Open
    if "Date" in dv.columns:
        dv["Date"] = coerce_date(dv["Date"])
    else:
        dv["Date"] = pd.NaT

    today = pd.Timestamp.today().normalize()

    dv["Days Since Update"] = (today - dv["Update Timestamp"]).dt.days

    # Use Date if available
    dv["Days Open"] = (today - dv["Date"]).dt.days

    # Only consider tickets that:
    #   - have an update timestamp
    #   - haven't been updated in >= stalled_days
    mask_stalled_basic = dv["Update Timestamp"].notna() & (
        dv["Days Since Update"] >= stalled_days
    )

    # Only *open* tickets (no RTN_CR_No)
    if "RTN_CR_No" in dv.columns:
        has_rtn = _has_rtn(dv["RTN_CR_No"])
        open_mask = ~has_rtn
    else:
        open_mask = pd.Series(True, index=dv.index)

    stalled_df = dv[mask_stalled_basic & open_mask].copy()

    if stalled_df.empty:
        return (
            f"I don't see any open tickets without RTN_CR_No that have been "
            f"stalled for **{stalled_days}+** days."
        )

    if "Ticket Number" in stalled_df.columns:
        credit_numeric = pd.to_numeric(
            stalled_df.get("Credit Request Total"), errors="coerce"
        ).fillna(0.0)
        per_ticket_total = (
            stalled_df.assign(_credit_total=credit_numeric)
            .groupby("Ticket Number")["_credit_total"]
            .sum()
        )
        stalled_df["Credit Request Total"] = stalled_df["Ticket Number"].map(
            per_ticket_total
        )

    # Deduplicate by ticket to avoid repeated rows
    if "Ticket Number" in stalled_df.columns:
        stalled_df = stalled_df.sort_values("Update Timestamp", ascending=False)
        stalled_df = stalled_df.drop_duplicates(subset=["Ticket Number"], keep="first")

    total_stalled = len(stalled_df)
    total_credits = pd.to_numeric(
        stalled_df.get("Credit Request Total"), errors="coerce"
    ).sum()
    total_credits_str = format_money(total_credits)

    # Small breakdown by how long they've been quiet
    def bucketize(x: float | int) -> str:
        if x < stalled_days:
            return f"<{stalled_days}"
        elif x <= stalled_days + 7:
            return f"{stalled_days}–{stalled_days + 7}"
        elif x <= 30:
            return "15–30"
        else:
            return "30+"

    stalled_df["Stall Bucket"] = stalled_df["Days Since Update"].apply(bucketize)
    bucket_counts = (
        stalled_df["Stall Bucket"].value_counts().reindex(
            [f"{stalled_days}–{stalled_days + 7}", "15–30", "30+"],
            fill_value=0,
        )
    )

    # Exposure per bucket
    bucket_exposure = (
        stalled_df.groupby("Stall Bucket")["Credit Request Total"]
        .apply(lambda s: pd.to_numeric(s, errors="coerce").sum())
        .reindex([f"{stalled_days}–{stalled_days + 7}", "15–30", "30+"], fill_value=0)
    )

    stalled_df["Latest Status"] = stalled_df["Status"].map(_latest_status)
    if "Status" not in stalled_df.columns:
        stalled_df["Status"] = stalled_df["Latest Status"]
    else:
        status_text = stalled_df["Status"].astype(str).str.strip()
        missing_status = status_text.isna() | (status_text == "") | (
            status_text.str.upper().isin(["N/A", "NONE", "NULL", "NAN"])
        )
        stalled_df.loc[missing_status, "Status"] = stalled_df["Latest Status"]

    # Alias columns for time_reasoning module compatibility
    stalled_df["Days_Open"] = stalled_df.get("Days Open", 0)
    stalled_df["Days_Since_Last_Status"] = stalled_df["Days Since Update"]
    stalled_df["Last_Status_Message"] = stalled_df["Latest Status"].astype(str)

    stalled_df = enrich_time_reasoning(stalled_df)
    if "Delay_Score" in stalled_df.columns:
        stalled_df["Delay_Score"] = stalled_df["Delay_Score"].round(1)
    time_summary = summarize_time_reasoning(stalled_df)

    lines: list[str] = [
        f"Stalled tickets snapshot (no credit number, no updates for **{stalled_days}+ days**):",
        f"- Total stalled tickets: **{total_stalled:,}**",
        f"- Total credit exposure: **{total_credits_str}**",
        "",
        "Stall buckets (days since last update):",
        f"- **{stalled_days}–{stalled_days + 7} days**: "
        f"{int(bucket_counts[f'{stalled_days}–{stalled_days + 7}'])} ticket(s) "
        f"({format_money(bucket_exposure[f'{stalled_days}–{stalled_days + 7}'])})",
        f"- **15–30 days**: {int(bucket_counts['15–30'])} ticket(s) "
        f"({format_money(bucket_exposure['15–30'])})",
        f"- **30+ days**: {int(bucket_counts['30+'])} ticket(s) "
        f"({format_money(bucket_exposure['30+'])})",
        "",
        "Time reasoning highlights:",
        f"- Aging not submitted: "
        f"{time_summary.get('follow_up_intent_counts', {}).get('I08_FLAG_AGING_NOT_SUBMITTED', 0)}",
        f"- Billing queue delay: "
        f"{time_summary.get('follow_up_intent_counts', {}).get('I04_CHECK_BILLING_QUEUE', 0)}",
        f"- Stale investigation: "
        f"{time_summary.get('follow_up_intent_counts', {}).get('I03_ESCALATE_STALE_INVESTIGATION', 0)}",
        "",
        "Here is a preview of the results.",
    ]

    preview = (
        stalled_df.assign(Status=stalled_df["Latest Status"])[
            [
                "Update Timestamp",
                "Ticket Number",
                "Customer Number",
                "Days Since Update",
                "Days Open",
                "Latest Status",
                "Status",
                "Macro_Phase",
                "Delay_Reason",
                "Follow_Up_Intent",
                "Delay_Score",
                "Checkpoint_At",
                "Credit Request Total",
                "RTN_CR_No",
                "Reason for Credit",
            ]
        ]
        .sort_values(["Days Since Update", "Days Open"], ascending=False)
        .head(60)
        .copy()
    )

    return (
        "\n".join(lines),
        preview,
        {
            "show_table": True,
            "csv_filename": "stalled_tickets.csv",
            "csv_rows": stalled_df,
            "csv_row_count": len(stalled_df),
            "columns": [
                "Update Timestamp",
                "Ticket Number",
                "Customer Number",
                "Days Since Update",
                "Days Open",
                "Latest Status",
                "Macro_Phase",
                "Delay_Reason",
                "Follow_Up_Intent",
                "Delay_Score",
                "Checkpoint_At",
                "Credit Request Total",
                "RTN_CR_No",
                "Reason for Credit",
                "Status",
            ],
        },
    )
