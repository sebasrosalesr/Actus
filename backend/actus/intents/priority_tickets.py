import pandas as pd
import re

from actus.intents._time_reasoning import (
    enrich_time_reasoning,
    summarize_time_reasoning,
)
from actus.utils.df_cleaning import coerce_date
from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "priority tickets",
    "urgent tickets",
    "high priority",
]


def _no_rtn_mask(dv: pd.DataFrame) -> pd.Series:
    """
    True = ticket still *needs* a credit number.
    False = ticket already has a credit number somewhere.

    We treat a ticket as having a credit number if:
      - RTN_CR_No column is non-empty / non-null
    """
    col = "RTN_CR_No"

    # 1) Normalize RTN_CR_No column if present
    if col in dv.columns:
        # Treat nulls/NaNs as empty so they are counted as missing.
        rtn_series = dv[col].fillna("").astype(str).str.strip()
    else:
        rtn_series = pd.Series("", index=dv.index)

    bad_vals = {"", "NAN", "NONE", "NULL", "NA"}
    has_rtn_col = ~rtn_series.str.upper().isin(bad_vals)

    # 2) Ticket is considered to HAVE a credit if RTN_CR_No is present
    has_credit = has_rtn_col

    # 3) We want only tickets that still need a credit number
    return ~has_credit


def _latest_status(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "N/A"
    matches = list(
        re.finditer(r"(?:\[)?(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]?", text)
    )
    if matches:
        start = matches[-1].start()
        return text[start:].strip()
    return text


def _latest_status_datetime(value: str) -> pd.Timestamp:
    text = str(value or "").strip()
    if not text:
        return pd.NaT
    matches = list(
        re.finditer(r"(?:\[)?(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]?", text)
    )
    if not matches:
        return pd.NaT
    last_ts = matches[-1].group(1)
    return pd.to_datetime(last_ts, errors="coerce")


def intent_priority_tickets(query: str, df: pd.DataFrame):
    """
    Handle queries like:
      - "SkyBar, what tickets are priority?"
      - "Which tickets are priority right now?"
      - "Show priority tickets older than 15 days"

    Logic:
      * Use 'Date' as ticket creation date
      * Exclude tickets that already have RTN_CR_No
      * Find the aging bucket with the highest credit exposure
      * Return that bucket plus top 10 tickets (by credit amount) in the 31–90 day window
    """
    q_low = query.lower()

    # Only trigger if user talks about tickets + priority
    if "priority" not in q_low or "ticket" not in q_low:
        return None

    if "Date" not in df.columns:
        return "I can't compute priority tickets because there is no `Date` column in the dataset."

    dv = df.copy()
    dv["Date"] = coerce_date(dv["Date"])

    # Keep only rows with a valid Date
    dv = dv.dropna(subset=["Date"])
    if dv.empty:
        return "I don't see any tickets with a valid `Date`, so I can't compute priorities yet."

    # Filter out tickets that already have a credit number (RTN_CR_No)
    mask_no_rtn = _no_rtn_mask(dv)
    dv = dv[mask_no_rtn].copy()

    if dv.empty:
        return "Nice! Every ticket with a valid date already has a credit number. No pending priority tickets."

    today = pd.Timestamp.today().normalize()
    # Compute age in days
    dv["Days Open"] = (today - dv["Date"]).dt.days
    dv = dv[dv["Days Open"] >= 0].copy()

    if dv.empty:
        return "I don't see any open tickets without a credit number to prioritize."

    dv["Latest Status"] = dv["Status"].map(_latest_status)
    dv["Last Status Date"] = dv["Status"].map(_latest_status_datetime)
    dv["Days Since Last Status"] = (today - dv["Last Status Date"]).dt.days
    dv["Credit Request Total"] = pd.to_numeric(
        dv.get("Credit Request Total"), errors="coerce"
    ).fillna(0)

    # Alias columns for time_reasoning module compatibility
    dv["Days_Open"] = dv.get("Days Open", 0)
    dv["Days_Since_Last_Status"] = dv["Days Since Last Status"]
    dv["Last_Status_Message"] = dv["Latest Status"].astype(str)

    dv = enrich_time_reasoning(dv)
    if "Delay_Score" in dv.columns:
        dv["Delay_Score"] = dv["Delay_Score"].round(1)
    time_summary = summarize_time_reasoning(dv)

    # Define aging buckets
    bins = [0, 7, 15, 30, 60, 90, 10**9]
    labels = ["0–7", "8–15", "16–30", "31–60", "61–90", "90+"]
    dv["Aging Bucket"] = pd.cut(
        dv["Days Open"],
        bins=bins,
        labels=labels,
        right=True,
        include_lowest=True,
    )

    bucket_counts = dv["Aging Bucket"].value_counts().reindex(labels, fill_value=0)
    bucket_exposure = (
        dv.groupby("Aging Bucket")["Credit Request Total"]
        .sum()
        .reindex(labels, fill_value=0)
    )

    top_bucket = bucket_exposure.idxmax()
    top_bucket_count = int(bucket_counts[top_bucket])
    top_bucket_exposure = format_money(bucket_exposure[top_bucket])

    bucket_df = dv[dv["Aging Bucket"] == top_bucket].copy()

    # Top 10 tickets in 31–90 days by credit amount
    top_31_90_df = dv[
        (dv["Days Open"] >= 31) & (dv["Days Open"] <= 90)
    ].copy()
    top_31_90_df = top_31_90_df.sort_values(
        "Credit Request Total", ascending=False
    ).head(10)

    combined_df = pd.concat([bucket_df, top_31_90_df], ignore_index=True)

    # Deduplicate by ticket to avoid repeated rows
    if "Ticket Number" in combined_df.columns:
        combined_df = combined_df.sort_values("Date", ascending=False)
        combined_df = combined_df.drop_duplicates(subset=["Ticket Number"], keep="first")

    # Keep bucket rows first, then top 10 rows
    combined_df["__section_order"] = combined_df["Aging Bucket"].ne(top_bucket).astype(int)
    combined_df = combined_df.sort_values(
        ["__section_order", "Days Open"],
        ascending=[True, False],
    ).drop(columns=["__section_order"])

    total_open = len(dv)
    oldest_days = int(dv["Days Open"].max()) if total_open else 0
    avg_days = float(dv["Days Open"].mean()) if total_open else 0.0
    total_credits_str = format_money(dv["Credit Request Total"].sum())

    message = "\n".join(
        [
            "Priority tickets snapshot (open tickets without RTN_CR_No):",
            f"- Total open tickets: **{total_open:,}**",
            f"- Oldest open: **{oldest_days}** days",
            f"- Avg days open: **{avg_days:.1f}**",
            f"- Total credit exposure: **{total_credits_str}**",
            f"- Highest exposure bucket: **{top_bucket} days** "
            f"({top_bucket_count} ticket(s), {top_bucket_exposure})",
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
    )

    preview = (
        combined_df[
            [
                "Date",
                "Ticket Number",
                "Customer Number",
                "Days Open",
                "Days Since Last Status",
                "Latest Status",
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
        .head(30)
        .copy()
    )

    return (
        message,
        preview,
        {
            "show_table": True,
            "csv_filename": "priority_tickets.csv",
            "csv_rows": dv,
            "csv_row_count": len(dv),
            "columns": [
                "Date",
                "Ticket Number",
                "Customer Number",
                "Days Open",
                "Days Since Last Status",
                "Latest Status",
                "Macro_Phase",
                "Delay_Reason",
                "Follow_Up_Intent",
                "Delay_Score",
                "Checkpoint_At",
                "Credit Request Total",
                "RTN_CR_No",
                "Reason for Credit",
            ],
        },
    )
