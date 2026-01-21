import re
from datetime import timedelta
import pandas as pd

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
    dv["Update Timestamp"] = dv["Status"].str.extract(ts_pattern)[0]
    dv["Update Timestamp"] = pd.to_datetime(
        dv["Update Timestamp"], errors="coerce"
    )
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
        start = matches[-1].start()
        return text[start:].strip()
    return text


def intent_stalled_tickets(query: str, df: pd.DataFrame):
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
        "Here is a preview of the results.",
    ]

    stalled_df["Latest Status"] = stalled_df["Status"].map(_latest_status)

    preview = (
        stalled_df[
            [
                "Update Timestamp",
                "Ticket Number",
                "Customer Number",
                "Days Since Update",
                "Days Open",
                "Latest Status",
                "Credit Request Total",
                "RTN_CR_No",
                "Reason for Credit",
            ]
        ]
        .sort_values(["Days Since Update", "Days Open"], ascending=False)
        .head(20)
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
                "Credit Request Total",
                "RTN_CR_No",
                "Reason for Credit",
                "Status",
            ],
        },
    )
