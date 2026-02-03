import re
import pandas as pd
from datetime import timedelta

from actus.utils.df_cleaning import coerce_date
from actus.utils.formatting import format_money

INTENT_ALIASES = [
    "credit aging",
    "aging summary",
    "credits over",
]


def _has_rtn(series: pd.Series) -> pd.Series:
    """
    Return a boolean mask where True = row HAS a credit number (RTN_CR_No).
    Treats NaN / '', 'NONE', 'NULL', 'NAN' as 'no credit number'.
    """
    s = series.fillna("").astype(str).str.strip()
    return (s != "") & ~s.str.upper().isin(["NAN", "NONE", "NULL", "NA"])


def intent_credit_aging(query: str, df: pd.DataFrame) -> str | None:
    """
    Handle queries like:
      - "SkyBar, show the credit aging summary"
      - "SkyBar, show credits over 60 days"
      - "What does credit aging look like right now?"

    Uses `Date` as the open date and looks ONLY at tickets WITHOUT RTN_CR_No.
    Buckets:
      - 0–7
      - 8–15
      - 16–30
      - 31–60
      - 61–90
      - 90+ days
    """
    q_low = query.lower()

    # Basic intent detection
    if (
        "aging" not in q_low
        and "ageing" not in q_low
        and not re.search(r"\bover\s+\d+\s+day", q_low)
        and not re.search(r"older than\s+\d+\s+day", q_low)
    ):
        return None

    if "credit" not in q_low and "ticket" not in q_low:
        # Feels like some other aging question, let other intents try
        return None

    # Determine a "highlight" threshold if user says:
    #   "over 60 days" / "older than 45 days"
    m = re.search(r"(?:over|older than)\s+(\d+)\s+day", q_low)
    highlight_threshold = int(m.group(1)) if m else 60

    if "Date" not in df.columns:
        return "I can't compute aging without a `Date` column in the dataset."

    dv = df.copy()
    dv["Date"] = coerce_date(dv["Date"])
    dv = dv.dropna(subset=["Date"])

    today = pd.Timestamp.today().normalize()
    dv["Days Open"] = (today - dv["Date"]).dt.days

    # Filter to tickets WITHOUT a credit number
    if "RTN_CR_No" in dv.columns:
        has_rtn_mask = _has_rtn(dv["RTN_CR_No"])
        open_mask = ~has_rtn_mask
    else:
        open_mask = pd.Series(True, index=dv.index)

    open_df = dv[open_mask & (dv["Days Open"] >= 0)].copy()

    if open_df.empty:
        return (
            "I don't see any open credits without a `RTN_CR_No` to build an aging summary."
        )

    # Define aging buckets
    bins = [0, 7, 15, 30, 60, 90, 10**9]
    labels = ["0–7", "8–15", "16–30", "31–60", "61–90", "90+"]

    open_df["Aging Bucket"] = pd.cut(
        open_df["Days Open"],
        bins=bins,
        labels=labels,
        right=True,
        include_lowest=True,
    )

    bucket_counts = open_df["Aging Bucket"].value_counts().reindex(labels, fill_value=0)

    total_open = len(open_df)
    total_credits = pd.to_numeric(
        open_df.get("Credit Request Total"), errors="coerce"
    ).sum()
    total_credits_str = format_money(total_credits)
    oldest_days = int(open_df["Days Open"].max()) if total_open else 0
    avg_days = float(open_df["Days Open"].mean()) if total_open else 0.0

    open_df_full = open_df.copy()

    # Bucket exposure by days open (use full open set)
    bucket_exposure = (
        open_df_full.groupby("Aging Bucket")["Credit Request Total"]
        .apply(lambda s: pd.to_numeric(s, errors="coerce").sum())
        .reindex(labels, fill_value=0)
    )

    lines: list[str] = [
        "Credit aging snapshot (open tickets without RTN_CR_No):",
        f"- Total open tickets: **{total_open:,}**",
        f"- Oldest open: **{oldest_days}** days",
        f"- Avg days open: **{avg_days:.1f}**",
        f"- Total credit exposure: **{total_credits_str}**",
        "",
        "Buckets (days open):",
    ]

    for label in labels:
        lines.append(
            f"- **{label} days**: {int(bucket_counts[label])} ticket(s) "
            f"({format_money(bucket_exposure[label])})"
        )

    message = "\n".join(lines) + "\n\nHere is a preview of the results."

    open_df_full["Latest Status"] = open_df_full["Status"].map(
        lambda v: re.sub(r"^.*?(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", r"\\1", str(v)).strip()
        if pd.notna(v) else "N/A"
    )

    preview_df = open_df_full.copy()
    # Deduplicate for preview to avoid repeated rows
    if "Ticket Number" in preview_df.columns:
        preview_df = preview_df.sort_values("Date", ascending=False)
        preview_df = preview_df.drop_duplicates(subset=["Ticket Number"], keep="first")

    preview = (
        preview_df[
            [
                "Date",
                "Ticket Number",
                "Customer Number",
                "Days Open",
                "Latest Status",
                "Credit Request Total",
                "RTN_CR_No",
                "Reason for Credit",
            ]
        ]
        .sort_values("Days Open", ascending=False)
        .head(20)
        .copy()
    )

    return (
        message,
        preview,
        {
            "show_table": True,
            "csv_filename": "credit_aging.csv",
            "csv_rows": open_df_full,
            "csv_row_count": len(open_df_full),
            "columns": [
                "Date",
                "Ticket Number",
                "Customer Number",
                "Days Open",
                "Latest Status",
                "Credit Request Total",
                "RTN_CR_No",
                "Reason for Credit",
                "Status",
            ],
        },
    )
